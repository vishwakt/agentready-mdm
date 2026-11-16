"""Ablation study (each control disabled in turn), conflict-policy comparison,
and a corpus-scale microbenchmark. 10 seeds, 15% fault rate."""
import json, statistics as st, sys, time
sys.path.insert(0, ".")
from corpus.generator import generate
from faults.injector import inject
from pipelines.baseline import BaselinePipeline
from pipelines.governed import GovernedPipeline
from consumers.tasks import ground_truth_state, run_c1, run_c2

SEEDS = list(range(1, 11))
RATE = 0.15
CONFIGS = {
    "full": {},
    "no_idempotency": {"disable": {"idempotency"}},
    "no_matcher": {"disable": {"matcher"}},
    "no_survivor": {"disable": {"survivor"}},
    "no_cascade": {"disable": {"cascade"}},
    "conflict_quarantine": {"conflict_policy": "quarantine"},
}


def dangling(state):
    items = set(state["item"])
    return sum(1 for a in state["assortment"].values()
               for i in a.get("item_ids", []) if i not in items)


def main():
    out = {}
    for name, cfg in CONFIGS.items():
        rows = []
        for seed in SEEDS:
            corpus = generate(seed=seed)
            truth = ground_truth_state(corpus)
            events = inject(corpus, RATE, seed=seed)
            gov = GovernedPipeline(**cfg)
            gov.run(events)
            c1 = run_c1(gov, truth, seed)
            c2 = run_c2(gov, truth, seed)
            rows.append({"silent": c1["silent_error_rate"], "flagged": c1["flagged_rate"],
                         "wrong": c2["wrong_action_rate"], "defer": c2["deferred_rate"],
                         "dangling": dangling(gov.visible_state())})
        out[name] = {k: (st.mean([r[k] for r in rows]),
                         st.stdev([r[k] for r in rows]) if len(rows) > 1 else 0)
                     for k in rows[0]}
        m = out[name]
        print(f"{name:22s} silent {100*m['silent'][0]:5.2f}±{100*m['silent'][1]:4.2f}  "
              f"flagged {100*m['flagged'][0]:5.2f}  wrong {100*m['wrong'][0]:5.2f}  "
              f"defer {100*m['defer'][0]:5.1f}  dangling {m['dangling'][0]:6.1f}")

    json.dump(out, open("results/ablation.json", "w"), indent=1)

    # scale microbenchmark: 10x corpus
    print("\nscale microbenchmark:")
    for label, kw in [("1x (2k items, 3k updates)", dict()),
                      ("10x (20k items, 30k updates)", dict(n_items=20000, n_updates=30000))]:
        c = generate(seed=7, **kw)
        ev = inject(c, RATE, seed=7)
        g = GovernedPipeline()
        t0 = time.time()
        g.run(ev)
        xs = sorted(g.latencies_us)
        print(f"  {label:30s} events={len(ev):6d}  p50={xs[len(xs)//2]:5.1f}us  "
              f"p95={xs[int(len(xs)*.95)]:5.1f}us  wall={time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
