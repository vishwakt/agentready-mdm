"""Experiment matrix: 10 seeds x {0, 0.05, 0.15} x {baseline, governed}.

Emits results/results.json with per-run metrics:
  containment per fault class, C1 error rate, C2 wrong-action rate,
  dangling references, latency p50/p95, replay fidelity, dup-detector FPs.
"""
from __future__ import annotations
import collections, json, statistics as st, sys, time

sys.path.insert(0, ".")
from corpus.generator import generate
from faults.injector import inject
from pipelines.baseline import BaselinePipeline
from pipelines.governed import GovernedPipeline
from consumers.tasks import ground_truth_state, run_c1, run_c2

SEEDS = list(range(1, 11))
RATES = [0.0, 0.05, 0.15]


def dangling(state: dict) -> int:
    items = set(state["item"])
    return sum(1 for a in state["assortment"].values()
               for i in a.get("item_ids", []) if i not in items)


def containment(events, replayable=True):
    """Classify each fault-labeled event by governed outcome (fresh pipeline)."""
    gov = GovernedPipeline()
    outcome_by_obj = {}
    for e in events:
        outcome_by_obj[id(e)] = gov.apply(e)
    per_class = collections.defaultdict(lambda: [0, 0])  # contained, total
    for e in events:
        if not e.injected_faults:
            continue
        f = e.injected_faults[0]
        per_class[f][1] += 1
        if outcome_by_obj[id(e)].startswith(("quarantined", "skipped")):
            per_class[f][0] += 1
    return {f: {"contained": c, "total": t} for f, (c, t) in sorted(per_class.items())}, gov


def one_run(seed: int, rate: float) -> dict:
    corpus = generate(seed=seed)
    truth = ground_truth_state(corpus)
    events = inject(corpus, rate, seed=seed)

    base = BaselinePipeline()
    base.run(events)

    cont, gov = containment(events)

    # dup-detector false positives: quarantined duplicate_entity with no injected fault label
    fps = sum(1 for q in gov.quarantine
              if q["reason"].startswith("duplicate_entity") and not q["faults"])

    replay_ok = gov.replay_to(len(gov.event_log))["item"] == gov.head["item"]

    def lat(p):
        xs = sorted(p.latencies_us)
        return {"p50": xs[len(xs)//2], "p95": xs[int(len(xs)*0.95)]}

    row = {"seed": seed, "rate": rate, "containment": cont,
           "dup_detector_false_positives": fps, "replay_fidelity": replay_ok,
           "dangling": {"baseline": dangling(base.visible_state()),
                        "governed": dangling(gov.visible_state())},
           "latency_us": {"baseline": lat(base), "governed": lat(gov)},
           "c1": {"baseline": run_c1(base, truth, seed),
                  "governed": run_c1(gov, truth, seed)},
           "c2": {"baseline": run_c2(base, truth, seed),
                  "governed": run_c2(gov, truth, seed)}}
    return row


def main():
    t0 = time.time()
    rows = [one_run(s, r) for s in SEEDS for r in RATES]
    import os
    os.makedirs("results", exist_ok=True)
    with open("results/results.json", "w") as f:
        json.dump(rows, f, indent=1)

    # console summary
    for rate in RATES:
        sub = [r for r in rows if r["rate"] == rate]
        c1b = [r["c1"]["baseline"]["silent_error_rate"] for r in sub]
        c1g = [r["c1"]["governed"]["silent_error_rate"] for r in sub]
        c2b = [r["c2"]["baseline"]["wrong_action_rate"] for r in sub]
        c2g = [r["c2"]["governed"]["wrong_action_rate"] for r in sub]
        c1f = [r["c1"]["governed"]["flagged_rate"] for r in sub]
        c2d = [r["c2"]["governed"]["deferred_rate"] for r in sub]
        c2p = [r["c2"]["governed"]["partial_wrong_action_rate"] for r in sub]
        c2i = [r["c2"]["governed"]["partial_item_error_rate"] for r in sub]
        print(f"rate {rate:>4}  C1 silent-err%  base {100*st.mean(c1b):5.1f}±{100*st.stdev(c1b):4.1f} | "
              f"gov {100*st.mean(c1g):5.2f}±{100*st.stdev(c1g):4.2f} (flagged {100*st.mean(c1f):4.1f})   "
              f"C2 wrong%  base {100*st.mean(c2b):5.1f} | gov {100*st.mean(c2g):5.2f} (deferred {100*st.mean(c2d):4.1f}, partial-commit wrong {100*st.mean(c2p):5.2f}, per-item {100*st.mean(c2i):5.2f})")
    agg = collections.defaultdict(lambda: [0, 0])
    for r in rows:
        for f, d in r["containment"].items():
            agg[f][0] += d["contained"]; agg[f][1] += d["total"]
    print("\ncontainment (all seeds, rates>0):")
    for f, (c, t) in sorted(agg.items()):
        print(f"  {f:26s} {c:4d}/{t:4d}  ({100*c/t:5.1f}%)")
    fps = [r["dup_detector_false_positives"] for r in rows if r["rate"] == 0.0]
    print(f"\ndup-detector FPs on clean corpus (per seed): {fps}")
    print(f"replay fidelity all runs: {all(r['replay_fidelity'] for r in rows)}")
    print(f"\n{len(rows)} runs in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
