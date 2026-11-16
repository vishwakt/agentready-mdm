"""Camera-ready analyses (reviewer responses): bootstrap CIs + Wilcoxon on the
existing matrix; propagation check; F3 outcome breakdown; conventional baseline."""
import json, random, statistics as st, sys
sys.path.insert(0, ".")
from scipy import stats
from corpus.generator import generate
from faults.injector import inject
from pipelines.governed import GovernedPipeline
from consumers.tasks import ground_truth_state, run_c1, run_c2, _sellable_for_store

out = {}
rows = json.load(open("results/results.json"))

def boot(xs, n=10000, seed=0):
    rng = random.Random(seed)
    ms = sorted(st.mean(rng.choices(xs, k=len(xs))) for _ in range(n))
    return [st.mean(xs), ms[int(.025*n)], ms[int(.975*n)]]

stats_out = {}
for rate in (0.05, 0.15):
    sub = [r for r in rows if r["rate"] == rate]
    for name, k1, k2 in [("silent", "c1", "silent_error_rate"), ("wrong", "c2", "wrong_action_rate")]:
        b = [r[k1]["baseline"][k2] for r in sub]; g = [r[k1]["governed"][k2] for r in sub]
        stats_out[f"{name}@{rate}"] = {"baseline_ci": boot(b), "governed_ci": boot(g),
                                       "wilcoxon_p": stats.wilcoxon(b, g).pvalue}
out["stats"] = stats_out

class View:  # consumer that does not consult the exception interface
    def __init__(self, s): self.s = s
    def visible_state(self): return self.s

prop, f3, conv = [], [], []
for seed in range(1, 11):
    corpus = generate(seed=seed); truth = ground_truth_state(corpus)
    ev = inject(corpus, 0.15, seed=seed)
    from pipelines.baseline import BaselinePipeline
    base = BaselinePipeline(); base.run(ev); bs = base.visible_state()
    # propagation: per-item exposure p among items in intended sets, mean set size n
    rng = random.Random(seed + 777)
    sids = rng.sample(sorted(truth["store"]), 40)
    sizes, bad, tot = [], 0, 0
    for sid in sids:
        I = _sellable_for_store(truth, sid); P = _sellable_for_store(bs, sid)
        sizes.append(len(I | P)); tot += len(I | P); bad += len(I ^ P)
    n, p = st.mean(sizes), bad / tot
    prop.append({"n": n, "p": p, "pred": 1 - (1 - p) ** n,
                 "obs": run_c2(base, truth, seed)["wrong_action_rate"]})
    # F3: accepted conflict events, is the item's final governed state correct?
    g = GovernedPipeline(); outc = {}
    for e in ev: outc[id(e)] = g.apply(e)
    acc = [e for e in ev if e.injected_faults == ["F3_conflicting_values"] and outc[id(e)] == "committed"]
    wrong = sum(1 for e in acc if g.head["item"].get(e.payload["item_id"], {}).get("sellable")
                != truth["item"].get(e.payload["item_id"], {}).get("sellable"))
    f3.append({"accepted": len(acc), "final_state_wrong": wrong})
    # conventional baseline: validation on; no survivorship, no cascade, no idempotency; exceptions not consulted
    c = GovernedPipeline(disable={"survivor", "cascade", "idempotency"}); c.run(ev)
    conv.append({"silent": run_c1(View(c.head), truth, seed)["silent_error_rate"],
                 "wrong": run_c2(View(c.head), truth, seed)["wrong_action_rate"],
                 "dangling": sum(1 for a in c.head["assortment"].values() for i in a.get("item_ids", []) if i not in c.head["item"])})

out["propagation"] = {k: st.mean(d[k] for d in prop) for k in prop[0]}
out["f3"] = {"accepted": sum(d["accepted"] for d in f3), "final_state_wrong": sum(d["final_state_wrong"] for d in f3)}
out["conventional"] = {k: st.mean(d[k] for d in conv) for k in conv[0]}
json.dump(out, open("results/camera_ready.json", "w"), indent=1, default=float)
print(json.dumps(out, indent=1, default=float))
