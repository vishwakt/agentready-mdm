"""Generate the three results figures from results/results.json.
IEEE single-column sizing (3.5in wide), grayscale-safe hatching."""
import json, statistics as st
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
import matplotlib.pyplot as plt

rows = json.load(open("results/results.json"))
RATES = [0.0, 0.05, 0.15]
G, B = "#404040", "#b0b0b0"

def mean(sel):
    xs = list(sel)
    return st.mean(xs), (st.stdev(xs) if len(xs) > 1 else 0)

# ---- Fig 1: containment per fault class ----
agg = {}
for r in rows:
    if r["rate"] == 0: continue
    for f, d in r["containment"].items():
        c, t = agg.get(f, (0, 0))
        agg[f] = (c + d["contained"], t + d["total"])
labels = [f.split("_", 1)[0] for f in sorted(agg)]
vals = [100 * c / t for c, t in (agg[f] for f in sorted(agg))]
fig, ax = plt.subplots(figsize=(3.5, 2.1))
bars = ax.bar(labels, vals, color=G, width=0.62)
ax.set_ylabel("Containment (%)", fontsize=8)
ax.set_ylim(0, 108)
ax.axhline(100, ls=":", lw=0.7, color="k")
for b, v in zip(bars, vals):
    ax.text(b.get_x() + b.get_width()/2, v + 1.5, f"{v:.0f}", ha="center", fontsize=6.5)
ax.tick_params(labelsize=7.5)
fig.tight_layout()
fig.savefig("results/fig_containment.pdf"); fig.savefig("results/fig_containment.png", dpi=200)

# ---- Fig 2: silent consumer error vs fault rate ----
fig, ax = plt.subplots(figsize=(3.5, 2.2))
for name, key, color, marker in [("Baseline (silent)", "baseline", "k", "o"),
                                  ("Governed (silent)", "governed", G, "s")]:
    ms, es = zip(*[mean(100 * r["c1"][key]["silent_error_rate"]
                        for r in rows if r["rate"] == rt) for rt in RATES])
    ax.errorbar([100*r for r in RATES], ms, yerr=es, label=name, color=color,
                marker=marker, ms=4, lw=1.2, capsize=2)
ms, _ = zip(*[mean(100 * r["c1"]["governed"]["flagged_rate"]
                   for r in rows if r["rate"] == rt) for rt in RATES])
ax.plot([100*r for r in RATES], ms, ls="--", color=B, marker="^", ms=4, lw=1.2,
        label="Governed (flagged)")
ax.set_xlabel("Injected fault rate (%)", fontsize=8)
ax.set_ylabel("Consumer decision error (%)", fontsize=8)
ax.legend(fontsize=6.5, frameon=False)
ax.tick_params(labelsize=7.5)
fig.tight_layout()
fig.savefig("results/fig_silent_errors.pdf"); fig.savefig("results/fig_silent_errors.png", dpi=200)

# ---- Fig 3: wrong autonomous actions by policy ----
fig, ax = plt.subplots(figsize=(3.5, 2.6))
policies = [
    ("Baseline\n(blind commit)", lambda r: r["c2"]["baseline"]["wrong_action_rate"], "k", ""),
    ("Governed\n(defer)", lambda r: r["c2"]["governed"]["wrong_action_rate"], G, ""),
    ("Governed\n(partial, per-op)", lambda r: r["c2"]["governed"]["partial_wrong_action_rate"], B, "//"),
    ("Governed\n(partial, per-item)", lambda r: r["c2"]["governed"]["partial_item_error_rate"], "w", "xx"),
]
x = range(len(RATES))
w = 0.2
for i, (name, sel, color, hatch) in enumerate(policies):
    ms, es = zip(*[mean(100 * sel(r) for r in rows if r["rate"] == rt) for rt in RATES])
    ax.bar([xi + (i - 1.5) * w for xi in x], ms, w, yerr=es, capsize=2,
           label=name, color=color, hatch=hatch, edgecolor="k", lw=0.5)
ax.set_xticks(list(x), [f"{int(100*r)}%" for r in RATES], fontsize=7.5)
ax.set_xlabel("Injected fault rate", fontsize=8)
ax.set_ylabel("Wrong autonomous actions (%)", fontsize=8)
ax.legend(fontsize=6, frameon=False, ncol=2, loc="lower center", bbox_to_anchor=(0.5, 1.01))
ax.tick_params(labelsize=7.5)
fig.tight_layout()
fig.savefig("results/fig_actions.pdf"); fig.savefig("results/fig_actions.png", dpi=200)
print("figures written")
