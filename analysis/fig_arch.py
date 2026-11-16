"""Architecture figure v2: three horizontal lanes, elbow-routed arrows,
no arrow passes over any box or label."""
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(3.5, 2.7))
ax.axis("off")

def box(x, y, w, h, label, fc="#e8e8e8", fs=6.2):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.008",
                                fc=fc, ec="k", lw=0.8))
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", fontsize=fs)

def arr(pts, ls="-"):
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        last = (x2, y2) == pts[-1]
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
                     arrowstyle="-|>" if last else "-",
                     mutation_scale=6.5, lw=0.8, ls=ls, color="k",
                     shrinkA=0, shrinkB=0))

# Lane 1 (top, y .80-.97): sources
box(0.02, 0.81, 0.19, 0.15, "Event\nstream", fc="white")
box(0.27, 0.81, 0.21, 0.15, "Fault injector\nF1..F8", fc="#d0d0d0")
arr([(0.21, 0.885), (0.27, 0.885)])

# Lane 2 (middle, y .50-.72): governed stages
gx = [0.02, 0.212, 0.404, 0.596, 0.788]
gw = 0.172
labels = ["Idem.\ngate", "Matcher", "Survivor", "Commit\n(immut.)", "Router\n(contracts)"]
for x, lab in zip(gx, labels):
    box(x, 0.52, gw, 0.16, lab)
for x in gx[:-1]:
    arr([(x + gw, 0.60), (x + gw + 0.02, 0.60)])
ax.text(0.5, 0.71, "Governed pipeline", fontsize=6.5, ha="center", style="italic")
# stream -> governed (elbow down the left margin, entering lane 2 from the left)
arr([(0.375, 0.81), (0.375, 0.76), (0.005, 0.76), (0.005, 0.60), (0.02, 0.60)])

# Lane 3 (y .27-.42): exception surfaces under their producing stages
box(0.212, 0.28, 0.26, 0.13, "Quarantine\n(exception states)", fc="white")
box(0.50, 0.28, 0.10, 0.13, "DLQ", fc="white")
box(0.63, 0.28, 0.24, 0.13, "Append-only log\n(replay)", fc="white")
arr([(0.30, 0.52), (0.30, 0.41)])          # matcher -> quarantine
arr([(0.49, 0.52), (0.49, 0.44), (0.55, 0.44), (0.55, 0.41)])  # survivor -> dlq side
arr([(0.68, 0.52), (0.68, 0.44), (0.72, 0.44), (0.72, 0.41)])  # commit -> log

# Lane 4 (bottom, y .02-.17): baseline + consumers
box(0.02, 0.03, 0.30, 0.14, "Baseline\n(last-write-wins, no controls)", fc="#f5f5f5")
box(0.55, 0.03, 0.43, 0.14, "Consumers C1 / C2\n(scored vs ground truth)", fc="white")
# stream -> baseline (elbow down the left margin, below governed inlet)
arr([(0.005, 0.60), (0.005, 0.10), (0.02, 0.10)])
arr([(0.32, 0.10), (0.55, 0.10)])           # baseline -> consumers
# router -> consumers (right margin elbow)
arr([(0.874, 0.52), (0.874, 0.17)])
# quarantine contest signal -> consumers (dotted, elbow below quarantine)
arr([(0.34, 0.28), (0.34, 0.22), (0.62, 0.22), (0.62, 0.17)], ls=":")
ax.text(0.475, 0.235, "contest signal", fontsize=5.6, ha="center")

ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
fig.tight_layout(pad=0.1)
fig.savefig("results/fig_architecture.pdf")
fig.savefig("results/fig_architecture.png", dpi=200)
print("architecture v2 written")
