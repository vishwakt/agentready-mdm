# agentready-mdm

Reference implementation and fault-injection evaluation for:

> V. Thatikonda and S. Parsi, "Trust Begins at the Record: An Agent-Ready Master Data
> Control Layer Evaluated Under Systematic Fault Injection," in *Proc. IEEE International
> Conference on AI Engineering and Innovation (AIEI 2027)*, Bangalore, India, Jan. 2027.

The control layer (matcher, survivor, router) enforces six trust properties of master data
that autonomous agents act upon. The evaluation injects eight master-data fault classes into a
synthetic retail-style corpus and measures containment, silent versus flagged consumer error,
and wrong autonomous actions against a no-controls baseline and a conventional
validate-and-overwrite pipeline.

## Layout

| Folder | Contents |
|---|---|
| `corpus/` | Deterministic synthetic corpus and event stream generator |
| `faults/` | Fault injector, classes F1 to F8, configurable rates |
| `pipelines/` | Baseline (no controls) and governed (control layer) pipelines, with ablation switches and conflict policies |
| `consumers/` | C1 task suite and C2 scripted agent; ground-truth oracle |
| `analysis/` | Experiment matrix, ablation, camera-ready statistics, figures |
| `results/` | Result files and figures reported in the paper |

## Reproduce (Python 3.11+)

```bash
pip install -r requirements.txt
python3 analysis/run_experiments.py        # 10 seeds x 3 fault rates (main matrix)
python3 analysis/run_ablation.py           # per-control ablation and conflict policy
python3 analysis/camera_ready_analyses.py  # bootstrap CIs, Wilcoxon, propagation, F3, conventional baseline
python3 analysis/figures.py                # figures
```

All randomness is seeded: identical seeds produce identical results.

## License

MIT. See `LICENSE`.
