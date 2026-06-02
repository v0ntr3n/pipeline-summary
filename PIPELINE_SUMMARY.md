# Pipeline Summary — MIT Supercloud Dataset

## Overview

End-to-end pipeline transforming raw MIT Supercloud monitoring data into a fused, labeled time-series dataset (`tide_dataset.csv`) suitable for AI agent training. Pure Python stdlib — no external dependencies.

---

## Pipeline Architecture

```
slurm-log.csv ──┐
                ├─→ Phase 0: Core Data Prep ──→ 46,355 COMPLETED jobs (peak 15%)
cpu/*/ts.csv ───┤
gpu/*.csv ──────┼─→ Phase 1: Telemetry Fusion ──→ tide_dataset.csv (12,896 bins × 19 features)
node-data.csv ──┘
labelled_jobids.csv ──→ Lognormal calibration per DNN label
```

---

## Phase 0 — Core Data Prep

| Step | Operation | Input | Output |
|------|-----------|-------|--------|
| 0.1 | Minimal column load | `slurm-log.csv` (29 cols) | 395,914 rows × 14 cols |
| 0.2 | Filter COMPLETED (state=3) | 395,914 rows | 309,037 jobs |
| 0.3 | Peak sampling (15% continuous window) | 309,037 jobs | 46,355 jobs |

**Step 0.3 detail:** Sweep-line algorithm computes concurrent job count per 5-min bin (O(n+bins)), identifies peak concurrency bin (956 jobs), then selects a continuous 15% window centered on peak. Preserves temporal ordering and convoy effects.

**Peak window:**
- Start epoch: 1,627,892,794
- End epoch: 1,631,761,576
- Duration: 1,074.7 hours (~44.8 days)
- Peak concurrency: 956

---

## Phase 1 — Empirical Telemetry Fusion & I/O Reconstruction

### Step 1.1 — I/O Extraction

| Source | Resolution | Records Found | Key Metrics |
|--------|------------|---------------|-------------|
| CPU time-series (`cpu/0000/*-timeseries.csv`) | 10s | 54 jobs | ReadMB, WriteMB, I/O rate stats |
| GPU time-series (`gpu/0000/*.csv`) | 100ms | 39 jobs | SM util, power, SM-dip fraction |
| Node data (`node-data.csv`) | 5min | 7,830,669 rows in window | Lustre RPC, FS latency, load avg |

**Note:** Only 54/46,355 jobs had CPU time-series files in the sample — the dataset provides time-series for a subset of jobs. GPU coverage is similarly sparse (39 jobs).

### Step 1.2 — Cross-Modal Join (5-min bins)

Sweep-line accumulation: for each job, mark all bins from `start_bin` to `end_bin`, accumulating:
- `active_jobs` — concurrent job count
- `sum_io_mb_real` — total I/O bytes from CPU time-series
- `io_p95_10s` / `io_max_10s` — micro-burst I/O rates
- `io_fano_mean` — burstiness (Fano factor = variance/mean)
- `sm_dip_job_count` — jobs with GPU util <5% for >50% of time (I/O stall signature)
- `gpu_jobs_active` — jobs with GPU allocation
- `lustre_rpc_total` / `fs_latency_mean` / `load_avg_mean` / `mem_free_kb_mean` — from node-data

**Output:** 12,896 bins × 5 min = 1,074.7 hours of system-state sequence.

### Step 1.3 — Calibrated Bandwidth Reconstruction

**Physical ceiling:** 4 OSS × bonded 100 GbE ≈ 40,000 MB/s

**Method:**
1. Observed bandwidth = `sum_io_mb_real / 300s`, capped at ceiling
2. Calibrate RPC→bandwidth ratio from bins with both I/O and RPC data
3. For bins with RPC but no direct I/O: `bandwidth = RPC / median(RPC/MB) / 300s`
4. Combined: prefer observed, fallback to reconstructed

**Lognormal calibration:** Fit lognormal (μ, σ) to real I/O per DNN model label from `labelled_jobids.csv`. Found 1 model label with sufficient samples.

### Step 1.4 — Empirical Risk Labeling

Multi-signal congestion detection:

| Condition | Label |
|-----------|-------|
| RPC > P95 AND saturation > 70% | Severe (2) |
| RPC > P95 AND SM-dip jobs > 0 | Severe (2) |
| Saturation > 70% AND SM-dip jobs > 0 | Moderate (1) |
| Otherwise | Clear (0) |

**Results:**
- Severe: 3 bins
- Moderate: 0 bins
- Clear: 12,893 bins
- Legacy IO>800 MB/s threshold hits: 203 bins

---

## Output Files

| File | Size | Description |
|------|------|-------------|
| `tide_dataset.csv` | 1.5 MB | 12,896 rows × 19 columns, 5-min binned system-state |
| `provenance.json` | 3.0 KB | Per-column data source, type (observed/reconstructed/derived), notes |
| `peak_window.json` | 210 B | Time window metadata (start, end, duration, job count, peak concurrency) |
| `label_distribution.csv` | 366 B | DNN model label counts |
| `pipeline.py` | 8.5 KB | Full pipeline source (stdlib only) |
| `pipeline.log` | 1.2 KB | Execution log |

---

## tide_dataset.csv Schema

| Column | Type | Source | Provenance |
|--------|------|--------|------------|
| `time_epoch` | int | derived | 5-min bin start timestamp |
| `time_bin_idx` | int | derived | Sequential bin index |
| `active_jobs` | int | slurm-log.csv | observed — concurrent job count |
| `sum_io_mb_real` | float | cpu/timeseries | observed — total I/O MB in bin |
| `io_p95_10s` | float | cpu/timeseries | observed — P95 of 10s I/O rate |
| `io_max_10s` | float | cpu/timeseries | observed — max 10s I/O rate |
| `io_fano_mean` | float | cpu/timeseries | observed — mean Fano factor |
| `lustre_rpc_total` | float | node-data.csv | observed — sum Lustre RPCs |
| `fs_latency_mean` | float | node-data.csv | observed — mean FS latency |
| `load_avg_mean` | float | node-data.csv | observed — mean load average |
| `mem_free_kb_mean` | float | node-data.csv | observed — mean free memory KB |
| `sm_dip_job_count` | int | gpu/*.csv | observed — I/O-stalled GPU jobs |
| `gpu_jobs_active` | int | slurm tres_alloc | observed — GPU-allocated jobs |
| `bandwidth_observed_mbs` | float | cpu/timeseries | observed — MB/s, capped 40 GB/s |
| `bandwidth_reconstructed_mbs` | float | node-data+cpu | reconstructed — from RPC ratio |
| `bandwidth_combined_mbs` | float | cpu+node-data | reconstructed — observed preferred |
| `saturation_ratio` | float | derived | reconstructed — bw/ceiling |
| `congestion_level` | int | derived | derived — 0/1/2 severity |
| `legacy_io_threshold` | int | derived | derived — bw > 800 MB/s flag |

---

## Key Statistics

| Metric | Value |
|--------|-------|
| Total slurm jobs loaded | 395,914 |
| COMPLETED jobs | 309,037 (78%) |
| Peak-sampled jobs | 46,355 (15%) |
| Peak concurrency | 956 |
| Time-series bins | 12,896 |
| Window duration | 1,074.7 hours (44.8 days) |
| Max bandwidth | 9,703.3 MB/s |
| Max active jobs in bin | 964 |
| CPU I/O coverage | 54 / 46,355 jobs (0.12%) |
| GPU stats coverage | 39 / 46,355 jobs (0.08%) |
| Node-data rows in window | 7,830,669 |
| Pipeline runtime | 98.2 seconds |

---

## Caveats & Known Limitations

1. **Sparse time-series coverage:** Only ~0.1% of peak-window jobs have CPU/GPU time-series files. Most `sum_io_mb_real` values are 0; bandwidth relies heavily on RPC-based reconstruction.
2. **Single DNN label:** Only 1 model label had ≥5 I/O samples for lognormal fitting. The labeled subset (`labelled_jobids.csv`) is small.
3. **Low congestion detection:** Only 3/12,896 bins flagged severe — expected given sparse I/O data. Congestion labels will become more meaningful as time-series coverage increases.
4. **No real-time API:** Dataset is offline CSV dumps — suitable for training, not live control.
5. **Aggressive anonymization:** No job names, paths, or kernel versions — agents must reason from resource signals only.

---

## Suggested Next Steps

1. **Expand time-series coverage** — incorporate all CPU/GPU files (not just peak-window matches)
2. **Semi-supervised labeling** — use the 54 I/O-profiled jobs as seeds for clustering the 46K unlabeled jobs
3. **Anomaly detection baseline** — compare against threshold alerts from Supercloud operators
4. **Idle-GPU reclaimer** — high-impact agent: detect allocated-but-idle GPUs (SM util ≈ 0)
5. **Cross-layer root-cause linker** — use preserved job IDs to trace scheduler→node→Lustre→GPU for diagnostic reasoning