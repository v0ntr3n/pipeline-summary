# MIT Supercloud Dataset — Pipeline Summary

## Overview

Two-stage data engineering pipeline transforming raw Slurm + telemetry logs into
ML-ready datasets for AI agent systems (predictive, anomaly-detection, diagnostic).

---

## Phase 0 — Core Data Prep

**File:** `pipeline_phase0.py`
**Input:**  `slurm-log.csv` (395,914 rows) + `tres-mapping.txt`
**Output:** `artefacts/clean_job_trace.csv` (46,355 rows, 11 columns)

### Data Flow

```
slurm-log.csv (395,914 rows)
  │
  ├─ Filter: state=3 (COMPLETED) → 309,037 rows
  ├─ Parse TRES → extract mem_mb, cpu_count, gpu_count
  ├─ Validate timestamps (time_submit ≤ time_start ≤ time_end)
  │     → 309,037 clean traces
  │
  └─ Peak-sampling: densest 15% continuous window
        → 46,355 most active jobs
```

### Output Schema (`clean_job_trace.csv`)

| Column         | Type    | Description                              |
|----------------|---------|------------------------------------------|
| id_job         | string  | Anonymized job ID (salted SHA-256 hash)  |
| time_submit    | int     | Submission epoch (s)                     |
| time_start     | int     | Start epoch (s)                          |
| time_end       | int     | End epoch (s)                            |
| nodes_alloc    | int     | Nodes allocated                          |
| cpus_req       | int     | CPUs requested                           |
| mem_mb         | int     | Memory requested (MB)                    |
| gpu_count      | int     | GPUs requested (from TRES + gres_used)   |
| partition      | string  | Slurm partition                          |
| job_type       | string  | Job type category                        |
| duration_s     | int     | Wall time (time_end − time_start)        |

### Key Statistics

| Metric              | Value             |
|---------------------|-------------------|
| Total slurm-log rows| 395,914           |
| COMPLETED (state=3) | 309,037 (78%)     |
| Clean traces        | 309,037           |
| Peak-sampled traces | 46,355            |
| Duration min        | 300 s             |
| Duration median     | 1,695 s           |
| Duration mean       | 7,323 s           |
| Duration max        | 872,851 s         |
| GPU jobs in sample  | 6,972             |
| Memory median       | 15,200 MB         |
| Memory max          | 70,720,000 MB     |

---

## Phase 1 — Telemetry Fusion & I/O Reconstruction

**File:** `pipeline_phase1.py`
**Inputs:** `clean_job_trace.csv`, `node-data.csv` (34.7M rows),
           `cpu/0000/*-timeseries.csv`, `gpu/0000/*.csv`,
           `labelled_jobids.csv`, `labelled_job_stats.csv`
**Outputs:** 6 artifacts in `artefacts/`

### Architecture & Memory Strategy

Phase 1 is designed to handle 34.7M node-data rows in ~62 MB RAM:

1. **Streaming** — `node-data.csv` read once with online aggregation per 5min bin
   (no raw row storage; only running sums/counts)
2. **Sweep-line** — active_jobs per bin computed O(N) job events, not O(bins × jobs)
3. **Pre-indexed I/O** — CPU/GPU timeseries grouped into bin buckets during load

### Artifact 1: `system_state_5min.csv`

**3.1 MB — 50,586 rows × 14 columns**

Layer 1 TiDE input — system-level 5-min aggregated telemetry.

| Column            | Description                                      |
|-------------------|--------------------------------------------------|
| time_bin          | 5-min epoch boundary (epoch//300 × 300)          |
| active_jobs       | Concurrent jobs (sweep-line, not node-derived)   |
| io_read_sum_mb    | Total read MB from CPU timeseries in bin         |
| io_write_sum_mb   | Total write MB                                   |
| io_p95_read_mb    | 95th percentile read sample (within bin)         |
| io_p95_write_mb   | 95th percentile write sample                     |
| io_max_read_mb    | Max single-sample read in bin                    |
| io_max_write_mb   | Max single-sample write                          |
| io_fano_read      | Dispersion index (Fano factor) of reads          |
| io_fano_write     | Dispersion index of writes                       |
| lustre_rpc_sum    | Sum of Lustre RPC totals from node-data          |
| load_avg_mean     | Mean LoadAvg per bin                             |
| sm_dip_count      | GPU SM utilization dips (<10%)                   |
| pcie_drop_count   | GPU PCIe link width drops (<16x)                 |

**Bin count:** 50,586 (time span 2018-01-28 to 2021-10-01)

### Artifact 2: `job_io_profile.csv`

**11.9 KB — 47 rows × 21 columns**

Per-job CPU I/O statistics for jobs with available cpu timeseries data.

| Column              | Description                          |
|---------------------|--------------------------------------|
| id_job              | Job ID                               |
| dnn_label           | DNN model label (UNKNOWN if missing) |
| duration_s          | Job wall time                        |
| num_samples_10s     | Number of 10s CPU samples            |
| read_mean_mb        | Mean read MB per sample              |
| read_p50/p95/max_mb | Read percentiles + max               |
| read_sum_mb         | Total read volume                    |
| write_mean/p50/p95  | Write statistics                     |
| read_fano           | Burstiness (Fano factor)             |
| write_fano          | Write burstiness                     |
| startup_read_avg    | First 10% of samples mean            |
| steady_read_avg     | Middle 80% samples mean              |
| term_read_avg       | Last 10% samples mean                |
| metadata_intensity  | "high" / "normal" classifier         |

### Artifact 3: `job_impact_dataset.csv`

**10.2 KB — 47 rows × 21 columns**

Layer 2 regressor dataset — combines job scheduling features with I/O profile.

| Column              | Source                  |
|---------------------|-------------------------|
| id_job, dnn_label   | From profile            |
| mem_mb, gpu_count    | From clean trace        |
| cpus_req, nodes_alloc| From clean trace        |
| partition, job_type  | From clean trace        |
| time_submit          | From clean trace        |
| est_duration_s       | From clean trace        |
| hist_read/write_*    | From I/O profile        |
| burstiness_fano      | max(read_fano, write_fano) |
| metadata_intensity   | From I/O profile        |

### Artifact 4: `threshold_calibration.csv`

**343 B — 9 rows × 3 columns**

| Metric                 | Value         | Method     |
|------------------------|---------------|------------|
| bw_knee_load           | 230           | kneedle    |
| bw_knee_io_MB_5min     | 2,978,202.89  | kneedle    |
| rpc_knee_load          | 0             | kneedle    |
| rpc_knee_value_5min    | 26,145,400,248| kneedle    |
| rpc_p50_5min           | 15,040,611    | percentile |
| rpc_p90_5min           | 61,142,743    | percentile |
| rpc_p95_5min           | 154,232,222   | percentile |
| rpc_p99_5min           | 937,344,815   | percentile |
| max_theoretical_bw_MB_s| 47,683.72     | physical   |

### Artifact 5: `guardrail_config.json`

Hard/soft thresholds for AI agent decision band:

```json
{
  "bw_threshold_soft_MB_per_5min": 2084742.02,
  "bw_threshold_hard_MB_per_5min": 2680382.6,
  "rpc_threshold_soft_per_5min":   48914194.4,
  "rpc_threshold_hard_per_5min":   73371291.6,
  "decision_band": {
    "description": "P90(sum) < soft → SUBMIT; P10(sum) > hard → HOLD; else gray-zone",
    "bw_axis": "OSS bandwidth (MB/5min)",
    "rpc_axis": "MDS metadata (RPC/5min)"
  },
  "ceiling_theoretical_bw_MB_s": 47683.72
}
```

### Artifact 6: `dnn_io_distributions.json`

Per-DNN-class I/O distributions. Currently 1 class (UNKNOWN) as the 47 jobs with
CPU data do not overlap with the labelled subset. Designed for synthetic augmentation.

### Phase 1 Statistics Summary

| Metric               | Value            |
|----------------------|------------------|
| Jobs in clean trace  | 46,355           |
| Jobs with CPU I/O    | 47               |
| Jobs with GPU traces | 18               |
| Jobs with I/O profile| 47               |
| Impact dataset rows  | 47               |
| System state bins    | 50,586           |
| Node-data streamed   | 34,655,892 rows  |
| Time span            | 1517137200 – 1633045800 (≈3.8 years) |

---

## Dataset Structure Reference

```
/root/202201/
├── context.txt              ← Full analysis: 5 data layers, AI agent use cases
├── slurm-log.csv            ← 395,914 raw Slurm records
├── tres-mapping.txt         ← TRES resource ID ↔ name mapping
├── labelled_jobids.csv      ← 3,430 job → DNN model labels
├── labelled_job_stats.csv   ← Per-model job count statistics
├── node-data.csv            ← 34.7M row node-level telemetry (5min)
├── cpu/0000/                ← CPU timeseries per job (10s, ~47 jobs)
│   └── {job_id}-timeseries.csv  (EpochTime, ReadMB, WriteMB, CPU, freq, RSS…)
├── gpu/0000/                ← GPU traces per job (100ms, ~18 jobs)
│   └── {job_id}.csv             (timestamp, utilization_gpu_pct, pcie_link_width…)
├── pipeline_phase0.py       ← Raw log → clean trace
├── pipeline_phase1.py       ← Telemetry fusion + calibration
├── artefacts/
│   ├── clean_job_trace.csv       Phase 0 output
│   ├── phase0_stats.txt
│   ├── system_state_5min.csv     Phase 1: Layer 1 TiDE input
│   ├── job_io_profile.csv        Phase 1: per-job I/O
│   ├── job_impact_dataset.csv    Phase 1: Layer 2 regressor
│   ├── threshold_calibration.csv Phase 1: calibrated thresholds
│   ├── guardrail_config.json     Phase 1: agent guardrails
│   ├── dnn_io_distributions.json Phase 1: DNN class stats
│   └── phase1_stats.txt          Phase 1: statistics log
```

## Agent Use-Case Coverage (from context.txt)

1. **Predictive agents** — runtime/failure prediction from timeseries
2. **Anomaly-detection agents** — RPC/Lustre behavior baselines via thresholds
3. **Root-cause agents** — cross-layer job-ID linking (scheduler → compute → GPU → Lustre)
4. **Scheduling agents** — carbon-aware scheduling from PUE/facility data
5. **Idle-GPU reclaimer** — SM utilization dips (<10%) directly from Phase 1 GPU data

---
*Generated: 2026-06-01 from pipeline_phase0.py + pipeline_phase1.py execution*