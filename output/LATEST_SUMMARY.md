# Phase 0 + Phase 1 Data Preprocessing Pipeline - Final Summary

**Date:** 2026-06-02  
**Dataset:** MIT Supercloud Dataset  
**Status:** ✅ Complete

---

## Pipeline Overview

The pipeline processes MIT Supercloud HPC job data into ML-ready datasets with two execution modes.

| Mode | Jobs | Duration | CPU Telemetry | GPU Telemetry | Processing Time |
|------|------|----------|---------------|---------------|-----------------|
| **Peak Window** | 73,400 | 41 days | 64 jobs (0.09%) | 16 jobs (0.02%) | ~24s |
| **Full Window** | 309,037 | 269 days | 319 jobs (0.10%) | 138 jobs (0.04%) | ~2min |

---

## Phase 0 — Core Data Prep

### Task 0.1: Minimal Loading ✅

**Input:** `slurm-log.csv` (395,914 rows)

**Action:** Load 8 scheduling columns:
- `id_job`, `state`, `time_submit`, `time_start`, `time_end`, `mem_req`, `gres_used`, `nodes_alloc`

**Output:** 309,037 jobs loaded

**Metrics:**
| Metric | Value |
|--------|-------|
| Total jobs | 395,914 |
| Columns loaded | 8 |
| COMPLETED jobs | 309,037 |
| Non-COMPLETED | 86,877 |
| Load time | 5.8s |

**Command:**
```bash
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv
```

---

### Task 0.2: Filtering ✅

**Action:** Filter state == 3 (COMPLETED)

**Reason:** Full lifecycle needed for duration modeling

**Output:** 309,037 COMPLETED jobs

---

### Task 0.3: Time Window Sampling ✅

**Two modes available:**

#### Peak Window (Default)
- Window: 2021-04-21 to 2021-06-01 (41 days)
- Jobs: 73,400
- Time bins: 17,281
- Mean active jobs: 224.5
- Max active jobs: 833

**Command:**
```bash
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv
```

#### Full Window
- Window: 2021-01-05 to 2021-10-01 (269 days)
- Jobs: 309,037
- Time bins: 77,473
- Mean active jobs: 189.7
- Max active jobs: 837

**Command:**
```bash
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_full_jobs.csv --full-window
```

---

## Phase 1 — Telemetry Fusion

### Task 1.1: CPU Telemetry Processing ✅

**Input:** `cpu/0000/*.csv` (408 files)

**Action:** Aggregate 10s samples → 5-min bins

**Peak Output:**
| Metric | Value |
|--------|-------|
| Jobs with CPU | 64 |
| Coverage | 0.09% |
| Aggregated rows | 3,970 |

**Full Output:**
| Metric | Value |
|--------|-------|
| Jobs with CPU | 319 |
| Coverage | 0.10% |
| Aggregated rows | 12,831 |

**Columns:** ReadMB_sum/max, WriteMB_sum/max, CPUUtilization_mean/max, RSS_max, VMSize_max

**Command:**
```bash
# Peak
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_cpu_5min.parquet

# Full
python3 src/phase1_cpu_processor.py --jobs output/phase0_full_jobs.csv --output output/job_cpu_5min_full.parquet
```

---

### Task 1.2: GPU Telemetry Processing ✅

**Input:** `gpu/0000/*.csv` (223 files, 192 unique jobs)

**Action:** Downsample 100ms → 5-min bins

**Peak Output:**
| Metric | Value |
|--------|-------|
| Jobs with GPU | 16 |
| Coverage | 0.02% |
| Aggregated rows | 620 |

**Full Output:**
| Metric | Value |
|--------|-------|
| Jobs with GPU | 138 |
| Coverage | 0.04% |
| Aggregated rows | 10,610 |

**Columns:** utilization_gpu_pct_mean/max, memory_used_MiB_max, power_draw_W_mean, temperature_gpu_max

**Key Finding:** `gres_used` column 100% NULL → GPU jobs identified by file matching

**Command:**
```bash
# Peak
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_gpu_5min.parquet

# Full
python3 src/phase1_gpu_processor.py --jobs output/phase0_full_jobs.csv --output output/job_gpu_5min_full.parquet
```

---

### Task 1.3: Node Metrics Join ⚠️

**Status:** Disabled (nodelist column not loaded)

**Output:** Empty schema

---

### Task 1.4: System State Builder ✅

**Action:** Build cluster-wide time series @5min

**Peak Output:**
| Metric | Value |
|--------|-------|
| Time bins | 17,281 |
| Mean active jobs | 224.5 |
| Max active jobs | 833 |
| Columns | 14 |

**Columns:** bin_timestamp, bin_index, active_jobs, cpu_ReadMB_sum, cpu_WriteMB_sum, cpu_ReadMB_max, cpu_WriteMB_max, cpu_CPUUtilization_mean, cpu_CPUUtilization_max, cpu_RSS_max, gpu_utilization_gpu_pct_mean, gpu_utilization_gpu_pct_max, gpu_memory_used_MiB_max, gpu_power_draw_W_mean

**Command:**
```bash
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --gpu output/job_gpu_5min.parquet --output output/system_state_5min.parquet
```

---

### Task 1.5: Job-Level Features ✅

**Action:** Extract per-job features for ML

**Peak Output:**
| Metric | Value |
|--------|-------|
| Total jobs | 73,400 |
| Features | 24 |
| Jobs with CPU | 64 |
| Jobs with GPU | 16 |
| Labeled jobs | 1,211 (1.65%) |
| Need reconstruction | 73,336 (99.91%) |

**Full Output:**
| Metric | Value |
|--------|-------|
| Total jobs | 309,037 |
| Features | 24 |
| Jobs with CPU | 319 |
| Jobs with GPU | 138 |
| Labeled jobs | 3,414 (1.10%) |
| Need reconstruction | 308,718 (99.90%) |

**Duration Stats (Peak):**
- Mean run time: 4.4 hours
- Mean wait time: 348.2 minutes

**Duration Stats (Full):**
- Mean run time: 4.0 hours
- Mean wait time: 304.0 minutes

**Command:**
```bash
# Peak
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --gpu output/job_gpu_5min.parquet --labels labelled_jobids.csv --output output/job_features.parquet

# Full
python3 src/phase1_job_features.py --jobs output/phase0_full_jobs.csv --cpu output/job_cpu_5min_full.parquet --gpu output/job_gpu_5min_full.parquet --labels labelled_jobids.csv --output output/job_features_full.parquet
```

---

## Output Files

### Peak Window (73,400 jobs)

| File | Size | Rows | Description |
|------|------|------|-------------|
| `phase0_peak_jobs.csv` | 8.3 MB | 73,400 | Peak window jobs |
| `job_cpu_5min.parquet` | 203 KB | 3,970 | CPU telemetry |
| `job_gpu_5min.parquet` | 33 KB | 620 | GPU telemetry |
| `system_state_5min.csv` | 975 KB | 17,281 | System state |
| `system_state_5min.parquet` | 249 KB | 17,281 | System state |
| `job_features.parquet` | 1.3 MB | 73,400 | Job features |

### Full Window (309,037 jobs)

| File | Size | Rows | Description |
|------|------|------|-------------|
| `phase0_full_jobs.csv` | 35 MB | 309,037 | Full window jobs |
| `job_cpu_5min_full.parquet` | 518 KB | 12,831 | CPU telemetry |
| `job_gpu_5min_full.parquet` | 418 KB | 10,610 | GPU telemetry |
| `job_features_full.parquet` | 4.3 MB | 309,037 | Job features |

### Reports

| File | Description |
|------|-------------|
| `pipeline_summary.md` | Detailed pipeline documentation |
| `window_comparison.md` | Peak vs Full comparison |
| `cpu_only_jobs.md` | 64 CPU telemetry jobs |
| `gpu_only_jobs.md` | 15 GPU telemetry jobs |
| `telemetry_jobs.md` | 79 jobs with any telemetry |

---

## Key Findings

### 1. Telemetry Coverage Limited
- **CPU**: 0.09-0.10% of jobs have telemetry
- **GPU**: 0.02-0.04% of jobs have telemetry
- **No overlap**: CPU and GPU jobs are disjoint sets

### 2. GPU Jobs Identified by File Matching
- `gres_used` column 100% NULL in slurm-log
- GPU jobs found by matching telemetry filenames
- 138 GPU jobs in full window vs 16 in peak

### 3. Temporal Dependencies Preserved
- Peak window preserves convoy effect and burst behavior
- Time-based sampling (not random) for TiDE forecasting
- 5-minute bin resolution matches node-data granularity

### 4. I/O Reconstruction Needed
- 99.90% of jobs need synthetic I/O generation
- Calibrate from 319 observed jobs
- Fit distribution by job characteristics

### 5. Label Coverage Low
- 1.10-1.65% of jobs labeled
- Labels: DNN models (vgg16, resnet50, inception3), U-series
- 98%+ marked as "unknown"

---

## Quick Start

### Run Peak Window (Recommended for ML)
```bash
bash run_pipeline.sh
```

### Run Full Window (For Analysis)
```bash
bash run_pipeline_full.sh
```

### Individual Modules
```bash
# Phase 0
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv

# Phase 1
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_cpu_5min.parquet
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_gpu_5min.parquet
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --output output/system_state_5min.parquet
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --output output/job_features.parquet
```

---

## Execution Timeline

| Phase | Task | Duration | Status |
|-------|------|----------|--------|
| 0 | Load slurm-log | 5.8s | ✅ |
| 0 | Filter COMPLETED | 0.1s | ✅ |
| 0 | Peak sampling | 4.6s | ✅ |
| 1 | CPU processing | 1.5s | ✅ |
| 1 | GPU processing | 2.7s | ✅ |
| 1 | Node join | 0.7s | ⚠️ Disabled |
| 1 | System state | 10s | ✅ |
| 1 | Job features | 1.1s | ✅ |
| **Total** | **All phases** | **~27s** | **✅ Complete** |

---

## Recommendations

### For ML Training
- Use **peak window** (73K jobs, faster iteration)
- Use `system_state_5min.csv` for cluster forecasting
- Use `job_features.parquet` for job-level prediction
- Filter to 79 jobs with telemetry for validation

### For Production
- Add `nodelist` column for node metrics join
- Implement I/O reconstruction for 99.9% of jobs
- Expand labeling coverage
- Consider broader time window if more telemetry needed

---

**Pipeline Status:** ✅ Complete  
**Total Output Size:** ~47 MB (peak), ~94 MB (full)  
**Processing Time:** ~27s (peak), ~2min (full)
