# Phase 0 + Phase 1 Data Preprocessing Pipeline - Complete Summary

## Pipeline Options

The pipeline now supports **two modes**:

| Mode | Flag | Jobs | Duration | Best For |
|------|------|------|----------|----------|
| **Peak Window** | (default) | 73,400 | 41 days | ML training, fast iteration |
| **Full Window** | `--full-window` | 309,037 | 269 days | Analysis, max telemetry |

---

## Phase 0 — Core Data Prep

### Task 0.1: Minimal Loading

**Input:** `slurm-log.csv` (395,914 rows)

**Action:** Load 8 scheduling-essential columns:
- `id_job`, `state`, `time_submit`, `time_start`, `time_end`, `mem_req`, `gres_used`, `nodes_alloc`

**Output:** 309,037 jobs loaded

**Metrics:**
| Metric | Value |
|--------|-------|
| Total jobs in slurm-log | 395,914 |
| Columns loaded | 8 |
| Load time | ~5.8s |

**Command:**
```bash
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv
```

---

### Task 0.2: Filtering

**Action:** Filter for COMPLETED jobs only (state == 3)

**Reason:** Need full lifecycle (submit → start → end) for duration modeling. Failed/cancelled jobs have broken timelines.

**Output:** 309,037 COMPLETED jobs

**Metrics:**
| Metric | Value |
|--------|-------|
| COMPLETED jobs | 309,037 |
| Non-COMPLETED removed | 86,877 |
| Invalid timestamps removed | 0 |

---

### Task 0.3: Time Window Sampling

**Two options available:**

#### Option 1: Peak Window (Default)

**Window:** 2021-04-21 to 2021-06-01 (41 days)

**Method:** 
- Time-window based: keep all jobs overlapping with peak window
- Peak measured by `active_jobs` (I/O contention proxy)
- Represents sustained high-load period

**Output:** 73,400 jobs

**Metrics:**
| Metric | Value |
|--------|-------|
| Peak window jobs | 73,400 |
| Window duration | 41 days |
| Time bins @5min | 11,809 |
| Mean active jobs | 315.7 |
| Max active jobs | 833 |

**Command:**
```bash
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv
```

#### Option 2: Full Window

**Window:** 2021-01-05 to 2021-10-01 (269 days)

**Method:** Use all COMPLETED jobs in dataset

**Output:** 309,037 jobs

**Metrics:**
| Metric | Value |
|--------|-------|
| Full window jobs | 309,037 |
| Window duration | 269 days |
| Time bins @5min | 77,473 |
| Mean active jobs | 189.7 |
| Max active jobs | 837 |

**Command:**
```bash
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_full_jobs.csv --full-window
```

**Rationale for peak window:**
- Preserves temporal dependencies for TiDE forecasting
- Maintains convoy effect and burst behavior
- Represents sustained high-load period (not just spike)
- Smaller dataset for faster ML iteration

---

## Phase 1 — Empirical Telemetry Fusion

### Task 1.1: CPU Telemetry Processing

**Input:** 
- `cpu/0000/*.csv` (408 files total)
- Jobs CSV (peak or full)

**Action:** 
- Load CPU timeseries (10s resolution)
- Aggregate to 5-minute bins
- Compute: ReadMB_sum/max, WriteMB_sum/max, CPUUtilization_mean/max, RSS_max, VMSize_max

**Peak Window Output:**
| Metric | Value |
|--------|-------|
| Jobs with CPU data | 64 |
| Coverage | 0.09% |
| Aggregated rows | 3,970 |

**Full Window Output:**
| Metric | Value |
|--------|-------|
| Jobs with CPU data | 319 |
| Coverage | 0.10% |
| Aggregated rows | 12,831 |

**Command:**
```bash
# Peak
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_cpu_5min.parquet

# Full
python3 src/phase1_cpu_processor.py --jobs output/phase0_full_jobs.csv --output output/job_cpu_5min_full.parquet
```

---

### Task 1.2: GPU Telemetry Processing

**Input:**
- `gpu/0000/*.csv` (223 files, 192 unique jobs)
- Jobs CSV

**Action:**
- Load GPU timeseries (100ms resolution)
- Downsample to 5-minute bins
- Compute: utilization_gpu_pct_mean/max, memory_used_MiB_max, power_draw_W_mean, temperature_gpu_max

**Peak Window Output:**
| Metric | Value |
|--------|-------|
| Jobs with GPU data | 16 |
| Coverage | 0.02% |
| Aggregated rows | 620 |

**Full Window Output:**
| Metric | Value |
|--------|-------|
| Jobs with GPU data | 138 |
| Coverage | 0.04% |
| Aggregated rows | 10,610 |

**Key Finding:**
- `gres_used` column in slurm-log is 100% NULL
- GPU jobs identified by matching telemetry file job IDs
- No overlap with CPU telemetry jobs (disjoint sets)

**Command:**
```bash
# Peak
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_gpu_5min.parquet

# Full
python3 src/phase1_gpu_processor.py --jobs output/phase0_full_jobs.csv --output output/job_gpu_5min_full.parquet
```

---

### Task 1.3: Node Metrics Join

**Status:** Disabled - `nodelist` column not in minimal loading

**Output:** Empty schema

---

### Task 1.4: System State Builder

**Action:** Build cluster-wide time series at 5-minute resolution

**Peak Window Output:**
| Metric | Value |
|--------|-------|
| Time bins | 11,809 |
| Mean active jobs | 315.7 |
| Max active jobs | 833 |

**Full Window Output:**
| Metric | Value |
|--------|-------|
| Time bins | 77,473 |
| Mean active jobs | 189.7 |
| Max active jobs | 837 |

**Note:** Full window system state builder is slow due to 77K bins. Use peak window for ML training.

**Command:**
```bash
# Peak
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --output output/system_state_5min.parquet

# Full (slow - 77K bins)
python3 src/phase1_system_state.py --jobs output/phase0_full_jobs.csv --output output/system_state_5min_full.parquet
```

---

### Task 1.5: Job-Level Feature Extraction

**Input:**
- Jobs CSV (peak or full)
- CPU telemetry parquet
- GPU telemetry parquet
- `labelled_jobids.csv`

**Action:** Extract per-job features for ML

**Peak Window Output:**
| Metric | Value |
|--------|-------|
| Total jobs | 73,400 |
| Features per job | 18 |
| Jobs with CPU features | 64 |
| Jobs with GPU features | 16 |
| Jobs with labels | 1,211 (1.65%) |
| Jobs needing reconstruction | 73,336 |

**Full Window Output:**
| Metric | Value |
|--------|-------|
| Total jobs | 309,037 |
| Features per job | 24 |
| Jobs with CPU features | 319 |
| Jobs with GPU features | 138 |
| Jobs with labels | 3,414 (1.10%) |
| Jobs needing reconstruction | 308,718 |

**Duration Statistics:**
| Statistic | Peak | Full |
|-----------|------|------|
| Mean run time | 4.4 hours | 4.0 hours |
| Mean wait time | 348.2 min | 304.0 min |

**Command:**
```bash
# Peak
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --output output/job_features.parquet

# Full
python3 src/phase1_job_features.py --jobs output/phase0_full_jobs.csv --output output/job_features_full.parquet
```

---

## Comparison: Peak vs Full

| Metric | Peak Window | Full Window | Improvement |
|--------|-------------|-------------|-------------|
| Jobs | 73,400 | 309,037 | 4.2× more |
| CPU telemetry jobs | 64 | 319 | 5× more |
| GPU telemetry jobs | 16 | 138 | 8.6× more |
| Labeled jobs | 1,211 | 3,414 | 2.8× more |
| Processing time | ~24s | ~2min | - |
| Time bins | 11,809 | 77,473 | - |

**Recommendation:**
- Use **peak window** for ML training (faster, focused)
- Use **full window** for analysis (more telemetry, all jobs)

---

## Output Files

### Peak Window

| File | Size | Rows | Description |
|------|------|------|-------------|
| `phase0_completed_jobs.csv` | 35 MB | 309,037 | All COMPLETED jobs |
| `phase0_peak_jobs.csv` | 8.3 MB | 73,400 | Peak window jobs |
| `job_cpu_5min.parquet` | 203 KB | 3,970 | CPU telemetry |
| `job_gpu_5min.parquet` | 6.4 KB | 620 | GPU telemetry |
| `system_state_5min.parquet` | 241 KB | 11,809 | System state |
| `job_features.parquet` | 1.3 MB | 73,400 | Job features |

### Full Window

| File | Size | Rows | Description |
|------|------|------|-------------|
| `phase0_full_jobs.csv` | 35 MB | 309,037 | Full window jobs |
| `job_cpu_5min_full.parquet` | 554 KB | 12,831 | CPU telemetry |
| `job_gpu_5min_full.parquet` | 33 KB | 10,610 | GPU telemetry |
| `job_features_full.parquet` | 5.2 MB | 309,037 | Job features |

---

## Quick Start

### Run Peak Window (Default)
```bash
bash run_pipeline.sh
```

### Run Full Window
```bash
bash run_pipeline_full.sh
```

### Run Individual Modules
```bash
# Phase 0
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv
# OR
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_full_jobs.csv --full-window

# Phase 1
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_cpu_5min.parquet
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_gpu_5min.parquet
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --output output/system_state_5min.parquet
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --output output/job_features.parquet
```

---

*Pipeline supports both peak and full window modes*  
*Peak: 73,400 jobs, 24s processing*  
*Full: 309,037 jobs, 2min processing*
