# Phase 0 + Phase 1 Data Preprocessing Pipeline - Execution Summary

## Overview

**Dataset:** MIT Supercloud Dataset  
**Peak Window:** 2021-04-21 to 2021-06-01 (41 days)  
**Total Jobs Processed:** 73,400

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

### Task 0.3: Representative Peak Sampling

**Action:** Select jobs from sustained load window (2021-04-21 to 2021-06-01)

**Method:** 
- NOT random sampling (breaks temporal dependencies)
- NOT percentage-based (breaks convoy effect)
- Time-window based: keep all jobs overlapping with peak window
- Peak measured by `active_jobs` (I/O contention proxy)

**Output:** 73,400 jobs in peak window

**Metrics:**
| Metric | Value |
|--------|-------|
| Peak window jobs | 73,400 |
| Window duration | 41 days |
| Time bins @5min | 11,809 |
| Mean active jobs | 315.7 |
| Max active jobs | 833 |

**Rationale:**
- Preserves temporal dependencies for TiDE forecasting
- Maintains convoy effect and burst behavior
- Represents sustained high-load period (not just spike)

**Command:**
```bash
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv
```

**Output Files:**
- `output/phase0_completed_jobs.csv` (35 MB) - All COMPLETED jobs
- `output/phase0_peak_jobs.csv` (8.3 MB) - Peak window jobs

---

## Phase 1 — Empirical Telemetry Fusion & I/O Reconstruction

### Task 1.1: CPU Telemetry Processing

**Input:** 
- `cpu/0000/*.csv` (408 files total)
- `output/phase0_peak_jobs.csv` (73,400 jobs)

**Action:** 
- Load CPU timeseries (10s resolution)
- Aggregate to 5-minute bins
- Compute: ReadMB_sum/max, WriteMB_sum/max, CPUUtilization_mean/max, RSS_max, VMSize_max

**Output:** 64 jobs with CPU telemetry (0.09% coverage)

**Metrics:**
| Metric | Value |
|--------|-------|
| CPU telemetry files in dataset | 408 |
| Jobs in peak window with CPU data | 64 |
| Coverage | 0.09% |
| Aggregated rows | 3,970 |
| Processing time | ~1.5s |

**Data Quality:**
- 100% of CPU telemetry files successfully parsed
- No empty files encountered
- All 64 jobs marked as "observed" provenance

**Command:**
```bash
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --cpu-dir cpu --output output/job_cpu_5min.parquet
```

**Output Files:**
- `output/job_cpu_5min.parquet` (203 KB) - Aggregated CPU metrics
- `output/cpu_missing_jobs.csv` (2.0 MB) - Jobs needing I/O reconstruction

---

### Task 1.2: GPU Telemetry Processing

**Input:**
- `gpu/0000/*.csv` (223 files, 192 unique jobs)
- `output/phase0_peak_jobs.csv` (73,400 jobs)

**Action:**
- Load GPU timeseries (100ms resolution)
- Downsample to 5-minute bins
- Compute: utilization_gpu_pct_mean/max, memory_used_MiB_max, power_draw_W_mean, temperature_gpu_max

**Output:** 16 jobs with GPU telemetry (0.02% coverage)

**Metrics:**
| Metric | Value |
|--------|-------|
| GPU telemetry files in dataset | 223 |
| Jobs in peak window with GPU data | 16 |
| Coverage | 0.02% |
| Aggregated rows | 620 |
| Processing time | ~2.7s |

**Key Finding:**
- `gres_used` column in slurm-log is 100% NULL for peak window
- GPU jobs identified by matching telemetry file job IDs
- No overlap with CPU telemetry jobs (disjoint sets)

**Command:**
```bash
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --gpu-dir gpu --output output/job_gpu_5min.parquet
```

**Output Files:**
- `output/job_gpu_5min.parquet` (6.4 KB) - Aggregated GPU metrics

---

### Task 1.3: Node Metrics Join

**Input:**
- `node-data.csv` (34.6M rows)
- `output/phase0_peak_jobs.csv` (73,400 jobs)

**Action:** Join node metrics to jobs by node ID + time window overlap

**Output:** 0 jobs matched

**Reason:** `nodelist` column not loaded in minimal columns (only 8 scheduling columns)

**Decision:** Node join disabled - requires `nodelist` column which was excluded from minimal loading

**Output File:**
- `output/job_node_metrics.parquet` (6.4 KB) - Empty schema

---

### Task 1.4: System State Builder

**Input:**
- `output/phase0_peak_jobs.csv` (73,400 jobs)
- `output/job_cpu_5min.parquet` (64 jobs)
- `output/job_gpu_5min.parquet` (16 jobs)

**Action:** Build cluster-wide time series at 5-minute resolution

**Output:** 11,809 bins with active_jobs and CPU/GPU metrics

**Metrics:**
| Metric | Value |
|--------|-------|
| Time bins | 11,809 |
| Window | 2021-04-21 to 2021-06-01 |
| Mean active jobs | 315.7 |
| Max active jobs | 833 |
| Bins with CPU data | 883 |
| Columns | 10 |
| Processing time | ~7.3s |

**Columns:**
- `bin_timestamp`, `bin_index`, `active_jobs`
- `total_ReadMB_sum`, `total_WriteMB_sum`
- `avg_cpu_utilization_mean`, `avg_cpu_utilization_max`
- `total_RSS_max_sum`

**Command:**
```bash
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --output output/system_state_5min.parquet
```

**Output File:**
- `output/system_state_5min.parquet` (241 KB) - System-wide time series

---

### Task 1.5: Job-Level Feature Extraction

**Input:**
- `output/phase0_peak_jobs.csv` (73,400 jobs)
- `output/job_cpu_5min.parquet` (64 jobs)
- `output/job_gpu_5min.parquet` (16 jobs)
- `labelled_jobids.csv` (3,430 labeled jobs)

**Action:** Extract per-job features for ML

**Output:** 73,400 jobs × 18 features

**Metrics:**
| Metric | Value |
|--------|-------|
| Total jobs | 73,400 |
| Features per job | 18 |
| Jobs with CPU features | 64 |
| Jobs with GPU features | 16 |
| Jobs with labels | 1,211 (1.6%) |
| Jobs needing reconstruction | 73,336 |
| Processing time | ~1.1s |

**Duration Statistics:**
| Statistic | Value |
|-----------|-------|
| Mean run time | 4.4 hours |
| Mean wait time | 348.2 minutes |
| Min run time | 0.1 hours |
| Max run time | 2,073 hours |

**Label Distribution:**
| Workload Type | Count | Percentage |
|---------------|-------|------------|
| unknown | 72,189 | 98.4% |
| U3-32 | 111 | 0.2% |
| U3-128 | 111 | 0.2% |
| U4-32 | 109 | 0.1% |
| U5-64 | 106 | 0.1% |
| U3-64 | 105 | 0.1% |
| U5-32 | 105 | 0.1% |
| U4-64 | 104 | 0.1% |
| U4-128 | 103 | 0.1% |
| U5-128 | 102 | 0.1% |
| vgg16 | 91 | 0.1% |
| resnet50 | 83 | 0.1% |
| inception3 | 81 | 0.1% |

**Command:**
```bash
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --gpu output/job_gpu_5min.parquet --labels labelled_jobids.csv --output output/job_features.parquet
```

**Output File:**
- `output/job_features.parquet` (1.3 MB) - Job-level features

---

## Summary Statistics

### Data Flow

```
slurm-log.csv (395,914 jobs)
    ↓ Phase 0.1: Load 8 columns
phase0_completed_jobs.csv (309,037 COMPLETED)
    ↓ Phase 0.2: Filter COMPLETED
    ↓ Phase 0.3: Peak sampling
phase0_peak_jobs.csv (73,400 in window)
    ↓ Phase 1.1: CPU telemetry join
    ↓ Phase 1.2: GPU telemetry join
    ↓ Phase 1.5: Feature extraction
job_features.parquet (73,400 × 18)
```

### Telemetry Coverage

| Telemetry Type | Jobs | Coverage | Source |
|----------------|------|----------|--------|
| CPU | 64 | 0.09% | cpu/0000/*.csv |
| GPU | 16 | 0.02% | gpu/0000/*.csv |
| Both CPU+GPU | 0 | 0% | Disjoint sets |
| Labels | 1,211 | 1.6% | labelled_jobids.csv |
| Node metrics | 0 | 0% | nodelist not loaded |

### Output Files

| File | Size | Rows | Description |
|------|------|------|-------------|
| `phase0_completed_jobs.csv` | 35 MB | 309,037 | All COMPLETED jobs |
| `phase0_peak_jobs.csv` | 8.3 MB | 73,400 | Peak window jobs |
| `job_cpu_5min.parquet` | 203 KB | 3,970 | CPU telemetry aggregated |
| `job_gpu_5min.parquet` | 6.4 KB | 620 | GPU telemetry aggregated |
| `job_node_metrics.parquet` | 6.4 KB | 0 | Empty (nodelist missing) |
| `system_state_5min.parquet` | 241 KB | 11,809 | System-wide time series |
| `job_features.parquet` | 1.3 MB | 73,400 | Job-level features |
| `cpu_missing_jobs.csv` | 2.0 MB | 73,336 | Jobs needing I/O reconstruction |

### Exported Reports

| File | Jobs | Description |
|------|------|-------------|
| `cpu_only_jobs.md` | 64 | CPU telemetry jobs with I/O metrics |
| `gpu_only_jobs.md` | 15 | GPU telemetry jobs with GPU metrics |
| `observed_jobs.md` | 64 | Same as CPU-only (observed provenance) |
| `telemetry_jobs.md` | 79 | All jobs with any telemetry |

---

## Key Findings

### 1. Limited Telemetry Availability
- Only 0.09% of peak window jobs have CPU telemetry
- Only 0.02% have GPU telemetry
- **No jobs have both CPU and GPU telemetry simultaneously**

### 2. GPU Jobs Identified by File Matching
- `gres_used` column is 100% NULL in slurm-log
- GPU jobs identified by matching telemetry file names to job IDs
- 16 GPU jobs found in peak window

### 3. Sustained Load Window Characteristics
- Mean active jobs: 315.7
- Max active jobs: 833
- Represents sustained high-load period (not just spike)
- Preserves temporal dependencies for forecasting

### 4. I/O Reconstruction Needed
- 73,336 jobs (99.91%) need I/O reconstruction
- Use calibration from 64 observed jobs
- Fit distribution from job characteristics (mem_req, duration, nodes)

### 5. Label Coverage
- Only 1.6% of jobs have workload type labels
- Labels include DNN models (vgg16, resnet50, inception3) and U-series jobs
- 98.4% marked as "unknown"

---

## Recommendations

### For Production Use

1. **Add nodelist column** to minimal loading for node metrics join
2. **Implement I/O reconstruction** for 99.91% of jobs without telemetry
3. **Expand labeling** to cover more workload types
4. **Consider broader time window** if more telemetry coverage needed

### For ML Training

1. Use `system_state_5min.parquet` for cluster-level forecasting
2. Use `job_features.parquet` for job-level prediction
3. Filter to 79 jobs with telemetry for ground truth validation
4. Use reconstruction for remaining 99.91% of training data

---

## Execution Timeline

| Phase | Task | Duration | Status |
|-------|------|----------|--------|
| 0 | Load slurm-log | 5.8s | ✅ Complete |
| 0 | Filter COMPLETED | 0.1s | ✅ Complete |
| 0 | Peak sampling | 4.6s | ✅ Complete |
| 1 | CPU processing | 1.5s | ✅ Complete |
| 1 | GPU processing | 2.7s | ✅ Complete |
| 1 | Node join | 0.7s | ✅ Complete (empty) |
| 1 | System state | 7.3s | ✅ Complete |
| 1 | Job features | 1.1s | ✅ Complete |
| **Total** | **All phases** | **~24s** | **✅ Complete** |

---

## Commands Reference

```bash
# Phase 0
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv

# Phase 1
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_cpu_5min.parquet
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --output output/job_gpu_5min.parquet
python3 src/phase1_node_joiner.py --jobs output/phase0_peak_jobs.csv --node-data node-data.csv --output output/job_node_metrics.parquet
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --output output/system_state_5min.parquet
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --output output/job_features.parquet

# Run all
bash run_pipeline.sh
```

---

*Pipeline completed: 2026-06-02*  
*Total execution time: ~24 seconds*  
*Output size: ~47 MB*
