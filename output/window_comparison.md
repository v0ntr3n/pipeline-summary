# Peak Window vs Full Window Comparison

## Overview

| Metric | Peak Window (21/04-01/06) | Full Window (Jan-Oct) |
|--------|---------------------------|------------------------|
| **Jobs** | 73,400 | 309,037 |
| **Duration** | 41 days | 269 days |
| **CPU telemetry jobs** | 64 (0.09%) | 319 (0.10%) |
| **GPU telemetry jobs** | 16 (0.02%) | 138 (0.04%) |
| **Labeled jobs** | 1,211 (1.65%) | 3,414 (1.10%) |
| **Mean run time** | 4.4 hours | 4.0 hours |
| **Mean wait time** | 348.2 min | 304.0 min |

## Telemetry Coverage

### CPU Telemetry
| Window | Jobs | Rows | Coverage |
|--------|------|------|----------|
| Peak | 64 | 3,970 | 0.09% |
| Full | 319 | 12,831 | 0.10% |

**Improvement with full window:**
- **+255 CPU jobs** (5× more than peak)
- **+8,861 rows** of aggregated metrics

### GPU Telemetry
| Window | Jobs | Rows | Coverage |
|--------|------|------|----------|
| Peak | 16 | 620 | 0.02% |
| Full | 138 | 10,610 | 0.04% |

**Improvement with full window:**
- **+122 GPU jobs** (8.6× more than peak)
- **+9,990 rows** of aggregated metrics

## When to Use Each

### Use Peak Window (Recommended for ML Training)
- **Smaller dataset** (73K jobs vs 309K)
- **Faster processing** (~24s vs ~2min)
- **More focused** - sustained high-load period
- **Better for TiDE** - manageable time bins (11K vs 77K)
- **Slightly higher label density** (1.65% vs 1.10%)

**Use peak window when:**
- Training TiDE forecasting model
- Need faster iteration
- Want representative workload sample
- Limited compute resources

### Use Full Window (Recommended for Analysis)
- **More telemetry coverage** (319 CPU vs 64, 138 GPU vs 16)
- **Complete dataset** - all available jobs
- **Better for analysis** - see all workload types
- **More labeled jobs** (3,414 vs 1,211)

**Use full window when:**
- Need maximum telemetry coverage
- Analyzing workload distribution
- Building comprehensive dataset
- Want all labeled jobs

## Recommendation

**For ML model training:**
- Use **peak window** (smaller, faster, focused)

**For exploratory analysis:**
- Use **full window** (more telemetry, all jobs)

**For production pipeline:**
- Support both via `--full-window` flag
