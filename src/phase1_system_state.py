#!/usr/bin/env python3
"""Phase 1.4: Build system-wide time series at 5-min resolution."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from typing import Dict, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    BIN_SIZE_SECONDS,
    PEAK_WINDOW_START,
    PEAK_WINDOW_END,
    EXPECTED_SYSTEM_STATE_BINS,
    MAX_ACTIVE_JOBS,
)
from src.logger import setup_logger, log_metric
from src.utils import (
    create_time_bins,
    parse_timestamp_series,
    timestamp_to_bin,
    bin_to_timestamp,
)


def build_system_state(
    jobs_path: str,
    cpu_path: Optional[str] = None,
    gpu_path: Optional[str] = None,
    node_path: Optional[str] = None,
    window_start: str = PEAK_WINDOW_START,
    window_end: str = PEAK_WINDOW_END,
    output_path: str = "output/system_state_5min.parquet",
) -> pd.DataFrame:
    """Build system-wide time series at 5-min resolution.
    
    Per-bin aggregation across all active jobs:
    - active_jobs: count jobs running in this bin
    - total_ReadMB_sum, total_WriteMB_sum: I/O totals
    - total_LustreRPC_sum, avg_FSlatency_p95: contention metrics
    - total_memory_used_sum, avg_CPU_utilization: resource usage
    
    Args:
        jobs_path: Path to phase0_peak_jobs.csv
        cpu_path: Path to job_cpu_5min.parquet (optional)
        gpu_path: Path to job_gpu_5min.parquet (optional)
        node_path: Path to job_node_metrics.parquet (optional)
        window_start: Start of time window
        window_end: End of time window
        output_path: Output Parquet path
    
    Returns:
        DataFrame with system state per 5-min bin
    """
    logger = setup_logger()
    logger.info("Building system state time series")
    
    # Create time bins
    time_bins = create_time_bins(window_start, window_end)
    logger.info(f"Created {len(time_bins):,} time bins from {window_start} to {window_end}")
    
    # Initialize system state DataFrame
    system_state = pd.DataFrame({
        "bin_timestamp": time_bins,
        "bin_index": range(len(time_bins)),
    })
    
    # Load jobs
    logger.info(f"Loading jobs from {jobs_path}")
    jobs_df = pd.read_csv(jobs_path)
    
    # Parse timestamps (handle string format from CSV)
    for col in ["time_submit", "time_start", "time_end"]:
        if jobs_df[col].dtype == "object":
            jobs_df[col] = pd.to_datetime(jobs_df[col], utc=True)
        elif jobs_df[col].dtype == "int64":
            jobs_df[col] = parse_timestamp_series(jobs_df[col])
    
    # Compute active jobs per bin
    logger.info("Computing active jobs per bin...")
    active_counts = []
    
    for bin_time in time_bins:
        active = (
            (jobs_df["time_start"] <= bin_time) &
            (jobs_df["time_end"] > bin_time)
        )
        active_counts.append(active.sum())
    
    system_state["active_jobs"] = active_counts
    
    # Statistics
    mean_active = system_state["active_jobs"].mean()
    max_active = system_state["active_jobs"].max()
    
    logger.info(f"Active jobs statistics:")
    logger.info(f"  Mean: {mean_active:.1f}")
    logger.info(f"  Max: {max_active}")
    logger.info(f"  Bins with 0 jobs: {(system_state['active_jobs'] == 0).sum()}")
    
    log_metric(logger, "mean_active_jobs", round(mean_active, 1))
    log_metric(logger, "max_active_jobs", max_active)
    
    # Load CPU telemetry if available
    if cpu_path and Path(cpu_path).exists():
        logger.info(f"Loading CPU telemetry from {cpu_path}")
        cpu_df = pd.read_parquet(cpu_path)
        
        if not cpu_df.empty:
            # Aggregate CPU metrics per bin
            cpu_agg = cpu_df.groupby("bin_index").agg({
                "ReadMB_sum": ["sum"],
                "WriteMB_sum": ["sum"],
                "ReadMB_max": ["max"],
                "WriteMB_max": ["max"],
                "CPUUtilization_mean": ["mean"],
                "CPUUtilization_max": ["max"],
                "RSS_max": ["sum"],
            })
            
            # Flatten columns
            cpu_agg.columns = [
                f"total_{col}_{func}" if "Utilization" not in col else f"avg_cpu_utilization_{func}"
                for col, func in cpu_agg.columns.to_flat_index()
            ]
            cpu_agg = cpu_agg.reset_index()
            
            # Merge with system state
            system_state = system_state.merge(
                cpu_agg,
                on="bin_index",
                how="left"
            )
            
            logger.info(f"Merged CPU metrics for {len(cpu_agg):,} bins")
    
    # Load GPU telemetry if available
    if gpu_path and Path(gpu_path).exists():
        logger.info(f"Loading GPU telemetry from {gpu_path}")
        gpu_df = pd.read_parquet(gpu_path)
        
        if not gpu_df.empty:
            # Aggregate GPU metrics per bin
            gpu_agg = gpu_df.groupby("bin_index").agg({
                "utilization_gpu_pct_mean": ["mean"],
                "utilization_gpu_pct_max": ["max"],
                "memory_used_MiB_max": ["sum"],
                "power_draw_W_mean": ["sum"],
            })
            
            # Flatten columns
            gpu_agg.columns = [
                f"avg_gpu_{col.split('_')[1]}" if "utilization" in col else f"total_gpu_{col.split('_')[0]}"
                for col, func in gpu_agg.columns.to_flat_index()
            ]
            gpu_agg = gpu_agg.reset_index()
            
            # Merge
            system_state = system_state.merge(
                gpu_agg,
                on="bin_index",
                how="left"
            )
            
            logger.info(f"Merged GPU metrics for {len(gpu_agg):,} bins")
    
    # Load node metrics if available
    if node_path and Path(node_path).exists():
        logger.info(f"Loading node metrics from {node_path}")
        node_df = pd.read_parquet(node_path)
        
        if not node_df.empty:
            # Node metrics are already per-job, need to aggregate differently
            # For now, compute summary stats
            logger.info("Node metrics available (per-job aggregation needed)")
    
    # Fill NaN values with 0 for numeric columns
    numeric_cols = system_state.select_dtypes(include=[np.number]).columns
    system_state[numeric_cols] = system_state[numeric_cols].fillna(0)
    
    # Validate
    if len(system_state) != EXPECTED_SYSTEM_STATE_BINS:
        logger.warning(
            f"Bin count {len(system_state)} differs from expected {EXPECTED_SYSTEM_STATE_BINS}"
        )
    
    if max_active > MAX_ACTIVE_JOBS * 1.5:
        logger.warning(
            f"Max active jobs {max_active} exceeds expected {MAX_ACTIVE_JOBS} by >50%"
        )
    
    # Save to Parquet
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    system_state.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(system_state):,} rows to {output_path}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"System State Summary:")
    print(f"  Time bins: {len(system_state):,}")
    print(f"  Window: {window_start} to {window_end}")
    print(f"  Mean active jobs: {mean_active:.1f}")
    print(f"  Max active jobs: {max_active}")
    print(f"  Columns: {len(system_state.columns)}")
    print(f"{'='*60}\n")
    
    return system_state


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1.4: Build system state time series"
    )
    parser.add_argument(
        "--jobs",
        default="output/phase0_peak_jobs.csv",
        help="Input peak jobs CSV",
    )
    parser.add_argument(
        "--cpu",
        default="output/job_cpu_5min.parquet",
        help="CPU telemetry Parquet",
    )
    parser.add_argument(
        "--gpu",
        default="output/job_gpu_5min.parquet",
        help="GPU telemetry Parquet",
    )
    parser.add_argument(
        "--node",
        default="output/job_node_metrics.parquet",
        help="Node metrics Parquet",
    )
    parser.add_argument(
        "--output",
        default="output/system_state_5min.parquet",
        help="Output Parquet file",
    )
    parser.add_argument(
        "--window-start",
        default=PEAK_WINDOW_START,
        help="Window start date",
    )
    parser.add_argument(
        "--window-end",
        default=PEAK_WINDOW_END,
        help="Window end date",
    )
    
    args = parser.parse_args()
    
    df = build_system_state(
        args.jobs,
        args.cpu,
        args.gpu,
        args.node,
        args.window_start,
        args.window_end,
        args.output,
    )
    
    print(f"\n{'='*60}")
    print(f"Phase 1.4 Complete")
    print(f"  Output: {args.output}")
    print(f"  Rows: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
