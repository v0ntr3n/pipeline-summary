#!/usr/bin/env python3
"""Phase 1.1: Process CPU telemetry and aggregate to 5-min bins."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from typing import Dict, Optional, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    BIN_SIZE_SECONDS,
    BIN_SIZE_MINUTES,
    PROVENANCE_OBSERVED,
    PROVENANCE_RECONSTRUCTED,
    CPU_TIMESERIES_COLUMNS,
    AGGREGATION_RULES,
)
from src.logger import setup_logger, log_metric, log_progress
from src.utils import bin_to_timestamp


def find_cpu_timeseries_file(job_id: int, cpu_dir: str = "cpu") -> Optional[Path]:
    """Find CPU telemetry file for a job.
    
    CPU files are in cpu/0000/ subdirectory (flat structure).
    """
    # Try flat structure first (all files in cpu/0000/)
    flat_path = Path(cpu_dir) / "0000" / f"{job_id}-timeseries.csv"
    
    if flat_path.exists():
        return flat_path
    
    # Try alternate prefix-based structure
    job_prefix = str(job_id)[:4]
    prefix_path = Path(cpu_dir) / job_prefix / f"{job_id}-timeseries.csv"
    
    if prefix_path.exists():
        return prefix_path
    
    return None


def load_cpu_timeseries(job_id: int, cpu_dir: str = "cpu") -> Optional[pd.DataFrame]:
    """Load CPU timeseries for a job.
    
    Returns None if file doesn't exist.
    """
    timeseries_file = find_cpu_timeseries_file(job_id, cpu_dir)
    
    if timeseries_file is None:
        return None
    
    try:
        df = pd.read_csv(timeseries_file)
        # Skip initialization row (Step == -1)
        df = df[df["Step"] != -1].copy()
        return df
    except Exception as e:
        return None


def aggregate_cpu_to_5min(df: pd.DataFrame, job_id: int) -> pd.DataFrame:
    """Aggregate CPU telemetry to 5-minute bins.
    
    Input: CPU timeseries with ElapsedTime (10s resolution)
    Output: Aggregated metrics per 5-min bin
    """
    if df.empty:
        return pd.DataFrame()
    
    # Compute bin index from ElapsedTime
    df["bin_index"] = df["ElapsedTime"] // BIN_SIZE_SECONDS
    
    # Define aggregation functions
    agg_dict = {}
    
    # I/O metrics: sum and max
    agg_dict["ReadMB"] = ["sum", "max"]
    agg_dict["WriteMB"] = ["sum", "max"]
    
    # CPU metrics: mean and max
    agg_dict["CPUUtilization"] = ["mean", "max"]
    
    # Memory metrics: max
    agg_dict["RSS"] = ["max"]
    agg_dict["VMSize"] = ["max"]
    
    # Count samples per bin
    agg_dict["ElapsedTime"] = ["count"]
    
    # Aggregate
    agg_df = df.groupby("bin_index").agg(agg_dict)
    
    # Flatten column names
    agg_df.columns = [
        f"{col}_{func}" for col, func in agg_df.columns.to_flat_index()
    ]
    
    # Rename count column
    agg_df = agg_df.rename(columns={"ElapsedTime_count": "samples_in_bin"})
    
    # Add job_id
    agg_df["id_job"] = job_id
    
    # Compute bin timestamp
    agg_df["bin_timestamp"] = agg_df.index.map(
        lambda x: bin_to_timestamp(x).isoformat()
    )
    
    return agg_df.reset_index()


def process_cpu_telemetry(
    jobs_path: str,
    cpu_dir: str = "cpu",
    output_path: str = "output/job_cpu_5min.parquet",
) -> pd.DataFrame:
    """Process CPU telemetry for all jobs in peak window.
    
    Args:
        jobs_path: Path to phase0_peak_jobs.csv
        cpu_dir: Directory containing CPU telemetry
        output_path: Path to output Parquet file
    
    Returns:
        DataFrame with aggregated CPU metrics per job per 5-min bin
    """
    logger = setup_logger()
    logger.info(f"Loading peak jobs from {jobs_path}")
    
    # Load jobs
    jobs_df = pd.read_csv(jobs_path)
    job_ids = jobs_df["id_job"].astype(int).tolist()
    
    total_jobs = len(job_ids)
    logger.info(f"Processing CPU telemetry for {total_jobs:,} jobs")
    
    # Build index of available CPU telemetry files
    logger.info("Indexing available CPU telemetry files...")
    cpu_files_index = set()
    
    for root, dirs, files in os.walk(cpu_dir):
        for f in files:
            if f.endswith("-timeseries.csv"):
                job_id_str = f.split("-")[0]
                try:
                    cpu_files_index.add(int(job_id_str))
                except ValueError:
                    continue
    
    logger.info(f"Found {len(cpu_files_index):,} jobs with CPU telemetry files")
    
    # Process each job
    all_aggregated = []
    missing_jobs = []
    empty_jobs = []
    
    progress_interval = 10_000
    
    for i, job_id in enumerate(job_ids):
        if i % progress_interval == 0:
            log_progress(logger, "Scanning CPU telemetry", i, total_jobs)
        
        # Check if job has telemetry
        if job_id not in cpu_files_index:
            missing_jobs.append(job_id)
            continue
        
        # Load timeseries
        df = load_cpu_timeseries(job_id, cpu_dir)
        
        if df is None:
            missing_jobs.append(job_id)
            continue
        
        if df.empty:
            empty_jobs.append(job_id)
            continue
        
        # Aggregate to 5-min bins
        agg_df = aggregate_cpu_to_5min(df, job_id)
        
        if not agg_df.empty:
            all_aggregated.append(agg_df)
    
    logger.info("Finalizing CPU telemetry processing...")
    
    # Combine all aggregated data
    if all_aggregated:
        combined_df = pd.concat(all_aggregated, ignore_index=True)
    else:
        logger.warning("No CPU telemetry data found")
        combined_df = pd.DataFrame()
    
    # Statistics
    jobs_with_cpu = len(all_aggregated)
    missing_count = len(missing_jobs)
    empty_count = len(empty_jobs)
    
    logger.info(f"CPU telemetry coverage:")
    logger.info(f"  Jobs with data: {jobs_with_cpu:,} ({jobs_with_cpu/total_jobs*100:.2f}%)")
    logger.info(f"  Missing files: {missing_count:,}")
    logger.info(f"  Empty files: {empty_count:,}")
    
    log_metric(logger, "cpu_jobs_with_data", jobs_with_cpu)
    log_metric(logger, "cpu_coverage_pct", round(jobs_with_cpu/total_jobs*100, 2))
    log_metric(logger, "cpu_missing_jobs", missing_count)
    
    # Add data provenance
    if not combined_df.empty:
        combined_df["data_provenance"] = PROVENANCE_OBSERVED
    
    # Save to Parquet
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not combined_df.empty:
        combined_df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(combined_df):,} rows to {output_path}")
    else:
        # Save empty file with schema
        schema_df = pd.DataFrame({
            "id_job": pd.Series(dtype="int64"),
            "bin_index": pd.Series(dtype="int64"),
            "bin_timestamp": pd.Series(dtype="str"),
            "ReadMB_sum": pd.Series(dtype="float64"),
            "ReadMB_max": pd.Series(dtype="float64"),
            "WriteMB_sum": pd.Series(dtype="float64"),
            "WriteMB_max": pd.Series(dtype="float64"),
            "CPUUtilization_mean": pd.Series(dtype="float64"),
            "CPUUtilization_max": pd.Series(dtype="float64"),
            "RSS_max": pd.Series(dtype="float64"),
            "VMSize_max": pd.Series(dtype="float64"),
            "samples_in_bin": pd.Series(dtype="int64"),
            "data_provenance": pd.Series(dtype="str"),
        })
        schema_df.to_parquet(output_path, index=False)
        logger.warning(f"Saved empty schema to {output_path}")
    
    # Save missing jobs list for reconstruction
    if missing_jobs or empty_jobs:
        missing_path = Path(output_path).parent / "cpu_missing_jobs.csv"
        missing_df = pd.DataFrame({
            "id_job": missing_jobs + empty_jobs,
            "reason": ["missing_file"] * len(missing_jobs) + ["empty_data"] * len(empty_jobs),
        })
        missing_df.to_csv(missing_path, index=False)
        logger.info(f"Saved {len(missing_df):,} missing jobs to {missing_path}")
    
    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1.1: Process CPU telemetry"
    )
    parser.add_argument(
        "--jobs",
        default="output/phase0_peak_jobs.csv",
        help="Input peak jobs CSV",
    )
    parser.add_argument(
        "--cpu-dir",
        default="cpu",
        help="CPU telemetry directory",
    )
    parser.add_argument(
        "--output",
        default="output/job_cpu_5min.parquet",
        help="Output Parquet file",
    )
    
    args = parser.parse_args()
    
    df = process_cpu_telemetry(args.jobs, args.cpu_dir, args.output)
    
    print(f"\n{'='*60}")
    print(f"Phase 1.1 Complete")
    print(f"  Jobs input: {args.jobs}")
    print(f"  CPU dir: {args.cpu_dir}")
    print(f"  Output: {args.output}")
    print(f"  Rows: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
