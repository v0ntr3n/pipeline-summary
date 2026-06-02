#!/usr/bin/env python3
"""Phase 1.2: Process GPU telemetry and downsample to 5-min bins."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    BIN_SIZE_SECONDS,
    PROVENANCE_OBSERVED,
    GPU_TIMESERIES_COLUMNS,
)
from src.logger import setup_logger, log_metric, log_progress
from src.utils import bin_to_timestamp


def find_gpu_files_for_job(job_id: int, gpu_dir: str = "gpu") -> list:
    """Find GPU telemetry files for a job.
    
    GPU files are in gpu/0000/ subdirectory with format: {job_id}-{node}.csv
    Returns list of GPU CSV files (multiple GPUs/nodes per job).
    """
    job_dir = Path(gpu_dir) / "0000"
    
    if not job_dir.exists():
        # Check if flat structure
        if Path(gpu_dir).exists():
            files = list(Path(gpu_dir).glob(f"{job_id}-*.csv"))
            return files
        return []
    
    # Find all GPU files for this job
    gpu_files = list(job_dir.glob(f"{job_id}-*.csv"))
    return gpu_files


def load_gpu_timeseries(job_id: int, gpu_dir: str = "gpu") -> Optional[pd.DataFrame]:
    """Load GPU timeseries for a job.
    
    Returns None if no GPU files exist.
    """
    gpu_files = find_gpu_files_for_job(job_id, gpu_dir)
    
    if not gpu_files:
        return None
    
    # Load all GPU files
    dfs = []
    for gpu_file in gpu_files:
        try:
            df = pd.read_csv(gpu_file)
            dfs.append(df)
        except Exception:
            continue
    
    if not dfs:
        return None
    
    # Combine all GPUs
    combined = pd.concat(dfs, ignore_index=True)
    return combined


def aggregate_gpu_to_5min(df: pd.DataFrame, job_id: int) -> pd.DataFrame:
    """Aggregate GPU telemetry to 5-minute bins.
    
    Input: GPU timeseries with timestamp (100ms resolution)
    Output: Aggregated metrics per 5-min bin
    """
    if df.empty:
        return pd.DataFrame()
    
    # Convert timestamp to datetime
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    
    # Compute bin index from timestamp
    df["bin_index"] = df["timestamp"].astype(float).astype(int) // BIN_SIZE_SECONDS
    
    # Define aggregation functions
    agg_dict = {
        "utilization_gpu_pct": ["mean", "max"],
        "utilization_memory_pct": ["mean", "max"],
        "memory_used_MiB": ["max"],
        "power_draw_W": ["mean"],
        "temperature_gpu": ["max"],
        "gpu_index": ["nunique"],  # Number of GPUs
    }
    
    # Aggregate
    agg_df = df.groupby("bin_index").agg(agg_dict)
    
    # Flatten column names
    agg_df.columns = [
        f"{col}_{func}" for col, func in agg_df.columns.to_flat_index()
    ]
    
    # Rename GPU count column
    agg_df = agg_df.rename(columns={"gpu_index_nunique": "num_gpus"})
    
    # Add job_id
    agg_df["id_job"] = job_id
    
    # Compute bin timestamp
    agg_df["bin_timestamp"] = agg_df.index.map(
        lambda x: bin_to_timestamp(x).isoformat()
    )
    
    return agg_df.reset_index()


def process_gpu_telemetry(
    jobs_path: str,
    gpu_dir: str = "gpu",
    output_path: str = "output/job_gpu_5min.parquet",
) -> pd.DataFrame:
    """Process GPU telemetry for jobs that have GPU data.
    
    Strategy: Index available GPU telemetry files, then match to jobs.
    Does NOT rely on gres_used column (which may be null).
    
    Args:
        jobs_path: Path to phase0_peak_jobs.csv
        gpu_dir: Directory containing GPU telemetry
        output_path: Path to output Parquet file
    
    Returns:
        DataFrame with aggregated GPU metrics per job per 5-min bin
    """
    logger = setup_logger()
    logger.info(f"Loading peak jobs from {jobs_path}")
    
    # Load jobs
    jobs_df = pd.read_csv(jobs_path)
    job_ids = set(jobs_df["id_job"].astype(int))
    
    total_jobs = len(jobs_df)
    logger.info(f"Total peak window jobs: {total_jobs:,}")
    
    # Build index of available GPU telemetry files
    logger.info("Indexing available GPU telemetry files...")
    gpu_files_index = {}
    
    import os
    for root, dirs, files in os.walk(gpu_dir):
        for f in files:
            if f.endswith(".csv"):
                # Parse job_id from filename: {job_id}-{node}.csv
                try:
                    job_id_str = f.split("-")[0]
                    job_id = int(job_id_str)
                    if job_id in job_ids:  # Only if in peak window
                        if job_id not in gpu_files_index:
                            gpu_files_index[job_id] = []
                        gpu_files_index[job_id].append(os.path.join(root, f))
                except ValueError:
                    continue
    
    gpu_jobs = list(gpu_files_index.keys())
    logger.info(f"Found {len(gpu_jobs):,} jobs in peak window with GPU telemetry")
    
    if not gpu_jobs:
        logger.warning("No GPU telemetry files found for jobs in peak window")
    
    # Process each GPU job
    all_aggregated = []
    no_data_jobs = []
    
    progress_interval = 5_000
    
    for i, job_id in enumerate(gpu_jobs):
        if i % progress_interval == 0 and i > 0:
            log_progress(logger, "Processing GPU telemetry", i, len(gpu_jobs))
        
        # Load timeseries
        df = load_gpu_timeseries(job_id, gpu_dir)
        
        if df is None or df.empty:
            no_data_jobs.append(job_id)
            continue
        
        # Aggregate to 5-min bins
        agg_df = aggregate_gpu_to_5min(df, job_id)
        
        if not agg_df.empty:
            all_aggregated.append(agg_df)
    
    logger.info("Finalizing GPU telemetry processing...")
    
    # Combine all aggregated data
    if all_aggregated:
        combined_df = pd.concat(all_aggregated, ignore_index=True)
    else:
        logger.warning("No GPU telemetry data processed")
        combined_df = pd.DataFrame()
    
    # Statistics
    jobs_with_gpu = len(all_aggregated)
    
    logger.info(f"GPU telemetry coverage:")
    logger.info(f"  Jobs with GPU telemetry files: {len(gpu_jobs):,}")
    logger.info(f"  Jobs successfully processed: {jobs_with_gpu:,}")
    logger.info(f"  Coverage of all peak jobs: {jobs_with_gpu/total_jobs*100:.2f}%")
    
    log_metric(logger, "gpu_jobs_with_files", len(gpu_jobs))
    log_metric(logger, "gpu_jobs_processed", jobs_with_gpu)
    log_metric(logger, "gpu_coverage_pct", round(jobs_with_gpu/total_jobs*100, 2))
    
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
            "utilization_gpu_pct_mean": pd.Series(dtype="float64"),
            "utilization_gpu_pct_max": pd.Series(dtype="float64"),
            "utilization_memory_pct_mean": pd.Series(dtype="float64"),
            "utilization_memory_pct_max": pd.Series(dtype="float64"),
            "memory_used_MiB_max": pd.Series(dtype="float64"),
            "power_draw_W_mean": pd.Series(dtype="float64"),
            "temperature_gpu_max": pd.Series(dtype="float64"),
            "num_gpus": pd.Series(dtype="int64"),
            "data_provenance": pd.Series(dtype="str"),
        })
        schema_df.to_parquet(output_path, index=False)
        logger.warning(f"Saved empty schema to {output_path}")
    
    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1.2: Process GPU telemetry"
    )
    parser.add_argument(
        "--jobs",
        default="output/phase0_peak_jobs.csv",
        help="Input peak jobs CSV",
    )
    parser.add_argument(
        "--gpu-dir",
        default="gpu",
        help="GPU telemetry directory",
    )
    parser.add_argument(
        "--output",
        default="output/job_gpu_5min.parquet",
        help="Output Parquet file",
    )
    
    args = parser.parse_args()
    
    df = process_gpu_telemetry(args.jobs, args.gpu_dir, args.output)
    
    print(f"\n{'='*60}")
    print(f"Phase 1.2 Complete")
    print(f"  Jobs input: {args.jobs}")
    print(f"  GPU dir: {args.gpu_dir}")
    print(f"  Output: {args.output}")
    print(f"  Rows: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
