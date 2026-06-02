#!/usr/bin/env python3
"""Phase 1.5: Extract job-level features for ML."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import PROVENANCE_OBSERVED, PROVENANCE_RECONSTRUCTED
from src.logger import setup_logger, log_metric
from src.utils import parse_timestamp_series


def extract_job_features(
    jobs_path: str,
    cpu_path: Optional[str] = None,
    gpu_path: Optional[str] = None,
    node_path: Optional[str] = None,
    labels_path: Optional[str] = None,
    output_path: str = "output/job_features.parquet",
) -> pd.DataFrame:
    """Extract job-level features from telemetry data.
    
    Features per job:
    - Duration metrics: run_time, wait_time
    - I/O profile: total ReadMB/WriteMB, rates, burstiness
    - Compute profile: CPU utilization, memory peaks
    - GPU profile (if applicable): utilization, memory, power
    - Contention exposure: active_jobs at start, avg during run
    - Labels: workload type from labelled_jobids.csv
    
    Args:
        jobs_path: Path to phase0_peak_jobs.csv
        cpu_path: Path to job_cpu_5min.parquet
        gpu_path: Path to job_gpu_5min.parquet
        node_path: Path to job_node_metrics.parquet
        labels_path: Path to labelled_jobids.csv
        output_path: Output Parquet path
    
    Returns:
        DataFrame with features per job
    """
    logger = setup_logger()
    logger.info("Extracting job-level features")
    
    # Load jobs
    logger.info(f"Loading jobs from {jobs_path}")
    jobs_df = pd.read_csv(jobs_path)
    
    # Parse timestamps (handle string format from CSV)
    for col in ["time_submit", "time_start", "time_end"]:
        if jobs_df[col].dtype == "object":
            jobs_df[col] = pd.to_datetime(jobs_df[col], utc=True)
        elif jobs_df[col].dtype == "int64":
            jobs_df[col] = parse_timestamp_series(jobs_df[col])
    
    # Compute duration metrics
    jobs_df["run_time_seconds"] = (
        jobs_df["time_end"] - jobs_df["time_start"]
    ).dt.total_seconds()
    
    jobs_df["wait_time_seconds"] = (
        jobs_df["time_start"] - jobs_df["time_submit"]
    ).dt.total_seconds()
    
    logger.info(f"Duration statistics:")
    logger.info(f"  Mean run time: {jobs_df['run_time_seconds'].mean()/3600:.1f} hours")
    logger.info(f"  Mean wait time: {jobs_df['wait_time_seconds'].mean()/60:.1f} minutes")
    
    # Initialize features DataFrame with base columns
    features_df = jobs_df[["id_job", "run_time_seconds", "wait_time_seconds", "mem_req", "nodes_alloc"]].copy()
    
    # Load CPU telemetry if available
    if cpu_path and Path(cpu_path).exists():
        logger.info(f"Loading CPU telemetry from {cpu_path}")
        cpu_df = pd.read_parquet(cpu_path)
        
        if not cpu_df.empty:
            # Aggregate per job
            cpu_agg = cpu_df.groupby("id_job").agg({
                "ReadMB_sum": ["sum", "max"],
                "WriteMB_sum": ["sum", "max"],
                "CPUUtilization_mean": ["mean", "max"],
                "CPUUtilization_max": ["max"],
                "RSS_max": ["max"],
                "VMSize_max": ["max"],
            })
            
            # Flatten columns
            cpu_agg.columns = [
                f"cpu_{col}_{func}" for col, func in cpu_agg.columns.to_flat_index()
            ]
            cpu_agg = cpu_agg.reset_index()
            
            # Merge with features (don't merge run_time_seconds again)
            features_df = features_df.merge(cpu_agg, on="id_job", how="left")
            
            # Compute I/O rates (MB/s) using run_time_seconds from features_df
            features_df["cpu_ReadMB_rate"] = features_df["cpu_ReadMB_sum_sum"] / features_df["run_time_seconds"]
            features_df["cpu_WriteMB_rate"] = features_df["cpu_WriteMB_sum_sum"] / features_df["run_time_seconds"]
            
            logger.info(f"Added CPU features for {len(cpu_agg):,} jobs")
    
    # Load GPU telemetry if available
    if gpu_path and Path(gpu_path).exists():
        logger.info(f"Loading GPU telemetry from {gpu_path}")
        gpu_df = pd.read_parquet(gpu_path)
        
        if not gpu_df.empty:
            # Aggregate per job
            gpu_agg = gpu_df.groupby("id_job").agg({
                "utilization_gpu_pct_mean": ["mean", "max"],
                "utilization_gpu_pct_max": ["max"],
                "memory_used_MiB_max": ["max"],
                "power_draw_W_mean": ["mean"],
                "temperature_gpu_max": ["max"],
            })
            
            # Flatten columns
            gpu_agg.columns = [
                f"gpu_{col}_{func}" for col, func in gpu_agg.columns.to_flat_index()
            ]
            gpu_agg = gpu_agg.reset_index()
            
            # Merge
            features_df = features_df.merge(gpu_agg, on="id_job", how="left")
            
            logger.info(f"Added GPU features for {len(gpu_agg):,} jobs")
    
    # Load node metrics if available
    if node_path and Path(node_path).exists():
        logger.info(f"Loading node metrics from {node_path}")
        node_df = pd.read_parquet(node_path)
        
        if not node_df.empty:
            # Aggregate per job
            node_agg = node_df.groupby("id_job").agg({
                "LustreRPCTotals_mean": ["mean", "max"],
                "FSlatency_mean": ["mean", "max"],
                "LoadAvg_mean": ["mean"],
                "MemoryFreeInactiveKB_min": ["min"],
            })
            
            # Flatten columns
            node_agg.columns = [
                f"node_{col}_{func}" for col, func in node_agg.columns.to_flat_index()
            ]
            node_agg = node_agg.reset_index()
            
            # Merge
            features_df = features_df.merge(node_agg, on="id_job", how="left")
            
            logger.info(f"Added node features for {len(node_agg):,} jobs")
    
    # Load labels if available
    if labels_path and Path(labels_path).exists():
        logger.info(f"Loading labels from {labels_path}")
        labels_df = pd.read_csv(labels_path)
        
        if not labels_df.empty:
            # Merge labels
            features_df = features_df.merge(
                labels_df.rename(columns={"model": "workload_type"}),
                on="id_job",
                how="left"
            )
            
            # Fill missing labels with "unknown"
            features_df["workload_type"] = features_df["workload_type"].fillna("unknown")
            
            # Label statistics
            label_counts = features_df["workload_type"].value_counts()
            logger.info(f"Workload type distribution:")
            for label, count in label_counts.items():
                logger.info(f"  {label}: {count:,} ({count/len(features_df)*100:.1f}%)")
            
            log_metric(logger, "labelled_jobs", int(label_counts.sum() - label_counts.get("unknown", 0)))
    
    # Add data provenance (observed for all jobs with telemetry)
    features_df["data_provenance"] = PROVENANCE_OBSERVED
    
    # Mark jobs without CPU telemetry as needing reconstruction
    if "cpu_ReadMB_sum_sum" in features_df.columns:
        missing_cpu = features_df["cpu_ReadMB_sum_sum"].isna()
        features_df.loc[missing_cpu, "data_provenance"] = PROVENANCE_RECONSTRUCTED
        
        logger.info(f"Jobs needing I/O reconstruction: {missing_cpu.sum():,}")
    
    # Fill NaN values for numeric columns with 0 (for missing telemetry)
    numeric_cols = features_df.select_dtypes(include=[np.number]).columns
    # Don't fill run_time/wait_time with 0, those are always present
    fill_cols = [col for col in numeric_cols if col not in ["id_job", "run_time_seconds", "wait_time_seconds", "mem_req"]]
    features_df[fill_cols] = features_df[fill_cols].fillna(0)
    
    # Save to Parquet
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    features_df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(features_df):,} jobs to {output_path}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Job Features Summary:")
    print(f"  Total jobs: {len(features_df):,}")
    print(f"  Features per job: {len(features_df.columns)}")
    print(f"  Mean run time: {features_df['run_time_seconds'].mean()/3600:.1f} hours")
    print(f"  Mean wait time: {features_df['wait_time_seconds'].mean()/60:.1f} minutes")
    if "workload_type" in features_df.columns:
        print(f"  Labelled jobs: {(features_df['workload_type'] != 'unknown').sum():,}")
    print(f"{'='*60}\n")
    
    return features_df


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1.5: Extract job-level features"
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
        "--labels",
        default="labelled_jobids.csv",
        help="Labels CSV file",
    )
    parser.add_argument(
        "--output",
        default="output/job_features.parquet",
        help="Output Parquet file",
    )
    
    args = parser.parse_args()
    
    df = extract_job_features(
        args.jobs,
        args.cpu,
        args.gpu,
        args.node,
        args.labels,
        args.output,
    )
    
    print(f"\n{'='*60}")
    print(f"Phase 1.5 Complete")
    print(f"  Output: {args.output}")
    print(f"  Jobs: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
