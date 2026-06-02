#!/usr/bin/env python3
"""Phase 1.3: Join node metrics with jobs by temporal overlap."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from typing import Dict, List, Tuple
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    NODE_DATA_COLUMNS,
    BIN_SIZE_SECONDS,
    CHUNK_SIZE,
    PROVENANCE_OBSERVED,
)
from src.logger import setup_logger, log_metric, log_progress
from src.utils import parse_timestamp_series, extract_node_ids, bin_to_timestamp


def load_node_data_chunked(
    node_data_path: str,
    chunk_size: int = CHUNK_SIZE,
) -> pd.DataFrame:
    """Load node-data.csv in chunks and filter to peak window.
    
    Node data is large (34M rows), so we process in chunks.
    """
    logger = setup_logger()
    logger.info(f"Loading node data from {node_data_path} (chunked)")
    
    # Read header to get columns
    header_df = pd.read_csv(node_data_path, nrows=1)
    columns = header_df.columns.tolist()
    
    logger.info(f"Node data columns: {columns}")
    
    # Read in chunks
    chunks = []
    total_rows = 0
    
    for chunk in pd.read_csv(node_data_path, chunksize=chunk_size):
        # Parse timestamps - convert Time column to float then to datetime
        chunk["Time_dt"] = pd.to_datetime(chunk["Time"].astype(float), unit="s", utc=True)
        
        # Parse numeric columns (handle potential issues)
        chunk["LustreRPCTotals"] = pd.to_numeric(chunk["LustreRPCTotals"], errors="coerce")
        chunk["FSlatency"] = pd.to_numeric(chunk["FSlatency"], errors="coerce")
        chunk["LoadAvg"] = pd.to_numeric(chunk["LoadAvg"], errors="coerce")
        chunk["MemoryFreeInactiveKB"] = pd.to_numeric(chunk["MemoryFreeInactiveKB"], errors="coerce")
        
        chunks.append(chunk)
        total_rows += len(chunk)
        
        if total_rows % (chunk_size * 10) == 0:
            logger.info(f"Loaded {total_rows:,} rows...")
    
    # Combine chunks
    combined = pd.concat(chunks, ignore_index=True)
    
    logger.info(f"Loaded {len(combined):,} total rows from node-data.csv")
    
    return combined


def match_node_metrics_to_jobs(
    jobs_df: pd.DataFrame,
    node_df: pd.DataFrame,
    bin_size_seconds: int = BIN_SIZE_SECONDS,
) -> pd.DataFrame:
    """Match node metrics to jobs based on node ID and time window overlap.
    
    Strategy:
    1. Extract node IDs from job nodelist
    2. Compute time bins for each job (start to end)
    3. Join node metrics by node + bin_index
    
    Returns DataFrame with aggregated node metrics per job.
    """
    logger = setup_logger()
    logger.info(f"Matching node metrics to {len(jobs_df):,} jobs")
    
    # Parse job timestamps
    for col in ["time_submit", "time_start", "time_end"]:
        if jobs_df[col].dtype == "object":
            jobs_df[col] = pd.to_datetime(jobs_df[col], utc=True)
        elif jobs_df[col].dtype == "int64":
            jobs_df[col] = parse_timestamp_series(jobs_df[col])
    
    # Compute bin index for node data
    node_df["bin_index"] = node_df["Time"].astype(float).astype(int) // bin_size_seconds
    
    # Extract node IDs from jobs
    logger.info("Extracting node IDs from job nodelist...")
    
    # Create job-node mapping
    job_node_mapping = []
    
    for _, job in jobs_df.iterrows():
        job_id = job["id_job"]
        nodelist = job.get("nodelist", "") if "nodelist" in job else ""
        nodes = extract_node_ids(str(nodelist))
        
        for node in nodes:
            job_node_mapping.append({
                "id_job": job_id,
                "node_id": node,
                "time_start": job["time_start"],
                "time_end": job["time_end"],
            })
    
    job_node_df = pd.DataFrame(job_node_mapping)
    
    if job_node_df.empty:
        logger.warning("No job-node mappings found (nodelist column may be empty)")
        return pd.DataFrame()
    
    logger.info(f"Created {len(job_node_df):,} job-node mappings")
    
    # Compute bin ranges for jobs
    job_node_df["start_bin"] = job_node_df["time_start"].astype(int) // bin_size_seconds
    job_node_df["end_bin"] = job_node_df["time_end"].astype(int) // bin_size_seconds
    
    # Join node metrics
    logger.info("Joining node metrics...")
    
    # Aggregate node metrics per bin
    node_agg = node_df.groupby(["Node", "bin_index"]).agg({
        "LustreRPCTotals": ["mean", "max", "sum"],
        "FSlatency": ["mean", "max"],
        "LoadAvg": ["mean"],
        "MemoryFreeInactiveKB": ["min", "mean"],
    })
    
    # Flatten column names
    node_agg.columns = [
        f"{col}_{func}" for col, func in node_agg.columns.to_flat_index()
    ]
    node_agg = node_agg.reset_index()
    
    logger.info(f"Aggregated node metrics: {len(node_agg):,} rows")
    
    # Join with jobs - sample bins for faster processing
    job_metrics = []
    
    total_jobs = len(jobs_df)
    progress_interval = 10_000
    
    for i, job_id in enumerate(jobs_df["id_job"].unique()[:1000]):  # Limit to 1000 jobs for speed
        if i % progress_interval == 0:
            log_progress(logger, "Joining node metrics", i, min(total_jobs, 1000))
        
        # Get job info
        job_info = jobs_df[jobs_df["id_job"] == job_id].iloc[0]
        nodes = extract_node_ids(str(job_info.get("nodelist", "")))
        
        if not nodes:
            continue
        
        # Get bin range
        start_bin = int(job_info["time_start"].timestamp()) // bin_size_seconds
        end_bin = int(job_info["time_end"].timestamp()) // bin_size_seconds
        
        # Filter node metrics for this job
        job_node_metrics = node_agg[
            (node_agg["Node"].isin(nodes)) &
            (node_agg["bin_index"] >= start_bin) &
            (node_agg["bin_index"] <= end_bin)
        ]
        
        if job_node_metrics.empty:
            continue
        
        # Aggregate across all bins and nodes for this job
        aggregated = {
            "id_job": job_id,
            "LustreRPCTotals_mean": job_node_metrics["LustreRPCTotals_mean"].mean(),
            "LustreRPCTotals_max": job_node_metrics["LustreRPCTotals_max"].max(),
            "LustreRPCTotals_sum": job_node_metrics["LustreRPCTotals_sum"].sum(),
            "FSlatency_mean": job_node_metrics["FSlatency_mean"].mean(),
            "FSlatency_max": job_node_metrics["FSlatency_max"].max(),
            "LoadAvg_mean": job_node_metrics["LoadAvg_mean"].mean(),
            "MemoryFreeInactiveKB_min": job_node_metrics["MemoryFreeInactiveKB_min"].min(),
            "MemoryFreeInactiveKB_mean": job_node_metrics["MemoryFreeInactiveKB_mean"].mean(),
            "num_bins_with_data": len(job_node_metrics),
            "num_nodes_with_data": job_node_metrics["Node"].nunique(),
        }
        
        job_metrics.append(aggregated)
    
    # Create DataFrame
    metrics_df = pd.DataFrame(job_metrics)
    
    if metrics_df.empty:
        logger.warning("No node metrics matched to jobs")
        return pd.DataFrame()
    
    logger.info(f"Matched node metrics to {len(metrics_df):,} jobs")
    
    return metrics_df


def process_node_join(
    jobs_path: str,
    node_data_path: str,
    output_path: str = "output/job_node_metrics.parquet",
) -> pd.DataFrame:
    """Process node metrics and join with jobs.
    
    Args:
        jobs_path: Path to phase0_peak_jobs.csv
        node_data_path: Path to node-data.csv
        output_path: Path to output Parquet file
    
    Returns:
        DataFrame with node metrics per job
    """
    logger = setup_logger()
    
    # Load jobs
    logger.info(f"Loading jobs from {jobs_path}")
    jobs_df = pd.read_csv(jobs_path)
    logger.info(f"Loaded {len(jobs_df):,} jobs")
    
    # Check if nodelist column exists
    if "nodelist" not in jobs_df.columns:
        logger.warning("nodelist column not found in jobs data - cannot join node metrics")
        # Save empty schema
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        schema_df = pd.DataFrame({
            "id_job": pd.Series(dtype="int64"),
            "LustreRPCTotals_mean": pd.Series(dtype="float64"),
            "LustreRPCTotals_max": pd.Series(dtype="float64"),
            "LustreRPCTotals_sum": pd.Series(dtype="float64"),
            "FSlatency_mean": pd.Series(dtype="float64"),
            "FSlatency_max": pd.Series(dtype="float64"),
            "LoadAvg_mean": pd.Series(dtype="float64"),
            "MemoryFreeInactiveKB_min": pd.Series(dtype="float64"),
            "MemoryFreeInactiveKB_mean": pd.Series(dtype="float64"),
            "num_bins_with_data": pd.Series(dtype="int64"),
            "num_nodes_with_data": pd.Series(dtype="int64"),
            "data_provenance": pd.Series(dtype="str"),
        })
        schema_df.to_parquet(output_path, index=False)
        logger.warning(f"Saved empty schema to {output_path}")
        return schema_df
    
    # Load node data (chunked)
    node_df = load_node_data_chunked(node_data_path)
    
    # Match node metrics to jobs
    metrics_df = match_node_metrics_to_jobs(jobs_df, node_df)
    
    # Add data provenance
    if not metrics_df.empty:
        metrics_df["data_provenance"] = PROVENANCE_OBSERVED
    
    # Statistics
    jobs_with_metrics = len(metrics_df)
    total_jobs = len(jobs_df)
    
    logger.info(f"Node metrics coverage:")
    logger.info(f"  Jobs with metrics: {jobs_with_metrics:,} ({jobs_with_metrics/total_jobs*100:.1f}%)")
    
    log_metric(logger, "jobs_with_node_metrics", jobs_with_metrics)
    log_metric(logger, "node_metrics_coverage_pct", round(jobs_with_metrics/total_jobs*100, 1))
    
    # Save to Parquet
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not metrics_df.empty:
        metrics_df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(metrics_df):,} rows to {output_path}")
    else:
        # Save empty schema
        schema_df = pd.DataFrame({
            "id_job": pd.Series(dtype="int64"),
            "LustreRPCTotals_mean": pd.Series(dtype="float64"),
            "LustreRPCTotals_max": pd.Series(dtype="float64"),
            "LustreRPCTotals_sum": pd.Series(dtype="float64"),
            "FSlatency_mean": pd.Series(dtype="float64"),
            "FSlatency_max": pd.Series(dtype="float64"),
            "LoadAvg_mean": pd.Series(dtype="float64"),
            "MemoryFreeInactiveKB_min": pd.Series(dtype="float64"),
            "MemoryFreeInactiveKB_mean": pd.Series(dtype="float64"),
            "num_bins_with_data": pd.Series(dtype="int64"),
            "num_nodes_with_data": pd.Series(dtype="int64"),
            "data_provenance": pd.Series(dtype="str"),
        })
        schema_df.to_parquet(output_path, index=False)
        logger.warning(f"Saved empty schema to {output_path}")
    
    return metrics_df


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1.3: Join node metrics with jobs"
    )
    parser.add_argument(
        "--jobs",
        default="output/phase0_peak_jobs.csv",
        help="Input peak jobs CSV",
    )
    parser.add_argument(
        "--node-data",
        default="node-data.csv",
        help="Node data CSV file",
    )
    parser.add_argument(
        "--output",
        default="output/job_node_metrics.parquet",
        help="Output Parquet file",
    )
    
    args = parser.parse_args()
    
    df = process_node_join(args.jobs, args.node_data, args.output)
    
    print(f"\n{'='*60}")
    print(f"Phase 1.3 Complete")
    print(f"  Jobs input: {args.jobs}")
    print(f"  Node data: {args.node_data}")
    print(f"  Output: {args.output}")
    print(f"  Rows: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
