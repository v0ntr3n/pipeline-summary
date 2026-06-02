#!/usr/bin/env python3
"""Phase 0.1: Load minimal scheduling columns from slurm-log.csv."""

import argparse
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    SCHEDULING_COLUMNS,
    STATE_COMPLETED,
    MIN_JOBS_EXPECTED,
    MAX_JOBS_EXPECTED,
)
from src.logger import setup_logger, log_metric
from src.utils import validate_job_lifecycle


def load_and_filter_slurm(
    input_path: str,
    output_path: str,
    chunk_size: int = 100_000,
) -> pd.DataFrame:
    """Load slurm-log, filter COMPLETED, validate timestamps.
    
    Args:
        input_path: Path to slurm-log.csv
        output_path: Path to output CSV
        chunk_size: Chunk size for reading large file
    
    Returns:
        Filtered DataFrame with only valid COMPLETED jobs
    """
    logger = setup_logger()
    logger.info(f"Loading slurm-log from {input_path}")
    
    # Read only needed columns
    logger.info(f"Loading columns: {SCHEDULING_COLUMNS}")
    df = pd.read_csv(input_path, usecols=SCHEDULING_COLUMNS)
    
    total_jobs = len(df)
    log_metric(logger, "total_jobs_loaded", total_jobs)
    
    # Filter COMPLETED
    logger.info(f"Filtering state == {STATE_COMPLETED} (COMPLETED)")
    df_completed = df[df["state"] == STATE_COMPLETED].copy()
    completed_count = len(df_completed)
    log_metric(logger, "completed_jobs", completed_count)
    
    removed_non_completed = total_jobs - completed_count
    logger.info(f"Removed {removed_non_completed:,} non-COMPLETED jobs")
    
    # Validate timestamps
    logger.info("Validating job lifecycle timestamps")
    df_valid = validate_job_lifecycle(df_completed, logger)
    valid_count = len(df_valid)
    log_metric(logger, "valid_jobs", valid_count)
    
    # Remove rows with null values in required columns
    df_clean = df_valid.dropna(subset=["id_job", "time_start", "time_end"])
    final_count = len(df_clean)
    
    if final_count < completed_count:
        removed_null = completed_count - final_count
        logger.warning(f"Removed {removed_null:,} jobs with null required fields")
    
    log_metric(logger, "final_job_count", final_count)
    
    # Validate count
    if final_count < MIN_JOBS_EXPECTED:
        logger.warning(
            f"Job count {final_count:,} below expected minimum {MIN_JOBS_EXPECTED:,}"
        )
    elif final_count > MAX_JOBS_EXPECTED:
        logger.info(
            f"Job count {final_count:,} above expected maximum {MAX_JOBS_EXPECTED:,}"
        )
    else:
        logger.info(f"Job count {final_count:,} within expected range")
    
    # Save to CSV
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df_clean):,} jobs to {output_path}")
    
    return df_clean


def main():
    parser = argparse.ArgumentParser(
        description="Phase 0.1: Load and filter slurm-log"
    )
    parser.add_argument(
        "--input",
        default="slurm-log.csv",
        help="Input slurm-log CSV file",
    )
    parser.add_argument(
        "--output",
        default="output/phase0_completed_jobs.csv",
        help="Output CSV file",
    )
    
    args = parser.parse_args()
    
    df = load_and_filter_slurm(args.input, args.output)
    
    print(f"\n{'='*60}")
    print(f"Phase 0.1 Complete")
    print(f"  Input: {args.input}")
    print(f"  Output: {args.output}")
    print(f"  Jobs: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
