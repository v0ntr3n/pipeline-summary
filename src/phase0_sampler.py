#!/usr/bin/env python3
"""Phase 0.2: Peak sampling - select jobs from peak time window."""

import argparse
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import (
    PEAK_WINDOW_START,
    PEAK_WINDOW_END,
    BIN_SIZE_SECONDS,
    EXPECTED_SYSTEM_STATE_BINS,
    MAX_ACTIVE_JOBS,
)
from src.logger import setup_logger, log_metric
from src.utils import (
    parse_timestamp_series,
    create_time_bins,
    compute_active_jobs_per_bin,
)


def peak_sample_jobs(
    input_path: str,
    output_path: str,
    window_start: str = PEAK_WINDOW_START,
    window_end: str = PEAK_WINDOW_END,
) -> pd.DataFrame:
    """Select jobs from peak time window.
    
    Args:
        input_path: Path to phase0_completed_jobs.csv
        output_path: Path to output CSV
        window_start: Start of peak window (YYYY-MM-DD)
        window_end: End of peak window (YYYY-MM-DD)
    
    Returns:
        DataFrame with jobs from peak window
    """
    logger = setup_logger()
    logger.info(f"Loading completed jobs from {input_path}")
    
    # Load completed jobs
    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df):,} completed jobs")
    
    # Parse timestamps if needed (handle string format from CSV)
    for col in ["time_submit", "time_start", "time_end"]:
        if df[col].dtype == "object":
            # String format from CSV
            df[col] = pd.to_datetime(df[col], utc=True)
        elif df[col].dtype == "int64":
            # Unix epoch format
            df[col] = parse_timestamp_series(df[col])
    
    # Define peak window
    window_start_dt = pd.to_datetime(window_start, utc=True)
    window_end_dt = pd.to_datetime(window_end, utc=True)
    
    logger.info(f"Peak window: {window_start} to {window_end}")
    logger.info(f"Window duration: {(window_end_dt - window_start_dt).days} days")
    
    # Filter jobs that overlap with window
    # Job overlaps if: time_start < window_end AND time_end > window_start
    overlaps_window = (
        (df["time_start"] < window_end_dt) &
        (df["time_end"] > window_start_dt)
    )
    
    df_peak = df[overlaps_window].copy()
    peak_count = len(df_peak)
    
    logger.info(f"Selected {peak_count:,} jobs overlapping peak window")
    log_metric(logger, "peak_window_jobs", peak_count)
    
    # Compute active jobs per bin to verify peak characteristics
    logger.info("Computing active jobs per 5-min bin in peak window")
    time_bins = create_time_bins(window_start, window_end)
    
    # Sample bins for faster computation (every 10th bin)
    sample_bins = time_bins[::10]
    active_counts = compute_active_jobs_per_bin(df_peak, sample_bins)
    
    mean_active = active_counts.mean()
    max_active = active_counts.max()
    
    logger.info(f"Active jobs in peak window (sampled bins):")
    logger.info(f"  Mean: {mean_active:.1f}")
    logger.info(f"  Max: {max_active}")
    
    log_metric(logger, "mean_active_jobs", round(mean_active, 1))
    log_metric(logger, "max_active_jobs", max_active)
    
    # Validate peak characteristics
    if max_active < MAX_ACTIVE_JOBS * 0.8:
        logger.warning(
            f"Max active jobs {max_active} significantly below expected {MAX_ACTIVE_JOBS}"
        )
    
    # Compute bin count
    expected_bins = EXPECTED_SYSTEM_STATE_BINS
    actual_bins = len(time_bins)
    
    logger.info(f"Time bins: {actual_bins} (expected ~{expected_bins})")
    
    if abs(actual_bins - expected_bins) > expected_bins * 0.1:
        logger.warning(
            f"Bin count {actual_bins} differs from expected {expected_bins} by >10%"
        )
    
    # Save to CSV
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_peak.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df_peak):,} peak jobs to {output_path}")
    
    # Print summary statistics
    logger.info(f"\n{'='*60}")
    logger.info(f"Peak Window Summary:")
    logger.info(f"  Window: {window_start} to {window_end}")
    logger.info(f"  Duration: {(window_end_dt - window_start_dt).days} days")
    logger.info(f"  Jobs in window: {peak_count:,}")
    logger.info(f"  Mean active jobs (sampled): {mean_active:.1f}")
    logger.info(f"  Max active jobs (sampled): {max_active}")
    logger.info(f"  Time bins @5min: {actual_bins:,}")
    logger.info(f"{'='*60}\n")
    
    return df_peak


def main():
    parser = argparse.ArgumentParser(
        description="Phase 0.2: Peak sampling by time window"
    )
    parser.add_argument(
        "--input",
        default="output/phase0_completed_jobs.csv",
        help="Input completed jobs CSV",
    )
    parser.add_argument(
        "--output",
        default="output/phase0_peak_jobs.csv",
        help="Output peak jobs CSV",
    )
    parser.add_argument(
        "--window-start",
        default=PEAK_WINDOW_START,
        help="Peak window start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--window-end",
        default=PEAK_WINDOW_END,
        help="Peak window end date (YYYY-MM-DD)",
    )
    
    args = parser.parse_args()
    
    df = peak_sample_jobs(
        args.input,
        args.output,
        args.window_start,
        args.window_end,
    )
    
    print(f"\n{'='*60}")
    print(f"Phase 0.2 Complete")
    print(f"  Input: {args.input}")
    print(f"  Output: {args.output}")
    print(f"  Window: {args.window_start} to {args.window_end}")
    print(f"  Jobs: {len(df):,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
