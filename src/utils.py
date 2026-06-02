"""Utility functions for time binning, validation, and data processing."""

import pandas as pd
import numpy as np
from typing import Optional, Tuple, List
from datetime import datetime, timedelta
from src.config import BIN_SIZE_SECONDS, BIN_SIZE_MINUTES


def parse_timestamp(ts: int) -> pd.Timestamp:
    """Parse Unix epoch timestamp to pandas Timestamp."""
    return pd.to_datetime(ts, unit="s", utc=True).tz_convert("UTC")


def create_time_bins(
    start: str,
    end: str,
    bin_size_minutes: int = BIN_SIZE_MINUTES,
) -> pd.DatetimeIndex:
    """Create 5-minute time bins between start and end."""
    start_dt = pd.to_datetime(start, utc=True)
    end_dt = pd.to_datetime(end, utc=True)
    return pd.date_range(start=start_dt, end=end_dt, freq=f"{bin_size_minutes}min")


def timestamp_to_bin(
    ts: pd.Timestamp,
    bin_size_seconds: int = BIN_SIZE_SECONDS,
) -> int:
    """Convert timestamp to bin index (Unix epoch seconds / bin_size)."""
    return int(ts.timestamp() // bin_size_seconds)


def bin_to_timestamp(
    bin_idx: int,
    bin_size_seconds: int = BIN_SIZE_SECONDS,
) -> pd.Timestamp:
    """Convert bin index to timestamp (start of bin)."""
    return pd.to_datetime(bin_idx * bin_size_seconds, unit="s", utc=True)


def validate_job_lifecycle(
    df: pd.DataFrame,
    logger=None,
) -> pd.DataFrame:
    """Validate job timestamps: start >= submit, end >= start."""
    initial_count = len(df)
    
    # Convert to datetime if needed
    for col in ["time_submit", "time_start", "time_end"]:
        if df[col].dtype in ["int64", "float64"]:
            df[col] = parse_timestamp_series(df[col])
    
    # Validation rules
    invalid_start = df["time_start"] < df["time_submit"]
    invalid_end = df["time_end"] < df["time_start"]
    
    # Filter invalid
    valid = df[~(invalid_start | invalid_end)].copy()
    
    removed = initial_count - len(valid)
    if removed > 0 and logger:
        logger.warning(
            f"Removed {removed:,} jobs with invalid timestamps",
            extra={"extra": {"removed_count": removed, "initial_count": initial_count}}
        )
    
    return valid


def parse_timestamp_series(ts_series: pd.Series) -> pd.Series:
    """Convert Unix epoch series to datetime."""
    return pd.to_datetime(ts_series, unit="s", utc=True)


def extract_node_ids(nodelist: str) -> List[str]:
    """Extract node IDs from nodelist string.
    
    Examples:
        "['r9189566-n911952']" -> ["r9189566-n911952"]
        "['r9189566-n911952', 'r1234567-n911953']" -> ["r9189566-n911952", "r1234567-n911953"]
    """
    if pd.isna(nodelist) or nodelist == "":
        return []
    
    # Remove brackets and quotes, split by comma
    cleaned = nodelist.strip("[]").replace("'", "").replace('"', "")
    nodes = [n.strip() for n in cleaned.split(",") if n.strip()]
    return nodes


def compute_active_jobs_per_bin(
    jobs_df: pd.DataFrame,
    time_bins: pd.DatetimeIndex,
) -> pd.Series:
    """Compute active job count per time bin.
    
    A job is active if: time_start <= bin_time < time_end
    """
    active_counts = pd.Series(0, index=time_bins, dtype=int)
    
    for i, bin_time in enumerate(time_bins):
        active = (
            (jobs_df["time_start"] <= bin_time) &
            (jobs_df["time_end"] > bin_time)
        )
        active_counts.iloc[i] = active.sum()
    
    return active_counts


def aggregate_to_5min_bins(
    df: pd.DataFrame,
    time_col: str = "ElapsedTime",
    bin_size_seconds: int = BIN_SIZE_SECONDS,
) -> pd.DataFrame:
    """Aggregate time-series data to 5-minute bins.
    
    Assumes df has a time column and metric columns.
    Returns aggregated DataFrame with bin_index as index.
    """
    df = df.copy()
    df["bin_index"] = df[time_col] // bin_size_seconds
    
    agg_dict = {}
    for col in df.columns:
        if col in [time_col, "bin_index", "Step", "Node", "Series"]:
            continue
        # Default aggregation: mean
        if col.endswith("_MB") or col.endswith("_Sum"):
            agg_dict[col] = "sum"
        elif "Utilization" in col or "pct" in col:
            agg_dict[col] = ["mean", "max"]
        else:
            agg_dict[col] = "mean"
    
    return df.groupby("bin_index").agg(agg_dict)


def calculate_percentile(series: pd.Series, percentile: float = 95) -> float:
    """Calculate percentile, handling NaN values."""
    if series.empty or series.isna().all():
        return np.nan
    return np.percentile(series.dropna(), percentile)


def validate_no_gaps(
    time_series: pd.DatetimeIndex,
    max_gap_minutes: int = 15,
) -> bool:
    """Validate that time series has no gaps larger than threshold."""
    if len(time_series) < 2:
        return True
    
    gaps = time_series[1:] - time_series[:-1]
    max_gap = gaps.max()
    
    return max_gap <= timedelta(minutes=max_gap_minutes)


def safe_divide(numerator: pd.Series, denominator: pd.Series, fill_value: float = 0.0) -> pd.Series:
    """Safe division handling division by zero."""
    result = numerator / denominator
    result = result.replace([np.inf, -np.inf], fill_value)
    result = result.fillna(fill_value)
    return result
