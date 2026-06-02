"""Configuration and constants for Phase 0 + Phase 1 pipeline."""

from dataclasses import dataclass
from typing import List
import pandas as pd

# Phase 0 - Minimal loading columns (adjusted to actual CSV columns)
SCHEDULING_COLUMNS = [
    "id_job",
    "state",
    "time_submit",
    "time_start",
    "time_end",
    "mem_req",
    "gres_used",  # Changed from gres_alloc to gres_used
    "nodes_alloc",
]

# Slurm state codes
STATE_COMPLETED = 3

# Peak sampling window (sustained load)
PEAK_WINDOW_START = "2021-04-21"
PEAK_WINDOW_END = "2021-06-01"

# Time bin size (5 minutes)
BIN_SIZE_MINUTES = 5
BIN_SIZE_SECONDS = 300

# Node data columns for join
NODE_DATA_COLUMNS = [
    "Node",
    "Time",
    "UserPIDCount",
    "FSlatency",
    "LoadAvg",
    "MemoryFreeInactiveKB",
    "LustreRPCTotals",
]

# CPU telemetry columns to aggregate
CPU_TIMESERIES_COLUMNS = [
    "ReadMB",
    "WriteMB",
    "CPUUtilization",
    "RSS",
    "VMSize",
]

# GPU telemetry columns to aggregate
GPU_TIMESERIES_COLUMNS = [
    "utilization_gpu_pct",
    "utilization_memory_pct",
    "memory_used_MiB",
    "power_draw_W",
    "temperature_gpu",
]

# Aggregation functions for different metrics
AGGREGATION_RULES = {
    "ReadMB": ["sum", "max"],
    "WriteMB": ["sum", "max"],
    "CPUUtilization": ["mean", "max"],
    "RSS": ["max"],
    "VMSize": ["max"],
    "utilization_gpu_pct": ["mean", "max"],
    "memory_used_MiB": ["max"],
    "power_draw_W": ["mean"],
    "temperature_gpu": ["max"],
    "LustreRPCTotals": ["sum", "max"],
    "FSlatency": ["mean", "max"],
    "LoadAvg": ["mean"],
    "MemoryFreeInactiveKB": ["min"],
}

# Data provenance labels
PROVENANCE_OBSERVED = "observed"
PROVENANCE_RECONSTRUCTED = "reconstructed"

# Chunk size for large file processing
CHUNK_SIZE = 1_000_000

# Validation thresholds
MIN_JOBS_EXPECTED = 68_000
MAX_JOBS_EXPECTED = 70_500
EXPECTED_SYSTEM_STATE_BINS = 11_647
MAX_ACTIVE_JOBS = 841

@dataclass
class PipelineConfig:
    """Pipeline configuration parameters."""
    input_dir: str = "."
    output_dir: str = "output"
    slurm_log: str = "slurm-log.csv"
    node_data: str = "node-data.csv"
    cpu_dir: str = "cpu"
    gpu_dir: str = "gpu"
    labels_file: str = "labelled_jobids.csv"
    window_start: str = PEAK_WINDOW_START
    window_end: str = PEAK_WINDOW_END
    chunk_size: int = CHUNK_SIZE
    
    @property
    def slurm_log_path(self) -> str:
        return f"{self.input_dir}/{self.slurm_log}"
    
    @property
    def node_data_path(self) -> str:
        return f"{self.input_dir}/{self.node_data}"
