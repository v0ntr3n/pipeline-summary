#!/bin/bash
# Run Phase 0 + Phase 1 pipeline

set -e

echo "=========================================="
echo "Phase 0 + Phase 1 Data Preprocessing Pipeline"
echo "=========================================="
echo ""

# Phase 0
echo "Phase 0.1: Load and filter slurm-log..."
python3 src/phase0_loader.py --input slurm-log.csv --output output/phase0_completed_jobs.csv

echo ""
echo "Phase 0.2: Peak sampling..."
python3 src/phase0_sampler.py --input output/phase0_completed_jobs.csv --output output/phase0_peak_jobs.csv

echo ""
echo "=========================================="
echo "Phase 0 Complete"
echo "=========================================="
echo ""

# Phase 1
echo "Phase 1.1: Process CPU telemetry..."
python3 src/phase1_cpu_processor.py --jobs output/phase0_peak_jobs.csv --cpu-dir cpu --output output/job_cpu_5min.parquet

echo ""
echo "Phase 1.2: Process GPU telemetry..."
python3 src/phase1_gpu_processor.py --jobs output/phase0_peak_jobs.csv --gpu-dir gpu --output output/job_gpu_5min.parquet

echo ""
echo "Phase 1.3: Join node metrics..."
python3 src/phase1_node_joiner.py --jobs output/phase0_peak_jobs.csv --node-data node-data.csv --output output/job_node_metrics.parquet

echo ""
echo "Phase 1.4: Build system state..."
python3 src/phase1_system_state.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --gpu output/job_gpu_5min.parquet --output output/system_state_5min.parquet

echo ""
echo "Phase 1.5: Extract job features..."
python3 src/phase1_job_features.py --jobs output/phase0_peak_jobs.csv --cpu output/job_cpu_5min.parquet --gpu output/job_gpu_5min.parquet --labels labelled_jobids.csv --output output/job_features.parquet

echo ""
echo "=========================================="
echo "Phase 0 + Phase 1 Complete"
echo "=========================================="
echo ""
echo "Output files:"
ls -lh output/
echo ""
echo "Log file:"
ls -lh logs/
echo ""
