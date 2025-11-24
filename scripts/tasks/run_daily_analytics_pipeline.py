#!/usr/bin/env python3
"""
Script to manually run the Daily Analytics Pipeline.
This orchestrates the full sequence: Ingestion -> Features -> Patterns -> Typologies -> Audit.

Usage:
    python -m scripts.tasks.run_daily_analytics_pipeline --network torus --date 2023-10-01 --days 1
"""
import argparse
import os
import sys
from datetime import datetime, timedelta
from loguru import logger

# Add project root to python path
sys.path.append(os.getcwd())

from packages.jobs.tasks.daily_analytics_pipeline_task import DailyAnalyticsPipelineTask
from packages.jobs.base.task_models import BaseTaskContext

def main():
    parser = argparse.ArgumentParser(description="Run daily analytics pipeline manually")
    parser.add_argument("--network", type=str, required=True, help="Network name")
    parser.add_argument("--processing-date", type=str, required=True, help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--window-days", type=int, default=1, help="Window size in days")
    parser.add_argument("--batch-size", type=int, default=1024, help="Batch size for processing")
    args = parser.parse_args()

    try:
        # Validate date
        datetime.strptime(args.window_date, "%Y-%m-%d")
        
        logger.info(f"Starting manual Daily Analytics Pipeline for {args.network} on {args.date}")
        
        context = BaseTaskContext(
            network=args.network,
            window_days=args.window_days,
            processing_date=args.processing_date,
            batch_size=args.batch_size
        )
        
        task = DailyAnalyticsPipelineTask()
        result = task.execute_task(context)
        
        logger.success(f"Pipeline completed: {result}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()