#!/usr/bin/env python3
from dotenv import load_dotenv
from chainswarm_core.observability import setup_logger
from packages.jobs.tasks.build_features_task import BuildFeaturesTask
from packages.jobs.base.task_models import AnalyticsTaskContext
import argparse


def main():
    parser = argparse.ArgumentParser(description='Build Features Task')
    parser.add_argument('--network', required=True, help='Network name')
    parser.add_argument('--window-days', type=int, required=True, help='Time window in days')
    parser.add_argument('--processing-date', required=True, help='Processing date (YYYY-MM-DD)')
    parser.add_argument('--batch-size', type=int, default=1024, help='Batch size')
    args = parser.parse_args()
    
    load_dotenv()
    
    # Setup logger once for the task
    service_name = f'features-{args.network}-build-features'
    setup_logger(service_name)
    
    context = AnalyticsTaskContext(
        network=args.network,
        window_days=args.window_days,
        processing_date=args.processing_date,
        batch_size=args.batch_size
    )
    
    task = BuildFeaturesTask()
    task.execute_task(context)


if __name__ == "__main__":
    main()