#!/usr/bin/env python3
from dotenv import load_dotenv
from chainswarm_core.jobs import BaseTaskContext
from chainswarm_core.observability import setup_logger
from packages.jobs.tasks.initialize_analyzers_task import InitializeAnalyzersTask
import argparse


def main():
    parser = argparse.ArgumentParser(description='Initialize Analyzers Task')
    parser.add_argument('--network', required=True, help='Network name')
    args = parser.parse_args()
    
    load_dotenv()
    
    # Setup logger once for the task
    service_name = f'analytics-{args.network}-initialize-analyzers'
    setup_logger(service_name)
    
    context = BaseTaskContext(
        network=args.network
    )
    
    task = InitializeAnalyzersTask()
    task.execute_task(context)


if __name__ == "__main__":
    main()