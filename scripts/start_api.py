#!/usr/bin/env python3
"""
Script to run the Analytics Pipeline API server.

Usage:
    python scripts/start_api.py --host 0.0.0.0 --port 8001
    python scripts/start_api.py --port 8001 --reload
"""

import argparse
import os
import uvicorn
from dotenv import load_dotenv
from loguru import logger

from chainswarm_core.observability import setup_logger


def main():
    parser = argparse.ArgumentParser(description='Run Analytics Pipeline API')
    parser.add_argument(
        '--host',
        type=str,
        default=os.getenv("API_HOST", "0.0.0.0"),
        help='Host to bind to (default: 0.0.0.0 or API_HOST env var)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv("API_PORT", "8009")),
        help='Port to listen on (default: 8001 or API_PORT env var)'
    )
    parser.add_argument(
        '--reload',
        action='store_true',
        default=True,
        help='Enable auto-reload for development (default: True)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Log level (default: INFO)'
    )

    args = parser.parse_args()

    # Setup logging
    service_name = 'analytics-api'
    setup_logger(service_name)

    # Load environment variables
    load_dotenv()

    logger.info(
        "Starting Analytics Pipeline API",
        extra={
            "host": args.host,
            "port": args.port,
            "reload": args.reload,
            "mode": os.getenv("ANALYTICS_EXECUTION_MODE", "unknown")
        }
    )
    logger.info(f"API will be available at http://{args.host}:{args.port}")
    logger.info("API docs available at http://{}:{}/docs".format(args.host, args.port))

    uvicorn.run(
        "packages.api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level.lower()
    )


if __name__ == "__main__":
    main()