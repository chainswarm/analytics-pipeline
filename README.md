# Analytics Pipeline

**Blockchain Analytics and Pattern Detection System**

The Analytics Pipeline is a specialized system extracted from the data-pipeline to handle advanced analytics, feature engineering, and pattern detection on blockchain data. It supports both automated daily processing and on-demand benchmarking/backfilling.

## Overview

Analytics Pipeline is a standalone service that provides:

- **Feature Engineering**: Build graph-based features from blockchain transaction data
- **Structural Pattern Detection**: Identify common transaction patterns (mixing, layering, etc.)
- **Typology Detection**: Detect complex multi-step suspicious activity patterns
- **Alert Generation**: Create and cluster security alerts based on detected patterns

## Architecture

### Data Access Model

- **Read-Only Access**: Analytics pipeline ingests data from `core_*` tables (conceptually read-only source).
- **Isolated Storage**: Creates a dedicated ClickHouse database per network (e.g., `analytics_torus`, `analytics_torus_benchmark_1`) to store results.
- **Complete Independence**: No code dependencies on data-pipeline, fully standalone.

### Key Components

#### 1. Job System (`packages/jobs/`)
- **Celery Integration**: Distributed task processing.
- **Daily Pipeline**: A consolidated task `DailyAnalyticsPipelineTask` orchestrating ingestion -> features -> patterns -> alerts.

#### 2. API (`packages/api/`)
- **FastAPI**: Provides HTTP endpoints to trigger on-demand runs for benchmarking or backfills.
- **Execution Modes**: Supports `daily` (scheduler) and `on-demand` (API) operation modes.

#### 3. Storage Layer (`packages/storage/`)
- **Repositories**: Data access for features, patterns, alerts.
- **Schema Migration**: Auto-migrates schemas to the target isolated database.
- **Specialized Pattern Tables**: Pattern detections are stored in 5 specialized tables (cycle, layering, network, proximity, motif) for optimal storage and query performance, with a backward-compatible view for unified access. See [`packages/storage/schema/README.md`](packages/storage/schema/README.md) for details.

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Execution Mode: 'daily' (starts Scheduler) or 'on-demand' (starts API)
ANALYTICS_EXECUTION_MODE=on-demand

# API Configuration
API_HOST=0.0.0.0
API_PORT=8001

# ClickHouse Database
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=password1234
# Note: Database name is dynamic (analytics_{network}), this var is base/default

# Celery/Redis
REDIS_URL=redis://localhost:6379/0

# Network
NETWORK=torus
```

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your configuration
```

## Usage

### 1. On-Demand Execution (API)

Run the API server:
```bash
python scripts/start_api.py
```
Trigger a pipeline run:
```bash
curl -X POST "http://localhost:8001/api/v1/pipelines/run" \
     -H "Content-Type: application/json" \
     -d '{
           "network": "torus-benchmark-1",
           "date_range": {
             "start_date": "2023-10-01",
             "end_date": "2023-10-05"
           },
           "window_days": 1
         }'
```
This will ingest data into `analytics_torus_benchmark_1` database and run all analytics steps isolated from production.

### 2. Automated Daily Execution (Celery Beat)

Set `ANALYTICS_EXECUTION_MODE=daily` and run:
```bash
# Start Scheduler
celery -A packages.jobs.celery_app beat --loglevel=info

# Start Worker (required for both modes)
celery -A packages.jobs.celery_app worker --loglevel=info
```

The scheduler uses `packages/jobs/beat_schedule.json` to trigger the `DailyAnalyticsPipelineTask` every day.

### 3. Docker Support

Run the full stack:
```bash
cd ops/infrastructure
docker-compose up -d
```
This starts:
- Read/Write ClickHouse
- Redis
- Celery Worker
- Celery Beat (Scheduler)
- API Server

## Project Structure

```
analytics-pipeline/
├── packages/
│   ├── api/                # REST API for on-demand control
│   ├── analyzers/          # Analytics engines
│   ├── storage/            # Data access & Schemas
│   ├── jobs/               # Celery tasks
│   │   └── tasks/          # Daily pipeline & sub-tasks
│   └── ingestion/          # Data ingestion logic
├── scripts/
│   └── start_api.py        # API startup script
├── ops/                    # Operations & deployment
├── .env.example            # Environment template
└── requirements.txt        # Dependencies
```

## License

See LICENSE file for details.