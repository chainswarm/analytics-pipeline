# Analytics Pipeline Operations (ops/)

This directory contains the Docker infrastructure for the analytics-pipeline project, focusing on analytics task processing via Celery workers.

## Quick Start

### Build Analytics Pipeline Image

```bash
cd /path/to/analytics-pipeline
docker build -t analytics-pipeline:latest -f ops/Dockerfile .
```

### Start Analytics Stack

```bash
cd ops
docker compose up -d
```

## Directory Structure

```
ops/
├── Dockerfile                       # Analytics pipeline image
├── README.md                        # This file
├── docker-compose.yml               # Celery worker + beat + Redis
└── .env.example                     # Environment template (copy from root)
```

## Architecture

### Analytics Pipeline Image
- Single Docker image containing all analytics packages
- GPU support for Celery workers (graph analytics with cuGraph)
- Python 3.13 with CUDA 12.4.1 runtime

### Service Components
- **Redis** - Message broker for Celery tasks
- **Celery Worker** - Executes analytics tasks (GPU-enabled)
- **Celery Beat** - Schedules periodic analytics tasks

## Analytics Tasks

The pipeline includes the following analytics tasks:

1. **Initialize Analyzers** - Set up analyzer configurations
2. **Build Features** - Compute graph-based features for addresses
3. **Detect Structural Patterns** - Identify structural patterns in transaction graphs
4. **Detect Typologies** - Detect suspicious activity typologies

## Environment Variables

Key variables (see `.env.example` in root):

```bash
# Database
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Graph Database (for feature computation)
MEMGRAPH_HOST=memgraph
MEMGRAPH_PORT=7687

# Redis
REDIS_URL=redis://redis:6379/0

# Celery
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0

# GPU Settings
NX_CUGRAPH_AUTOCONFIG=1
NX_CUGRAPH_VERBOSE=0
```

## Resource Requirements

### Analytics Stack
- **Redis:** 512MB RAM
- **Celery Worker:** 4-8GB RAM, 1x GPU (for graph analytics)
- **Celery Beat:** 256MB RAM

## Running Analytics Tasks

### Via Celery (Scheduled)

Tasks are automatically scheduled via Celery Beat according to `packages/jobs/beat_schedule.json`:

```bash
# View scheduled tasks
docker compose exec celery-beat celery -A packages.jobs.celery_app inspect scheduled

# Monitor active tasks
docker compose exec celery-worker celery -A packages.jobs.celery_app inspect active
```

### Via Scripts (Manual)

You can run tasks manually using the provided scripts:

```bash
# Initialize analyzers
docker compose exec celery-worker python3 scripts/tasks/initialize_analyzers.py --network torus

# Build features
docker compose exec celery-worker python3 scripts/tasks/build_features.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09 \
    --batch-size 1024

# Detect structural patterns
docker compose exec celery-worker python3 scripts/tasks/detect_structural_patterns.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09

# Detect typologies
docker compose exec celery-worker python3 scripts/tasks/detect_typologies.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09
```

### Via Example Scripts (Development)

For local development with predefined parameters:

```bash
# Run individual tasks
python3 scripts/examples/example_initialize_analyzers.py
python3 scripts/examples/example_build_features.py
python3 scripts/examples/example_detect_structural_patterns.py
python3 scripts/examples/example_detect_typologies.py

# Run full pipeline
python3 scripts/examples/pipeline_daily_analytics.py
```

## Monitoring

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f celery-worker
docker compose logs -f celery-beat
```

### Celery Monitoring

```bash
# Worker status
docker compose exec celery-worker celery -A packages.jobs.celery_app status

# Active tasks
docker compose exec celery-worker celery -A packages.jobs.celery_app inspect active

# Scheduled tasks
docker compose exec celery-beat celery -A packages.jobs.celery_app inspect scheduled

# Task stats
docker compose exec celery-worker celery -A packages.jobs.celery_app inspect stats
```

## Troubleshooting

### Restart Services

```bash
docker compose restart celery-worker
docker compose restart celery-beat
```

### Check Status

```bash
docker compose ps
```

### Rebuild Image

```bash
cd /path/to/analytics-pipeline
docker build -t analytics-pipeline:latest -f ops/Dockerfile .
docker compose up -d --force-recreate
```

### GPU Issues

If GPU is not detected:

```bash
# Check GPU availability
docker compose exec celery-worker nvidia-smi

# Verify cuGraph installation
docker compose exec celery-worker python3 -c "import cugraph; print(cugraph.__version__)"
```

## Integration with Data Pipeline

The analytics pipeline consumes data from the data-pipeline's ClickHouse database and Memgraph instance. Ensure:

1. ClickHouse is accessible with transfer and label data
2. Memgraph is running and synchronized with transfer data
3. Network connectivity between analytics-pipeline and data-pipeline infrastructure

## Docker Compose Services

### Redis
- Message broker for Celery
- Port: 6379 (internal)
- Persistent volume for task queue

### Celery Worker
- Executes analytics tasks
- GPU-enabled for graph computations
- Auto-restart on failure
- Scalable (can run multiple workers)

### Celery Beat
- Schedules periodic tasks
- Single instance (do not scale)
- Uses `beat_schedule.json` for configuration

## Scaling

To run multiple workers for parallel processing:

```bash
docker compose up -d --scale celery-worker=3
```

## Documentation

- [Analytics Pipeline README](../README.md)
- [Scripts Documentation](../scripts/README.md)
- [Task Implementation](../packages/jobs/tasks/)

## Support

For issues or questions:
1. Check service logs with `docker compose logs`
2. Verify environment variables in `.env`
3. Ensure database connectivity
4. Check GPU availability for worker