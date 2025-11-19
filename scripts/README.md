# Analytics Pipeline Scripts

This directory contains scripts for running analytics tasks both manually and as scheduled jobs.

## Directory Structure

```
scripts/
├── README.md                                    # This file
├── tasks/                                       # Production task scripts
│   ├── initialize_analyzers.py                 # Initialize analyzer configurations
│   ├── build_features.py                       # Build graph-based features
│   ├── detect_structural_patterns.py           # Detect structural patterns
│   └── detect_typologies.py                    # Detect typologies
└── examples/                                    # Development example scripts
    ├── example_initialize_analyzers.py         # Example: Initialize analyzers
    ├── example_build_features.py               # Example: Build features
    ├── example_detect_structural_patterns.py   # Example: Detect patterns
    ├── example_detect_typologies.py            # Example: Detect typologies
    └── pipeline_daily_analytics.py             # Example: Full pipeline
```

## Task Scripts (`scripts/tasks/`)

These scripts are designed for production use and accept command-line arguments.

### Prerequisites

All scripts require:
- A `.env` file in the project root with database credentials
- Access to ClickHouse database with transfer data
- Access to Memgraph for graph analytics (for features task)

### 1. Initialize Analyzers

Initializes analyzer configurations and prepares the system for analytics processing.

**Usage:**
```bash
python3 scripts/tasks/initialize_analyzers.py --network <network_name>
```

**Parameters:**
- `--network` (required): Network name (e.g., 'torus', 'bittensor')

**Example:**
```bash
python3 scripts/tasks/initialize_analyzers.py --network torus
```

**What it does:**
- Loads analyzer configurations
- Validates database connections
- Prepares system for subsequent analytics tasks

### 2. Build Features

Computes graph-based features for addresses using cuGraph-accelerated algorithms.

**Usage:**
```bash
python3 scripts/tasks/build_features.py \
    --network <network_name> \
    --window-days <days> \
    --processing-date <YYYY-MM-DD> \
    --batch-size <size>
```

**Parameters:**
- `--network` (required): Network name
- `--window-days` (required): Time window in days for feature computation
- `--processing-date` (required): Processing date in YYYY-MM-DD format
- `--batch-size` (optional): Batch size for processing (default: 1024)

**Example:**
```bash
python3 scripts/tasks/build_features.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09 \
    --batch-size 1024
```

**What it does:**
- Computes graph centrality metrics (PageRank, betweenness, etc.)
- Calculates network features for addresses
- Stores results in `analyzers_features` table
- Uses GPU acceleration via cuGraph when available

### 3. Detect Structural Patterns

Identifies structural patterns in transaction graphs such as fan-out, fan-in, and circular patterns.

**Usage:**
```bash
python3 scripts/tasks/detect_structural_patterns.py \
    --network <network_name> \
    --window-days <days> \
    --processing-date <YYYY-MM-DD>
```

**Parameters:**
- `--network` (required): Network name
- `--window-days` (required): Time window in days
- `--processing-date` (required): Processing date in YYYY-MM-DD format

**Example:**
```bash
python3 scripts/tasks/detect_structural_patterns.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09
```

**What it does:**
- Detects fan-out patterns (one-to-many transactions)
- Detects fan-in patterns (many-to-one transactions)
- Detects circular transaction patterns
- Stores detections in `analyzers_pattern_detections` table

### 4. Detect Typologies

Detects suspicious activity typologies and generates alerts.

**Usage:**
```bash
python3 scripts/tasks/detect_typologies.py \
    --network <network_name> \
    --window-days <days> \
    --processing-date <YYYY-MM-DD>
```

**Parameters:**
- `--network` (required): Network name
- `--window-days` (required): Time window in days
- `--processing-date` (required): Processing date in YYYY-MM-DD format

**Example:**
```bash
python3 scripts/tasks/detect_typologies.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09
```

**What it does:**
- Applies typology detection rules
- Generates alerts for suspicious patterns
- Creates alert clusters
- Stores alerts in `analyzers_alerts` and `analyzers_alert_clusters` tables

## Example Scripts (`scripts/examples/`)

These scripts are designed for development and testing with predefined parameters. They can be run directly without command-line arguments.

### Quick Start Examples

**Initialize Analyzers:**
```bash
python3 scripts/examples/example_initialize_analyzers.py
```

**Build Features:**
```bash
python3 scripts/examples/example_build_features.py
```

**Detect Structural Patterns:**
```bash
python3 scripts/examples/example_detect_structural_patterns.py
```

**Detect Typologies:**
```bash
python3 scripts/examples/example_detect_typologies.py
```

**Run Full Pipeline:**
```bash
python3 scripts/examples/pipeline_daily_analytics.py
```

### Full Pipeline Script

The `pipeline_daily_analytics.py` script runs all analytics tasks in sequence:

1. Initialize Analyzers
2. Build Features
3. Detect Structural Patterns
4. Detect Typologies

This provides a complete end-to-end analytics workflow.

## Running in Docker

### Via Docker Compose

```bash
# Build the image
cd analytics-pipeline
docker build -t analytics-pipeline:latest -f ops/Dockerfile .

# Start services
cd ops
docker compose up -d

# Run a task
docker compose exec celery-worker python3 scripts/tasks/build_features.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09
```

### Via Celery

Tasks can also be triggered via Celery (see `packages/jobs/tasks/`):

```python
from packages.jobs.tasks.build_features_task import BuildFeaturesTask
from packages.jobs.base.task_models import BaseTaskContext

context = BaseTaskContext(
    network='torus',
    window_days=90,
    processing_date='2025-09-09',
    batch_size=1024
)

task = BuildFeaturesTask()
result = task.delay(context)  # Async execution
```

## Environment Setup

Create a `.env` file in the project root with the following variables:

```bash
# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Memgraph (for graph features)
MEMGRAPH_HOST=localhost
MEMGRAPH_PORT=7687
MEMGRAPH_USER=
MEMGRAPH_PASSWORD=

# Redis (for Celery)
REDIS_URL=redis://localhost:6379/0

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

## Typical Workflow

For daily analytics processing:

```bash
# 1. Initialize analyzers (one-time setup or when config changes)
python3 scripts/tasks/initialize_analyzers.py --network torus

# 2. Build features for the analysis window
python3 scripts/tasks/build_features.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09 \
    --batch-size 1024

# 3. Detect structural patterns
python3 scripts/tasks/detect_structural_patterns.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09

# 4. Detect typologies and generate alerts
python3 scripts/tasks/detect_typologies.py \
    --network torus \
    --window-days 90 \
    --processing-date 2025-09-09
```

## Performance Considerations

### GPU Acceleration

The `build_features.py` task uses GPU acceleration via cuGraph when available:
- Significantly faster for large graphs (10-100x speedup)
- Falls back to CPU if GPU is unavailable
- Requires NVIDIA GPU with CUDA support

### Batch Processing

Use the `--batch-size` parameter to control memory usage:
- Larger batches: Faster but more memory
- Smaller batches: Slower but less memory
- Default: 1024 (balanced)

### Time Windows

The `--window-days` parameter affects:
- Amount of data processed
- Memory requirements
- Processing time

Common values:
- 7 days: Quick analysis
- 30 days: Monthly analysis
- 90 days: Quarterly analysis (recommended)

## Troubleshooting

### Import Errors

If you get import errors, ensure `PYTHONPATH` includes the project root:

```bash
export PYTHONPATH=/path/to/analytics-pipeline:$PYTHONPATH
```

### Database Connection Errors

Verify your `.env` file has correct credentials:

```bash
# Test ClickHouse connection
clickhouse-client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT

# Test Memgraph connection (for features)
mgconsole --host $MEMGRAPH_HOST --port $MEMGRAPH_PORT
```

### GPU Not Available

If GPU is not detected:

```bash
# Check GPU availability
nvidia-smi

# Verify CUDA installation
python3 -c "import torch; print(torch.cuda.is_available())"

# Check cuGraph
python3 -c "import cugraph; print(cugraph.__version__)"
```

The system will fall back to CPU if GPU is not available.

## Monitoring

### Task Logs

All tasks log to stdout. Monitor with:

```bash
# Local execution
python3 scripts/tasks/build_features.py ... 2>&1 | tee analytics.log

# Docker execution
docker compose logs -f celery-worker
```

### Celery Monitoring

When running via Celery:

```bash
# View scheduled tasks
celery -A packages.jobs.celery_app inspect scheduled

# View active tasks
celery -A packages.jobs.celery_app inspect active

# View worker stats
celery -A packages.jobs.celery_app inspect stats
```

## Related Documentation

- [Main README](../README.md) - Project overview
- [Operations Guide](../ops/README.md) - Docker deployment
- [Celery Configuration](../packages/jobs/celery_app.py) - Task scheduling
- [Task Implementations](../packages/jobs/tasks/) - Task code

## Support

For issues or questions:
1. Check logs for error messages
2. Verify environment variables
3. Ensure database connectivity
4. Check GPU availability (for features task)
5. Refer to main project documentation