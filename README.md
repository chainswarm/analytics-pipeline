# Analytics Pipeline

**Blockchain Analytics and Pattern Detection System**

The Analytics Pipeline is a specialized system extracted from the data-pipeline to handle advanced analytics, feature engineering, and pattern detection on blockchain data.

## Overview

Analytics Pipeline is a standalone service that provides:

- **Feature Engineering**: Build graph-based features from blockchain transaction data
- **Structural Pattern Detection**: Identify common transaction patterns (mixing, layering, etc.)
- **Typology Detection**: Detect complex multi-step suspicious activity patterns
- **Alert Generation**: Create and cluster security alerts based on detected patterns

## Architecture

### Data Access Model

- **Read-Only Access**: Analytics pipeline has read-only access to `core_*` tables from data-pipeline
- **Owned Tables**: Analytics pipeline owns and manages all `analyzers_*` tables
- **Complete Independence**: No code dependencies on data-pipeline, fully standalone

### Key Components

#### 1. Analyzers (`packages/analyzers/`)
- **Features**: Graph analytics and address feature computation
- **Structural Patterns**: Common blockchain pattern detection
- **Typologies**: Complex multi-hop suspicious activity detection

#### 2. Storage Layer (`packages/storage/`)
- **Schema**: SQL table definitions for all `analyzers_*` tables
- **Repositories**: Data access layer for features, patterns, alerts, and clusters

#### 3. Job System (`packages/jobs/`)
- **Celery Integration**: Distributed task processing
- **Beat Schedule**: Automated periodic analytics jobs
- **Tasks**: Feature building, pattern detection, typology detection

#### 4. Utilities (`packages/utils/`)
- Shared utility functions for decimal handling, decorators, and pattern utilities

## Database Schema

### Analytics-Owned Tables

- `analyzers_features`: Computed blockchain address features
- `analyzers_pattern_detections`: Detected structural patterns
- `analyzers_alerts`: Generated security alerts
- `analyzers_alert_clusters`: Grouped related alerts
- `analyzers_computation_audit`: Computation tracking and auditing

### Core Tables (Read-Only)

- `core_transfers`: Blockchain transfer events
- `core_money_flows`: Money flow tracking
- `core_address_labels`: Address labeling data

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# ClickHouse Database
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=blockchain

# Celery/Redis
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Network
NETWORK=torus

# Processing
WINDOW_DAYS=180
BATCH_SIZE=1000
CHUNK_SIZE=10000

# Logging
LOG_LEVEL=INFO
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

### Running Analytics Tasks

#### Feature Building
```bash
# Build features for specific time window
python -m packages.jobs.tasks.build_features_task --network torus --days 7
```

#### Structural Pattern Detection
```bash
# Detect structural patterns
python -m packages.jobs.tasks.detect_structural_patterns_task --network torus --days 7
```

#### Typology Detection
```bash
# Detect complex typologies
python -m packages.jobs.tasks.detect_typologies_task --network torus --days 7
```

### Running Celery Workers

```bash
# Start Celery worker
celery -A packages.jobs.celery_app worker --loglevel=info

# Start Celery beat scheduler (for periodic tasks)
celery -A packages.jobs.celery_app beat --loglevel=info
```

## Project Structure

```
analytics-pipeline/
├── packages/
│   ├── analyzers/          # Analytics engines
│   │   ├── features/       # Feature engineering
│   │   ├── structural/     # Structural pattern detection
│   │   └── typologies/     # Typology detection
│   ├── storage/            # Data access layer
│   │   ├── repositories/   # Repository classes
│   │   └── schema/         # SQL table definitions
│   ├── jobs/               # Celery task system
│   │   ├── base/           # Base task classes
│   │   └── tasks/          # Analytics tasks
│   └── utils/              # Shared utilities
├── scripts/
│   └── tasks/              # Standalone task scripts
├── ops/                    # Operations & deployment
├── tests/                  # Test suite
│   └── integration/
│       └── analyzers/
├── docs/                   # Documentation
├── .env.example            # Environment configuration template
├── requirements.txt        # Python dependencies
└── pytest.ini             # Test configuration
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run specific test module
pytest tests/integration/analyzers/

# Run with coverage
pytest --cov=packages
```

### Code Style

The project follows the same conventions as data-pipeline:
- Python 3.9+
- Type hints where appropriate
- Docstrings for public methods
- PEP 8 code style

## Deployment

See `ops/` directory for deployment configurations and Docker setups.

## Integration with Data Pipeline

While analytics-pipeline is completely independent code-wise:

1. **Data Flow**: Reads blockchain data from `core_*` tables populated by data-pipeline
2. **Schema Coordination**: Uses compatible data types and conventions
3. **Network Support**: Supports same blockchain networks (Torus, Bittensor, Bitcoin, etc.)

## License

See LICENSE file for details.

## Support

For issues and questions, please refer to the documentation in the `docs/` directory.