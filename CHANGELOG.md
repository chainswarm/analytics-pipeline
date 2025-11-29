# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2025-11-29

### Changed

- **Jobs module**: Migrated to use common celery/job/task infrastructure from `chainswarm_core.jobs`
  - All tasks now extend `BaseTask` directly from `chainswarm_core.jobs` instead of using local `BaseDataPipelineTask` alias
  - Removed `packages/jobs/base/base_task.py` (redundant alias file)
  - Removed `packages/utils/decorators.py` (now provided by `chainswarm_core.observability.log_errors`)
  - Standardized imports across all task files to use `chainswarm_core.jobs.BaseTask`

- **Celery app** (`packages/jobs/celery_app.py`):
  - Now uses `create_celery_app()` factory from `chainswarm_core.jobs`
  - Removed local Celery configuration and loguru integration (provided by chainswarm_core)

- **Task models**: Added `AnalyticsTaskContext` extending `BaseTaskContext`
  - New dataclass with analytics-specific fields: `start_date`, `end_date`, `batch_size`, `min_edge_weight`, `sampling_percentage`, `chain_min_length`, `chain_max_length`
  - Extends core context which provides `network`, `window_days`, `processing_date`

- **Base module exports** (`packages/jobs/base/__init__.py`):
  - Now re-exports `BaseTask`, `BaseTaskContext`, `BaseTaskResult` from `chainswarm_core.jobs`
  - Added `AnalyticsTaskContext` export

- **Package initialization** (`packages/__init__.py`):
  - Removed `setup_logger` function (now use `chainswarm_core.observability.setup_logger` directly)
  - Simplified to just load environment variables

- **Scripts**:
  - `scripts/start_api.py` - Updated to import `setup_logger` from `chainswarm_core.observability`
  - `scripts/tasks/run_initialize_analyzers.py` - Updated to use `AnalyticsTaskContext`
  - `scripts/tasks/run_ingest_batch.py` - Updated to use `AnalyticsTaskContext`
  - `scripts/tasks/run_detect_structural_patterns.py` - Updated to use `AnalyticsTaskContext`
  - `scripts/tasks/run_daily_analytics_pipeline.py` - Updated to use `AnalyticsTaskContext`
  - `scripts/tasks/run_build_features.py` - Updated to use `AnalyticsTaskContext`

- **Repositories**: Updated all repositories to use `log_errors` from `chainswarm_core.observability`:
  - `packages/storage/repositories/transfer_repository.py`
  - `packages/storage/repositories/transfer_aggregation_repository.py`
  - `packages/storage/repositories/money_flows_repository.py`

- **Ingestion loaders** (`packages/ingestion/loaders/parquet_loader.py`):
  - Updated to use `log_errors` from `chainswarm_core.observability`

### Removed

- `packages/jobs/base/base_task.py` - Replaced by `chainswarm_core.jobs.BaseTask`
- `packages/utils/decorators.py` - Replaced by `chainswarm_core.observability.log_errors`

### Dependencies

- Added `chainswarm-core>=0.1.8` to requirements.txt

## [0.1.0] - 2025-11-23

- Initial commit
 