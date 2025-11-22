<!-- Copilot / AI contributor instructions for the Analytics Pipeline repo -->

# Quick Context

This repository implements an isolated Analytics Pipeline for blockchain data: ingestion -> feature engineering -> structural pattern detection -> alerting. Core runtime modes are `daily` (Celery beat + worker) and `on-demand` (FastAPI). Use these notes to be productive quickly.

## Big-picture architecture

- Orchestration: `packages/jobs/` (Celery tasks & schedule). See `packages/jobs/celery_app.py` and `packages/jobs/tasks/`.
- API: `packages/api/` exposes on-demand pipeline triggers. Entry: `packages/api/main.py` and `scripts/start_api.py`.
- Ingestion: `packages/ingestion/` contains `IngestionService` which picks an extractor (`S3`, `CLICKHOUSE`, `HTTP`) and loads parquet via `packages/ingestion/loaders/parquet_loader.py`.
- Storage: `packages/storage/` holds schema SQL files; ingestion recreates MVs by executing `packages/storage/schema/*.sql` (see `IngestionService._execute_schema_file`).
- Analyzers: `packages/analyzers/` contains feature and pattern logic used by tasks.

## Important runtime and dev workflows

- Start API (on-demand): `python scripts/start_api.py` (reads `.env` next to repo root). See example request in `README.md`.
- Celery (daily mode):

  - Start beat: `celery -A packages.jobs.celery_app beat --loglevel=info`
  - Start worker: `celery -A packages.jobs.celery_app worker --loglevel=info`

  `beat_schedule.json` is loaded by `packages/jobs/celery_app.py`. You can override its path with `CELERY_BEAT_SCHEDULE_PATH`.
- Docker Compose: `ops/infrastructure/docker-compose.yml` brings up ClickHouse + Redis + workers + API for integration testing / local stack.
- Tests: run `pytest` from repo root. (There are integration tests under `tests/integration`.)

## Project conventions and gotchas

- Environment: copy `.env.example` -> `.env` (root). Key vars: `ANALYTICS_EXECUTION_MODE`, `CLICKHOUSE_*`, `REDIS_URL`, `API_HOST`, `API_PORT`, `NETWORK`.
- Database naming: analytics DBs are created per network and use `analytics_{network}` (see README). Code expects dynamic DB names; do not hardcode a single DB name.
- Ingestion sources: `S3`, `CLICKHOUSE`, `HTTP` are supported. Review `packages/ingestion/service.py` to follow how extractors are chosen and how schema SQL files are located.
- Schema execution: `IngestionService._execute_schema_file` attempts multiple candidate paths. When adding SQL files, place them under `packages/storage/schema/` and reference by filename.
- Logging: `loguru` is used and Celery logging is intercepted in `packages/jobs/celery_app.py`. If you add log statements, prefer the existing logging pattern.
- Celery discovery: tasks are autodiscovered from `packages.jobs.tasks` (see `celery_app.autodiscover_tasks`). Keep task modules importable under that package path.

## Integration points & external dependencies

- ClickHouse: code uses `clickhouse_connect` client. Schema and MV creation happen at runtime.
- Redis: used as Celery broker + results (`REDIS_URL`).
- S3/HTTP endpoints: extractors may call external storage or HTTP APIs to fetch universal-format parquet files. See `packages/ingestion/extractors/`.

## Examples & snippets (use these exact paths)

- Trigger on-demand run (example from README):

```bash
curl -X POST "http://localhost:8001/api/v1/pipelines/run" \
     -H "Content-Type: application/json" \
     -d '{
           "network": "torus-benchmark-1",
           "date_range": {"start_date": "2023-10-01","end_date": "2023-10-05"},
           "window_days": 1
         }'
```

- Run API locally (dev):

```bash
cp .env.example .env
# set ANALYTICS_EXECUTION_MODE=on-demand
python scripts/start_api.py
```

- Run local daily stack via docker-compose:

```bash
cd ops/infrastructure
docker-compose up -d
```

## What to look for when modifying code

- If changing task scheduling, update `packages/jobs/beat_schedule.json` and verify `CELERY_BEAT_SCHEDULE_PATH` usage.
- If adding new ingestion formats, implement a new `BaseExtractor` in `packages/ingestion/extractors/` and update `IngestionService._get_extractor`.
- If adding new data schemas, add SQL to `packages/storage/schema/` and ensure `IngestionService._execute_schema_file` will locate it.

## Files that explain behavior (read these first)

- `README.md` (repo root) — overall usage and examples
- `packages/jobs/celery_app.py` — Celery config, beat schedule loading, logging
- `packages/ingestion/service.py` — ingestion orchestration and schema execution
- `packages/api/main.py` + `scripts/start_api.py` — API startup and routes
- `packages/analyzers/` — feature & pattern implementations used by tasks

---

If any of the above items are unclear or you want more specific examples (unit tests, data schema examples, or sample Celery task flow), tell me which area to expand and I will update this file.
