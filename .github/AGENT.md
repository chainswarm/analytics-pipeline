<!-- AGENT Guide: Developer agent instructions for the Analytics Pipeline repo -->

# Purpose

This file gives AI agents and new contributors precise, actionable guidance for editing, debugging, and running the Analytics Pipeline repository. It complements the shorter `.github/copilot-instructions.md` by focusing on troubleshooting, common pitfalls, and developer debug patterns.

## Quick dev/start commands

- Start API (on-demand mode):

```bash
cp .env.example .env
# (set ANALYTICS_EXECUTION_MODE=on-demand in .env)
python scripts/start_api.py
```

- Start Celery worker and beat (daily mode):

```bash
# recommended for development: run two terminals
celery -A packages.jobs.celery_app worker --loglevel=info
celery -A packages.jobs.celery_app beat --loglevel=info
```

- Start both beat + worker in-process (dev helper):

```bash
python -m packages.jobs.celery_app
```

- Run full local stack (ClickHouse + Redis + services):

```bash
cd ops/infrastructure
docker-compose up -d
```

- Tests:

```bash
pytest
```

## Key files & where to look

- `README.md` — high-level usage and examples.
- `packages/jobs/celery_app.py` — Celery config, `beat_schedule` loading, and Loguru logging integration.
- `packages/jobs/tasks/` — task implementations (Daily pipeline & subtasks).
- `packages/api/main.py` + `scripts/start_api.py` — FastAPI startup and routes.
- `packages/ingestion/service.py` — ingestion orchestration; schema execution; extractor selection.
- `packages/ingestion/extractors/` — `S3Extractor`, `ClickHouseExtractor`, `HttpExtractor`.
- `packages/storage/schema/` — SQL schema / materialized view files executed at ingestion time.
- `packages/analyzers/` — feature engineering & structural pattern logic.

## Troubleshooting & common pitfalls (practical fixes)

- ClickHouse connection errors:
  - Verify env vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` in `.env`.
  - When running locally, prefer the `ops/infrastructure/docker-compose.yml` stack (starts ClickHouse).
  - Remember DB names are dynamic: code uses `analytics_{network}`. Don't hardcode `analytics`.

- Schema file not found / SQL execution fails:
  - `IngestionService._execute_schema_file` tries multiple candidate paths; the most reliable place for new SQL is `packages/storage/schema/`.
  - When calling `_execute_schema_file`, use the filename or the path under `packages/storage/schema/` (e.g. `packages/storage/schema/core_money_flows.sql`).
  - Check the function's `potential_paths` logic if you see path-resolution issues.

- Beat schedule not triggering tasks:
  - `packages/jobs/celery_app.load_beat_schedule` reads `packages/jobs/beat_schedule.json` by default.
  - You can override the path with `CELERY_BEAT_SCHEDULE_PATH` env var.
  - The loader accepts five-part cron strings (e.g. `0 2 * * *`) and converts them to `crontab()`.

- Missing extractor / unknown ingestion source:
  - `IngestionService._get_extractor` expects `ingestion_source` uppercased to one of `S3`, `CLICKHOUSE`, `HTTP`.
  - Provide correct `ingestion_source` or add new `BaseExtractor` in `packages/ingestion/extractors/` and extend `_get_extractor`.

- Logging feels inconsistent (Celery logs not appearing):
  - The repo uses `loguru` and installs an `InterceptHandler` in `packages/jobs/celery_app.py` to route stdlib logs to `loguru`.
  - Celery's root logger hijack is disabled in the config. If adding logging, prefer `loguru.logger`.

## How to run a single pipeline step locally

- Use the script runners in `scripts/tasks/` for quick runs (they wrap task invocation). Inspect the script header for required args.
- For on-demand testing, use the API route `/api/v1/pipelines/run` (see `packages/api/routes.py`) and call it with `curl`.

## Debugging tips for agents

- Reproduce state first: run `docker-compose` to bring up ClickHouse + Redis, then start a worker and the API.
- Add small, local changes and run tests — avoid broad refactors without tests.
- When editing task code, run a single task in isolation using its runner script or unit test to keep feedback tight.

## Adding new features / schema changes

- Schema: add new SQL files to `packages/storage/schema/`. Ingestion recreates MVs by executing those SQL files at runtime.
- Extractors: implement `BaseExtractor` in `packages/ingestion/extractors/` and ensure `ParquetLoader` compatibility.
- Tasks: add Celery tasks under `packages/jobs/tasks/` and ensure they are importable; Celery autodiscovers `packages.jobs.tasks`.

## When to ask for human help

- If you cannot reproduce ClickHouse errors locally using the Docker stack, escalate (these often involve credentials or environment-specific datasets).
- If changes require production data or schema migrations across networks, coordinate with maintainers — this repo isolates analytics DBs per network and such changes can be destructive.

---

If you want, I can:
- Expand this into a step-by-step onboarding checklist for contributors, or
- Add runnable examples that create a test network and run an end-to-end pipeline locally.
