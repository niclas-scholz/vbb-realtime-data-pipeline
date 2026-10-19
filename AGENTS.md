# AGENTS.md

Authoritative for agents. `agent.md` is a stale duplicate — ignore it. Commands live in `RUN_ORDER.md`.

## Rules
- Explain before changing. Change nothing unless explicitly asked. Smallest possible diff, no unrelated refactors.
- Before handoff: `uv run ruff check .` (no ruff config → defaults; covers `spark/` + `viz/`).

## Code style
Write the smallest change that works, in the existing style. This repo prefers failures you can see over code that absorbs them.
- Let exceptions propagate. No `try/except` around `requests`, Kafka, parquet or file I/O unless you actually recover, translate, or add context the traceback lacks.
- Keep the deliberate loud checks: `raise_for_status()` before overwriting `data/gtfs-rt.pb`, and the raise when `Producer.flush()` leaves messages queued. A non-zero exit is intended.
- No retries, sleeps or broker-health polling. `RUN_ORDER.md` says retry the command.
- No logging setup, custom exception classes, result wrappers, dataclasses, or helper functions with one caller.
- No invented fallbacks: no `or 0`, no `""` for a missing GTFS lookup, no guard for a column the schema just declared.
- No docstrings that restate the name, no TODO scaffolding, no unused parameters for later.
- Stdlib and existing dependencies; `print` is fine for progress. No pandas inside Spark jobs.
- If a requirement is ambiguous, ask — don't implement both branches.

## Shape
`main.py` (fetch GTFS-RT → Kafka) → `spark/bronze.py` → `spark/silver.py` → `spark/enrich.py` / `spark/gold.py` → `viz/app.py`.
- Two worlds, never mix: **host** runs `main.py` (`uv run python`, Python 3.13, `uv` only). **Everything Spark** runs via `docker exec ... spark-submit`.
- `gold/otp_by_route_hour` is a batch summary (`routeId x collection hour`). `viz/app.py` derives its views from **silver** (`stop_updates`) and only cross-checks gold, so gold is not a dashboard dependency.
- `main.py` writes `data/gtfs-rt.{pb,json}`, publishes, returns — never blocks. `spark_job.py` is a debug sink, writes nothing.
- Ports: kafka-ui :8080, Spark UI :8081, viz :8501.

## Order
`RUN_ORDER.md` is a real dependency chain, not style: `enrich.py` needs silver **and** an extracted `data/gtfs/`.
Steps 3-4 use `trigger(availableNow=True)` — they drain and exit by themselves. `enrich.py` / `gold.py` are batch jobs and also exit.

## Gotchas
- `main.py` replaces the collection session on every run: it truncates the `gtfs-rt` topic and deletes `data/checkpoints/`, `data/bronze/`, `data/silver/` before publishing. Run it only while no bronze/silver job is active: a kept checkpoint would skip the new offsets, kept outputs would keep a second session. `enrich.py` / `gold.py` are batch + `mode("overwrite")` — always rerunnable, but they keep the previous session's numbers until re-run.
- Never `outputMode("complete")`: memory/Delta sinks only, the parquet DataSource sink raises `STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE`. `gold.py` is a batch job for that reason.
- Spark paths are absolute container paths (`/opt/spark/work-dir/...`) — only `./spark`, `./spark_job.py`, `./data` are mounted. Relative and host paths cannot work.
- Every `spark/*.py` hardcodes `.master("spark://spark:7077")`, so `spark-submit` must run in the `spark` container. Never run PySpark on Windows (fails on `HADOOP_HOME` / `winutils.exe`).
- Kafka address depends on caller: host `localhost:29092`, containers `kafka:9092`.
- VBB RT is rate-limited (60 req/min) and needs `VBB_USER_AGENT` from `.env`. `main.py` never downloads the static feed; refresh `data/gtfs.zip` by hand weekly and extract it to `data/gtfs/`.
- `pandas>=3.0.6` raises a PySpark `FutureWarning` (wants `<3.0.0`). Leave it; `viz/app.py` needs current pandas.
- `viz/` needs `altair` and `pydeck`, which arrive transitively with `streamlit` — the container pip-installs streamlit on start, so there is no `pyproject.toml` entry. The host `uv` env has neither, so `import app` only works via `uv run --with streamlit`. The stop map (`st.pydeck_chart`) fetches CARTO basemap tiles, so the browser needs internet.
- **Delay semantics.** ~53 % of stop updates carry no `departure_delay` at all, and VBB sends `32767` (the GTFS-RT int16 max) as "no delay info". Never `coalesce` a delay to 0: that pulls the mean down and sends `max` to 9 hours. Average and percentile only over reported values, and always show coverage next to them.
- A `SKIPPED` stop is not an on-time stop. Restrict delay statistics to `scheduleRelationship == "SCHEDULED"` and report skipped separately.
- `gold.hour` is the **ingestion** hour, one value per `main.py` run, not the service hour. A time-of-day profile is not derivable until the feed has been collected repeatedly.

## Never commit
`data/` (~610 MB), `viz/data/`, `.idea/`, `.vscode/`, `.env`, `.venv/`. `viz/app.py` **is** source — commit it.