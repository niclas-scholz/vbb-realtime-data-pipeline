# Run order

Copy-paste from repo root in PowerShell, in order. Steps 3-4 drain and exit by themselves (`availableNow`); steps 5-6 are batch jobs and also exit — do not wait, do not `Ctrl+C`.

## 0. Once

```powershell
uv sync
```

`.env` needs `VBB_USER_AGENT=your-identifying-user-agent` (60 req/min limit).
`enrich.py` additionally needs an extracted `data/gtfs/` — run once:

```powershell
Expand-Archive data\gtfs.zip -DestinationPath data\gtfs -Force
```

## 1-6. Infra to gold

```powershell
docker compose up -d
uv run python main.py
docker exec -e HOME=/tmp spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/spark/work-dir/spark/bronze.py
docker exec -e HOME=/tmp spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/spark/work-dir/spark/silver.py
docker exec -e HOME=/tmp spark /opt/spark/bin/spark-submit /opt/spark/work-dir/spark/enrich.py
docker exec -e HOME=/tmp spark /opt/spark/bin/spark-submit /opt/spark/work-dir/spark/gold.py
```

Verify:

```powershell
Get-ChildItem data\bronze\gtfs-rt, data\silver\stop_updates, data\gold\delay_validation, data\gold\otp_by_route_hour -Filter part-*.parquet | Group-Object DirectoryName | Select-Object Count, Name
```

`main.py` resets the pipeline on every run: it truncates the `gtfs-rt` topic and deletes `data/checkpoints/`, `data/bronze/`, `data/silver/`, so exactly one collection session exists after steps 2-6. Do not run step 2 while steps 3-6 are still running.

Three deliberate deviations:

- **No `-it` on `docker exec`.** `-t` needs a TTY and aborts when the block is pasted non-interactively (CI, `powershell -File`).
- **No broker health wait before `main.py`.** It can lose the race against a still-starting Kafka; `main.py` now exits non-zero when nothing is delivered, so retry rather than trusting a silent run.
- **No `--packages` for `enrich` / `gold`.** They read parquet, not Kafka.

`enrich.py` reports ~29% schedule match on a stale `data/gtfs.zip` — expected, refresh the zip.

`gold.py` delay stats cover `scheduleRelationship == SCHEDULED` only and drop values `>= 32767` (VBB's "no delay info" sentinel). `n_reported` is how many updates actually carried a delay, so `n_reported / n_total` is the coverage, ~47% on a normal feed. `n_total` counts stop updates, not trips.

## 7. Dashboard

```powershell
docker compose up -d viz
```

`http://localhost:8501`, reads `data/silver/stop_updates/` (the stop-level updates) and only cross-checks `data/gold/otp_by_route_hour/`. Line names, stop names and fare zones come from `data/gtfs/`; missing → internal ids plus a warning. Six tabs: Overview, Distribution, Lines, Stops & Zones, Data quality, Trip explorer. First start ~2 min (pip install). UIs: kafka-ui :8080, Spark master :8081.

## 8. Debug console (optional)

```powershell
docker exec -e HOME=/tmp -it spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/spark/work-dir/spark_job.py
```

Prints live Kafka rows to this terminal, runs until `Ctrl+C` (this one only). Writes nothing.

## 9. Checks + stop

```powershell
uv run ruff check .
docker compose ps
docker compose down
```

Reading never deletes: the debug console and bronze only advance offsets. Kafka address depends on the caller — host `localhost:29092`, containers `kafka:9092`. `main.py` is the one place that truncates the `gtfs-rt` topic, so re-running step 2 alone starts a fresh collection session.