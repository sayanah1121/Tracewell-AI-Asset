# 06 – Setup & Operations

## 1. Prerequisites

| Requirement | Recommended |
|---|---|
| OS | Windows 11 with WSL2, macOS or Linux |
| Docker Desktop | Latest, with **≥ 8 GB RAM** (10–12 GB recommended), 6 CPUs and 40 GB disk allocated |
| Shell | Git Bash, WSL, macOS or Linux terminal (for `tracewell.sh`) |
| Java | JDK 17 (only needed on the host to build the Phase 2 Flink job JAR) |
| Python | 3.11 (simulator; Spark and Airflow run in containers) |
| Build tool | Maven or Gradle (Flink job JAR) |

## 2. Version matrix (pinned in Phase 0)

The **same Iceberg version (1.12.0)** is used by Spark and Flink. The REST catalog server speaks the same REST protocol.

| Component | Version | Image / key artefacts |
|---|---|---|
| Apache Flink | 1.20.3 (Java 17) | `tracewell/flink` = `flink:1.20.3-scala_2.12-java17` + `iceberg-flink-runtime-1.20`, `iceberg-aws-bundle`, `hadoop-client-api/runtime 3.3.6`, `flink-sql-connector-kafka 3.4.0-1.20`, S3 plugin |
| Apache Spark | 3.5.8 (Scala 2.12, Python 3.11) | `tracewell/spark` = Python 3.11 + Temurin 17 + `pyspark==3.5.8` + `iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle` |
| Apache Iceberg | 1.12.0 | REST catalog: `tracewell/iceberg-rest` = `apache/iceberg-rest-fixture:1.10.1` + PostgreSQL JDBC 42.7.12 (pointers in Postgres) |
| Apache Airflow | 3.3.2 (Python 3.11) | `tracewell/airflow` = `apache/airflow:3.3.2-python3.11` + Java 17 + `pyspark` + `apache-airflow-providers-apache-spark` |
| Redpanda | v25.3.18 | `redpandadata/redpanda`, Console `redpandadata/console:v3.12.0` |
| SeaweedFS | 4.47 | `chrislusf/seaweedfs`: S3-compatible object storage |
| Trino | 483 | `trinodb/trino`, Iceberg connector against the REST catalog |
| Superset | 6.1.0 | `tracewell/superset` = `apache/superset:6.1.0` + `trino` driver |
| PostgreSQL | 16.15 | Airflow metadata + Iceberg catalog (`iceberg_catalog` database) |
| Flink job | Java 17, Maven 3.9.13 (in Docker) | Kafka connector 3.4.0-1.20, Avro 1.11.5, Jackson 2.17.3 bundled in the job jar |
| Simulator | Python 3.11 | confluent-kafka 2.16, fastavro 1.13, Faker, PyYAML |

> **Why SeaweedFS instead of MinIO?** MinIO's public Docker Hub images were withdrawn, so a fresh `docker pull minio/minio` no longer works. SeaweedFS is actively maintained, implements the S3 API and works with Iceberg's `S3FileIO` (path-style access).
>
> **Why Airflow 3?** Airflow 2.x reached end of life in 2026. Airflow 3 runs here in `standalone` mode (scheduler, DAG processor, API server and triggerer in one container) with the LocalExecutor on Postgres.

## 3. Services (`docker-compose.yml`)

| Service | Profile | Host port(s) | Purpose |
|---|---|---|---|
| `redpanda` | core | 19092 (Kafka), 18081 (Schema Registry), 9644 (admin) | Event bus |
| `redpanda-console` | core | 8080 | Topic, schema and consumer-lag UI |
| `seaweedfs` | core | 8333 (S3 API), 8888 (filer UI) | Object storage: buckets `warehouse`, `checkpoints` (1.5 GB limit) |
| `s3-init` | core | – | One-shot job that creates the buckets |
| `catalog-db-init` | core | – | One-shot job that creates the Iceberg catalog's Postgres database and role |
| `iceberg-rest` | core | 8181 | Iceberg REST catalog (catalog name `tracewell`), pointers in Postgres |
| `flink-jobmanager` | core | 8081 | Flink UI / REST |
| `flink-taskmanager` | core | – | 1 instance, 4 slots, 1.5 GB, RocksDB state, checkpoints to `s3://checkpoints/flink` (retained on cancel) |
| `spark-master` | batch | 7077, 8082 (UI) | Spark standalone master |
| `spark-worker` | batch | 8083 (UI) | 2 cores, 1 GB |
| `postgres` | core, batch | – | Iceberg catalog + Airflow metadata |
| `simulator` | stream | – | Fleet simulator, run on demand with `./tracewell.sh simulate` |
| `airflow` | batch | 8088 | Airflow 3 UI + API (no login in local dev); 7 DAGs (`catalog_daily`, `gold_marts`, `late_event_backfill`, `iceberg_maintenance`, `audit_verify`, `flink_job_health`, `hello_spark_iceberg`). Image adds Java, PySpark and `confluent-kafka`; mounts `flink-jobs/` read-only for auto-resubmit |
| `trino` | bi (on demand) | 8085 | SQL engine (catalog `tracewell`); started by the first command that needs it |
| `superset` | bi (on demand) | 8089 | 5 dashboards built by `./tracewell.sh dashboards` (user `admin`, password in `.env`) |

Memory limits are set on every container; together they add up to ~14.7 GB, more than the default 8 GB WSL2 VM, so not everything may peak at once. `./tracewell.sh up` therefore starts only `core` + `batch` (~3.8 GB in use under load); Trino and Superset start on demand and the simulator only when asked (`up all` starts everything, ~5 GB). Measured per service in [benchmarks](benchmarks.md#5-memory-8-gb-docker-vm); for more headroom see section 9.

## 4. Repository layout

```
tracewell/
├── docker-compose.yml
├── tracewell.sh              # helper: up / down / smoke / catalog / simulate / gold / demos / bench ...
├── .github/workflows/ci.yml  # lint, pytest (Spark local mode), Flink build + JUnit, compose check
├── .env.example              # passwords & tokens (copy to .env, never commit .env)
├── infra/                    # Dockerfiles and configs per service
│   ├── flink/  spark/  airflow/  superset/
│   ├── trino/                # catalog/tracewell.properties, jvm.config
│   └── redpanda-console/
├── simulator/                # Python fleet generator                         (Phase 2)
├── flink-jobs/               # smoke/ now; tracewell-stream job (Java/Maven)        (Phase 2)
├── spark-jobs/
│   ├── smoke/                # Phase 0 smoke test
│   ├── catalog/              # Phase 1: ingest_*, resolve_entities, score_assets, publish, dq
│   │   └── seed/             # curated AI services/agents, allow-list, trusted publishers, seed packages
│   ├── gold/                 # Phase 3: marts.py, build_gold, compact_stream_tables, dq_gold
│   │   └── seed/             # simulated licences, HR directory (people), device inventory
│   ├── sessions/             # Phase 4: sessionize.py, late_event_backfill.py
│   ├── investigate/          # Phase 4: investigate.py (as-of join, time travel, blast radius)
│   ├── audit/                # Phase 5: verify_audit_chain.py
│   └── maintenance/          # Phase 5: iceberg_maintenance.py
├── airflow/dags/             # catalog_daily, gold_marts, late_event_backfill, iceberg_maintenance,
│                             # audit_verify, flink_job_health (+ hello_spark_iceberg)
├── schemas/                  # Avro schemas (asset_risk_update.avsc)
├── policies/                 # *.yaml policy rules                             (Phase 2)
├── sql/                      # DDL, investigation queries
├── dashboards/               # build_dashboards.py: Superset dashboards as code   (Phase 3)
├── tests/                    # pytest: catalog/, gold/, sessions/, investigate/, audit/
└── docs/
```

## 5. Getting started

Run these from **Git Bash** (Windows), WSL, macOS or Linux, in the project folder.

```bash
cp .env.example .env
```
Edit `.env` and replace every `change-me`.

```bash
./tracewell.sh up core batch bi
```
This builds the custom images (the first run takes several minutes and downloads about 15 GB of images), starts everything and waits until each service answers. All 13 containers use about 4.2 GB of RAM when idle, so an 8 GB Docker allocation is enough for Phase 0.

```bash
./tracewell.sh smoke
```
This runs the Phase 0 smoke test (section 5.1).

```bash
./tracewell.sh down
```
This stops everything but keeps the data. `./tracewell.sh reset` also deletes all volumes.

### 5.1 What the smoke test proves
1. **Spark** creates `tracewell.smoke.engine_writes` (an Iceberg v2 table, partitioned by `engine`) through the REST catalog and writes 2 rows to SeaweedFS.
2. **Flink SQL** connects to the same catalog, appends 2 rows and reads back all rows.
3. **Airflow** runs the DAG `hello_spark_iceberg`. Its `SparkSubmitOperator` submits a job to the Spark cluster, which appends 2 more rows.
4. **Trino** reads rows from all three writers.
5. **Spark** checks that all three engines wrote and that there is one Iceberg snapshot per commit.

### 5.2 Phase 1: build the risk catalog

```bash
./tracewell.sh catalog
```
This runs the `catalog_daily` DAG once, immediately (`airflow dags test`). The first run takes about 20–25 minutes on a slow connection; later runs are incremental. In the Airflow UI the DAG is paused by default; unpause it to run nightly at 02:00.

```bash
./tracewell.sh test
```
This runs the unit tests (pytest inside the Spark container).

No API tokens are needed: the catalog reads the curated list in `spark-jobs/catalog/seed/curated_assets.csv` and the public PyPI, npm and OSV APIs. To track another AI tool, add a row to that CSV (and its package, if any) and rerun the DAG. Package discovery is sized by `CATALOG_NPM_DISCOVER` (default 12,000) and `CATALOG_PYPI_DISCOVER` (8,000); `CATALOG_MAX_PACKAGES` and `CATALOG_MIN_ASSETS` can also be set in `.env`. The first run fetches every PyPI project one by one (about 10 min; the whole DAG about 22 min); later runs only refresh packages older than 7 days.

Look at the result in Trino (http://localhost:8085 or `docker compose --profile bi exec trino trino`):
```sql
SELECT band, count(*) FROM tracewell.silver.dim_asset_risk WHERE is_current GROUP BY 1;
```

### 5.3 Phase 2: the real-time pipeline

```bash
./tracewell.sh init
```
Creates all Redpanda topics, registers the event schema, creates `bronze.ai_events` and `silver.fct_ai_events` (Trino DDL in `sql/ddl/`), and publishes the policies.

```bash
./tracewell.sh build-flink
```
Compiles and unit-tests the Java job in a Maven container (dependencies are cached in the `tracewell_maven-cache` volume).

```bash
./tracewell.sh submit-flink
```
Submits `tracewell-stream`, cancelling a running copy first and **resuming from the newest checkpoint**. Use `--fresh` to start from scratch.

```bash
./tracewell.sh simulate --rate 50 --duration 300 --scenarios rogue_mcp,finance_upload,secret_paste,poisoned_package
```
Runs the fleet simulator; it prints live verdict counts and every block.

```bash
./tracewell.sh stream-check
```
Runs the acceptance checks (`sql/checks/stream_checks.sql`): dedupe, raw-PII scan, verdicts by policy, resolution rate, latency percentiles, late data, recent blocks.

```bash
./tracewell.sh policies
```
Re-publishes `policies/*.yaml`; Flink applies the change within seconds.

### 5.4 Phase 3: gold marts and dashboards

```bash
./tracewell.sh gold
```
Runs the `gold_marts` DAG once, now: compacts the stream tables, loads the reference seeds, builds the six marts and runs `dq_gold`. It takes about 12 minutes (one Spark app at a time, ~1–1.5 min each, mostly Spark start-up). To keep the marts current, unpause `gold_marts` in the Airflow UI (it runs hourly at :15); pause it again to free the CPU.

```bash
./tracewell.sh dashboards
```
Creates or updates the Trino connection, datasets, charts and the four dashboards in Superset (safe to rerun). Then open http://localhost:8089/superset/dashboard/shadow-ai/ (also `risk-posture`, `ai-roi`, `pipeline-health`).

To regenerate the simulated company directory (only if you change `--people`/`--devices`):
```bash
MSYS_NO_PATHCONV=1 docker compose --profile stream run --rm -v "$PWD/spark-jobs/gold/seed:/out" simulator python simulator.py --export-directory /out
```

### 5.5 Phase 4: sessions and investigation

After pulling Phase 4, create the sessions table and the `late-events` topic, rebuild the Flink job, and resubmit it. Adding the sessions sink changes the auto-generated ids of the Table API operators, so stop the simulator first (nothing pending) and allow unmapped state once:
```bash
./tracewell.sh init && ./tracewell.sh build-flink && ./tracewell.sh submit-flink --allow-non-restored
```
Sessionize the history already in silver (one time; safe to rerun):
```bash
./tracewell.sh backfill --full
```
Simulate a laptop that was offline for 12 hours (expected sessions are written to `simulator/out/offline_burst_expected.json`):
```bash
./tracewell.sh simulate --duration 600 --scenario-after 120 --scenarios offline_burst
```
Repair sessions after late events (the `late_event_backfill` DAG does this hourly at :45 once unpaused), then check them:
```bash
./tracewell.sh backfill && ./tracewell.sh session-check
```
Investigate a session (pick one from *Sessions worth investigating* on the Investigation dashboard):
```bash
./tracewell.sh investigate --session <session_id>
```
The report prints in the terminal and opens as the default on http://localhost:8089/superset/dashboard/investigation/.

### 5.6 Phase 5: hardening and demos
```bash
./tracewell.sh demo-tamper
```
Edits one audit row, shows `audit-verify` catching it, then restores it ([04, H.1](04-pipelines.md#h1-tamper-evident-audit-log)). `./tracewell.sh audit-verify` runs the check alone.

```bash
./tracewell.sh demo-crash
```
Kills the Flink TaskManager mid-stream and proves nothing was lost or duplicated (about 5 min; H.4).

```bash
./tracewell.sh demo-schema
```
Adds an event field (Avro v2 + Iceberg column) under the running job (H.5).

```bash
./tracewell.sh maintenance
```
Compaction, snapshot expiry and orphan cleanup on every table, the same as the nightly DAG (H.2).

```bash
./tracewell.sh bench 50 200 500
```
Throughput and latency at each simulator rate; results in [benchmarks](benchmarks.md).

New DAGs (paused like the others): `iceberg_maintenance` 03:30, `audit_verify` 04:00, `flink_job_health` every 15 min. To let the health check restart a dead job: `docker compose exec airflow airflow variables set flink_auto_resubmit true`.

**Memory profiles.** `./tracewell.sh up` now starts only `core` + `batch` (streaming + scheduled batch, about 4 GB in use). Trino and Superset (`bi`) start on demand the first time a command needs them (`dashboards`, `stream-check`, the demos); `./tracewell.sh up all` starts everything including the continuous simulator, which on an 8 GB Docker VM leaves little headroom (section 9).

Then open Superset (http://localhost:8089), the Flink UI (http://localhost:8081), the Airflow UI (http://localhost:8088) and Redpanda Console (http://localhost:8080).

## 6. Monitoring

| What | Where | Healthy when |
|---|---|---|
| Flink job status and checkpoints | Flink UI / REST `/jobs/:id/checkpoints` | RUNNING; last checkpoint < 2 min old; duration < 10 s |
| Consumer lag | Redpanda Console | Lag on `ai-events-raw` < 10k and not growing |
| Verdict latency | `gold.pipeline_health` | p95 < 1 s |
| Late / DLQ / unknown-asset % | `gold.pipeline_health` | late < 5%, DLQ < 0.1%, unknown < 2% |
| Batch runs | Airflow UI | All DAGs green; `audit_verify` never red |
| Table health | Spark `…files` / `…snapshots` metadata tables | Average file size > 64 MB after compaction; snapshots < 100 |

## 7. Testing strategy

| Level | What | Tools |
|---|---|---|
| Unit | Redaction detectors, Luhn, scoring rules, entity-resolution normalisation and link rules, package discovery, every gold mart incl. the OCSF mapping, session aggregation and id, audit chain hash (both with the same test vector in Java and Python), tamper/deletion/truncation detection, as-of join, snapshot lookup, commit-lag explanation, blast radius | JUnit (Flink, 18 tests), pytest (Spark, 54 tests: `./tracewell.sh test`) |
| Operator | Dedupe, watermarks, broadcast enrichment, session windows | Flink test harness (`KeyedOneInputStreamOperatorTestHarness`) |
| Integration | Simulator → Redpanda → Flink → Iceberg row counts across a crash | `./tracewell.sh demo-crash` on the compose stack |
| Data quality | The checks in [04 – Pipelines, Part E](04-pipelines.md#part-e-data-quality-checks) and `dq_gold` ([Part F.3](04-pipelines.md#f3-gold-data-quality-checks-dq_goldpy)) | Airflow tasks |
| Chaos demos | TaskManager kill, offline-device burst, schema evolution, audit tampering | `tracewell.sh` commands |

CI (GitHub Actions, `.github/workflows/ci.yml`): ruff (pyflakes rules) and all pytest tests with Spark in local mode (Java 17, the same pyspark/pytest versions as the Spark image); the Flink job built and unit-tested with Maven; `docker compose config` and a syntax check of `tracewell.sh`. The full stack is too heavy for a free CI runner, so the end-to-end demos run locally.

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Flink commits nothing to Iceberg | Checkpointing disabled (the Iceberg sink commits on checkpoint) | Enable checkpointing |
| `ClassNotFoundException` for Iceberg/S3 | Missing runtime or AWS bundle JARs | Add `iceberg-flink-runtime-1.20` + `iceberg-aws-bundle` to `/opt/flink/lib` |
| Spark and Flink see different tables | Different catalog configs | Point both at `http://iceberg-rest:8181`, same warehouse |
| Watermark doesn't advance | Idle Redpanda partitions | `withIdleness(Duration.ofMinutes(1))` |
| Thousands of tiny Parquet files; marts take minutes for a few rows | Flink commits every 20 s per partition | `compact_stream_tables` (first task of `gold_marts`) |
| `ValidationException: Deleted manifest … could not be found` | `rewrite_manifests` racing Flink's commits | Don't rewrite manifests on tables Flink is writing |
| Spark task fails with exit code -9, or `BlockManager re-registration` | OOM killer: the Docker VM ran out of RAM and swap | Run one Spark app at a time (`gold_marts` does); stop Superset/Trino while running batch; or give WSL more memory (section 9) |
| Docker becomes very slow | Not enough RAM | Start only the profiles you need; 1 Spark worker with 2 GB |
| GitHub API 403 | Rate limit | Use a token; cache responses; incremental ingestion |
| `pip install pyspark` times out during `docker build` | Slow connection; the wheel is about 300 MB | The Dockerfiles already use `--default-timeout=600 --retries 10`; just re-run the build |
| `curl: (23) ... ERROR on write` in Git Bash | `MSYS_NO_PATHCONV=1` stops Windows curl understanding `-o /dev/null` | Redirect with `>/dev/null` instead (as `tracewell.sh` does) |
| `Invalid call to dataType on unresolved object` | `F.nullif` bug in PySpark 3.5 | Use `F.when(col != value, col)` (see `resolve_entities.nullif`) |
| `ClassCastException: Integer cannot be cast to Short` in py4j | py4j turns a Java `Short` into a Python int | Avoid passing Java `Short` through Python (replication factor left to the broker) |
| Catalog jobs slow on tiny tables | 200 default shuffle partitions | `spark.sql.shuffle.partitions 8` in `spark-defaults.conf` |
| Iceberg commits fail with HTTP 500, catalog log shows `SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT` | SQLite catalog can't take concurrent commits | The catalog is on Postgres now; don't switch back to the SQLite fixture default |
| Catalog requests hang; log shows `UnknownHostException: seaweedfs` | SeaweedFS stopped (it was OOM-killed at 400 MB) | `docker compose ps seaweedfs`; it now has 1.5 GB and `restart: unless-stopped` |
| Flink job fails with `NoSuchMethodError` in Jackson | Mixed Jackson versions in the job jar | Keep the `jackson-bom` import in `pom.xml` |
| Flink SQL: `Encountered "identity"` | Reserved word | Quote with backticks |
| Flink job disappeared after `docker compose up` recreated the JobManager | No JobManager HA | `./tracewell.sh submit-flink` resumes from the newest retained checkpoint |
| `docker pull minio/minio` fails | MinIO's Docker Hub images were withdrawn | Use SeaweedFS (already configured) |
| Fresh events land in `late-events`; sessions missing | Event-time watermark frozen at "end of time" | Fixed in Phase 4 (control source marked idle); resubmit the job if running an older jar |
| Latest sessions missing after the simulator stops | Normal: the watermark stops with the events, so open sessions fire when new events arrive | Run more events, or accept; `session-check` leaves those runs out |
| Trino restarts (exit 137) during heavy queries | Docker VM out of RAM and swap; the VM OOM killer picks Trino | Stop Superset/Airflow while querying, or give WSL more memory (section 9) |
| `/opt/...` paths become `C:/Program Files/Git/opt/...` | Git Bash path conversion | `export MSYS_NO_PATHCONV=1` before `docker compose exec` (as `tracewell.sh` does) |
| `audit_verify` fails | A row of `silver.audit_log` was changed, deleted or the tail cut off | The log names chain, `seq_no` and event; the table's `$snapshots` history shows when and by whom. Restore from Iceberg history; never "fix" the hashes |
| `remove_orphan_files`: `No FileSystem for scheme "s3"` | Hadoop listing without an S3 filesystem | `prefix_listing => true` (as `iceberg_maintenance` does) |
| `flink_job_health` red: checkpoint too old | Job stuck or TaskManager overloaded/out of memory | Flink UI → Checkpoints; `./tracewell.sh submit-flink` resumes from the last retained checkpoint |

## 9. Giving Docker more memory (Windows / WSL2)

By default WSL2 gives Docker about half of the machine's RAM. To raise it, create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=11GB
processors=8
swap=4GB
```

Then restart WSL from PowerShell and start Docker Desktop again:

```bash
wsl --shutdown
```
