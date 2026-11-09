# Benchmarks

Measured on one laptop: Windows 11, Docker Desktop with an **8 GB WSL2 VM** (12 CPUs), every service of the stack running in it. These are single-run numbers from a shared, noisy machine; they show the shape of the system, not a tuned production cluster.

Reproduce with `./tracewell.sh bench 50 200 350 500` (stream), `./tracewell.sh demo-crash` (recovery) and `./tracewell.sh maintenance` (compaction).

## 1. Streaming: throughput and verdict latency

Flink job `tracewell-stream`: parallelism 2 on one TaskManager (4 slots, 1.5 GB), RocksDB state, checkpoints every 20 s, exactly-once Iceberg and Kafka sinks, the 20,011-asset catalog in broadcast state. Each step sends 90 s of simulated fleet traffic (500 devices, ~1% duplicates, offline flushes, clock skew, sensitive prompts) at the given rate.

Final run (3 Nov 2026, 90 s per step):

| Target rate | Events | Processed/s | p50 | p95 | p99 | Late % | Max lag |
|---|---|---|---|---|---|---|---|
| 50/s | 5,216 | 58 | 111 ms | 166 ms | 194 ms | 0.00 | 1,426 |
| 200/s | 18,760 | 202 | 146 ms | 760 ms | 2.2 s | 0.00 | 5,080 |
| 350/s | 32,240 | 312 | 157 ms | 8.8 s | 12.0 s | 0.00 | 8,476 |
| 500/s | 45,754 | 401 | 309 ms | 22.0 s | 23.6 s | 0.00 | 15,762 |

Across three runs the p95 at 200/s ranged from 0.4 s to 1.3 s and at 500/s from 10 s to 27 s: the laptop is shared with everything else, so the tail moves between runs while the median and the throughput stay put.

- **events/s** = rows committed to `silver.fct_ai_events` ÷ the time between the first and last verdict of the step.
- **Latency** = device send time (`ts_sent`) → verdict (`processed_at`), from silver.
- **late %** = events processed more than 5 min after `ts_event` (beyond the watermark bound).
- **max lag** = the largest backlog of the Flink consumer group on `ai-events-raw` (`rpk group describe`), sampled every 5 s. Flink commits its offsets on each checkpoint, so the figure always includes up to one checkpoint's worth of records (20 s × rate): ~1,100 at 50/s is that, not a backlog.

**Reading it:** at 50 events/s p95 stays far below 1 s. At 200/s the job still keeps up (processed = sent) but the tail reaches about 1 s. Beyond that it falls behind: the median stays low (events that find no queue are fast), but the backlog and the tail grow for as long as the overload lasts, and drain afterwards. On this laptop the ceiling is roughly 300–400 events/s, and the 1 s p95 goal is safe at 50/s and borderline at 200/s. The usual next steps would be more TaskManager slots and parallelism, a larger checkpoint interval, or splitting the Iceberg sinks into their own job.

The SLO used by `gold_marts` (`dq_gold`: p95 < 1 s) holds at the default rate of 50 events/s (Phase 2 measured p50 130 ms · p95 229 ms · p99 710 ms).

## 2. Recovery and correctness under failure

| Test | Result |
|---|---|
| TaskManager killed mid-stream (`demo-crash`, 120 s at 50/s) | **6,825** records sent → **6,825** bronze rows; **6,749** distinct events → **6,749** silver rows and **6,749** committed verdicts. Nothing lost, nothing duplicated |
| Time to recover | Events sent while the TaskManager was down waited up to ~95 s (worst hourly p95 in `gold.pipeline_health` for that hour): 10 s down + Flink's restart back-off + restore from the last checkpoint + catch-up |
| Audit chain after the crash | intact: 3,025 links, 4 chains, 0 gaps |
| Schema change under the running job (`demo-schema`) | Avro v2 + Iceberg `ADD COLUMN`: same job, 0 restarts, 0 dead-letter records, 3,069 v2 events landed |
| Tampering (`demo-tamper`) | 1 edited row of 632 detected, by chain and `seq_no`; restored, verified again |

## 3. Batch

| Job | Size | Time |
|---|---|---|
| `catalog_daily` (first run) | 20,011 assets: 12,012 npm + 7,963 PyPI + 48 curated; 19,975 OSV version checks | 22 min (15 of them fetching packages) |
| `gold_marts` (7 marts incl. OCSF, 19 DQ checks) | ~100k events in the 2-day window, 20,011 assets | ~9 min (one Spark app at a time) |
| `audit_verify` | ~3,000 links | about 1 min, mostly Spark start-up |
| `iceberg_maintenance` | 26 tables | not timed |

## 4. Table health (Iceberg)

| | Before | After |
|---|---|---|
| Stream tables, data files (first run) | 1,332 | 80 (**−94%**) |
| Stream tables, data files (next run, 1 h later) | 607 | 72 (−88%) |
| `silver.fct_ai_events` snapshots | 129 | 48 (expired > 2 days) |
| Expired data files freed / orphan files removed | | ~1,500 / 93 |

Flink commits every 20 s into up to 16 buckets per day partition, so while events flow `silver.fct_ai_events` gains up to ~2,900 small files an hour (16 per 20 s checkpoint); the hourly `compact_stream_tables` (last two days) plus the nightly full compaction keep it at a few dozen.

## 5. Memory (8 GB Docker VM)

Measured with `docker stats` while the stream ran at 200–500 events/s:

| Service | Profile | Limit | Under load (stream at 500/s) | Idle |
|---|---|---|---|---|
| redpanda | core | 900 MB | 199 MB | 338 MB |
| redpanda-console | core | 200 MB | 39 MB | 44 MB |
| seaweedfs (S3) | core | 1.5 GB | 562 MB | 860 MB |
| postgres | core + batch | 256 MB | 65 MB | 53 MB |
| iceberg-rest | core | 400 MB | 181 MB | 182 MB |
| flink-jobmanager | core | 900 MB | 443 MB | 387 MB |
| flink-taskmanager | core | 1.76 GB | 981 MB | 824 MB |
| spark-master | batch | 900 MB | 179 MB | 153 MB |
| spark-worker | batch | 1.76 GB | 136 MB (no job running) | 81 MB |
| airflow | batch | 3 GB | 1.0 GB | 1.25 GB |
| trino | bi (on demand) | 1.37 GB | 856 MB | 947 MB |
| superset | bi (on demand) | 1 GB | 163 MB | 230 MB |
| simulator | stream (when asked) | 512 MB | 73 MB | – |
| **core + batch** (`./tracewell.sh up`) | | | **3.8 GB** | **4.2 GB** |
| **everything** | | | **4.9 GB** | **5.4 GB** |

All memory limits together add up to ~14.7 GB, so the stack relies on not everything peaking at once; Spark jobs (driver in the Airflow container, executor in the worker) add 1–2 GB while they run. That is why `./tracewell.sh up` now starts only `core` + `batch`, Trino and Superset start on demand, and heavy checks run one at a time. With more RAM, raise the WSL2 limit ([06, section 9](06-setup-and-operations.md#9-giving-docker-more-memory-windows--wsl2)).
