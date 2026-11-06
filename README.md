# Tracewell: AI Asset Lineage & Risk Lakehouse

> A real-time + batch data platform that **finds every AI tool being used inside a company, scores how risky each one is, catches sensitive data going into AI, and keeps a tamper-proof record of what happened.**

**Stack:** Apache Flink · Apache Spark · Apache Iceberg · Apache Airflow · Redpanda (Kafka-compatible) · SeaweedFS (S3) · Trino · Superset · Docker Compose

---

## Part 1: The project in plain language

### The world right now

Ten years ago, if a company wanted to know what software its employees were using, it was fairly easy: IT installed everything.

Today it's different. Any employee can, in under a minute:
- open ChatGPT, Claude or Gemini in a browser and paste a customer list into it,
- install an AI coding assistant (Cursor, Copilot, Cline) that can **read every file on their laptop**,
- connect an AI "agent" to the company database, Slack or Google Drive through a plug-in (called an **MCP server**) downloaded from the internet,
- run an AI model directly on their laptop, which never touches the company network at all.

Nobody asked IT. Nobody told security. This is called **"Shadow AI"**: AI that is running inside the company without anyone in charge knowing about it.

### Why that is a real problem

Picture a bank or a hospital.

| What happens | Why it hurts |
|---|---|
| An analyst uploads `finance_q3.xlsx` to a free chatbot to "make a summary" | Confidential financial data now sits on a third-party server. It may be stored, logged or used for training. |
| A developer pastes code with a database password into an AI tool | The password has leaked. Anyone who gets access to that tool's logs can get into the database. |
| An engineer installs an unknown MCP plug-in from GitHub | That plug-in can read files, run commands and send data out, and it acts *on its own* when the AI asks it to. |
| A popular AI Python package gets hacked (this really happened to LiteLLM in 2026) | Every laptop that installed the bad version starts leaking credentials within minutes. |
| The company pays for ChatGPT, Claude, Copilot *and* Gemini licences | Half of those licences are never used, and nobody can prove which ones. |

And when something *does* go wrong, the security team can't answer the basic questions: *Which AI tool was it? What data did it touch? Was that tool even allowed at the time? Who else is using it?*

Real examples: in 2023 Samsung engineers pasted confidential source code into ChatGPT, and the company then banned the tool. New laws like the **EU AI Act** now require companies to know and document which AI systems they use, with heavy fines if they don't.

### Why it matters

- **Data leaks are expensive.** One leak of customer or payment data can mean regulatory fines, lawsuits and lost trust.
- **Regulators now demand evidence.** "We didn't know" is no longer an acceptable answer under the EU AI Act, ISO 42001 or data-protection laws.
- **AI agents act by themselves.** A chatbot only *talks*. An agent *does things*: it runs code, queries databases, sends emails. A risky agent is like an unsupervised intern with admin keys.
- **Traditional security tools are blind to it.** Firewalls and antivirus weren't built to understand prompts, AI models or MCP servers, and a lot of AI runs entirely on the laptop where network tools can't see it.

### What we are building: a simple analogy

Think of **airport security, but for AI inside a company**.

| Airport | Our project |
|---|---|
| A list of known dangerous items and banned passengers | **The AI Risk Catalog**: a database of thousands of real AI models, agents and MCP plug-ins, each with a risk score (like a credit score, but for danger) |
| Scanners at every gate watching every passenger and bag | **The live event stream**: every AI action on every laptop (a prompt typed, a file uploaded, a tool called) flows into the system in real time |
| The officer who checks the list and decides "pass", "inspect" or "stop" | **The real-time decision engine (Flink)**: within milliseconds it checks the action against the catalog and the company's rules, then says **allow, warn or block** |
| Blurring faces and passport numbers on the CCTV footage | **Automatic redaction**: card numbers, emails, phone numbers and passwords are masked *before* anything is stored |
| CCTV recordings that can't be edited | **The tamper-proof audit log**: every block or warning is chained together like a blockchain, so any edit is detectable |
| The control room with screens and reports | **The dashboards**: what AI is in use, how risky the company is, which licences are wasted, and an investigation view for incidents |

### What it will do, step by step

1. **Every day**, it builds a catalog of AI tools (a curated list of AI services, coding agents, browser extensions, MCP servers and local models, plus about 20,000 real AI packages discovered on npm and PyPI, and their known vulnerabilities from OSV) and gives each one a **risk score from 0 to 10**.
2. **All the time**, it receives activity from a simulated company of 500 laptops: who used which AI tool, what they typed, which files they uploaded.
3. **For every action**, within a fraction of a second, it:
   - hides any sensitive data,
   - looks up how risky the AI tool is,
   - applies the company's rules ("never send card data to an unapproved AI"),
   - decides **allow / warn / block**.
4. **It stores everything** in a modern data lake (Apache Iceberg), cleaned and organised.
5. **It answers questions** on dashboards:
   - *"What AI tools are being used that we never approved?"* (Shadow AI)
   - *"Which department is carrying the most AI risk?"* (Risk posture)
   - *"Which AI licences are we paying for but nobody uses?"* (Saving money)
   - *"For this incident last Tuesday: what happened, step by step, and how risky was the tool **on that day**?"* (Investigation)
6. **It proves nothing was tampered with**, through the chained audit log.

### Who would use it
- **Security team:** sees threats and blocks them.
- **Compliance / legal:** gets evidence for auditors.
- **IT / finance:** cuts wasted AI spending.
- **Management:** gets a simple picture of how safely the company uses AI.

---

## Part 2: The project for engineers

| Capability | How it's built |
|---|---|
| Ingest AI asset metadata from 5 public sources | Spark batch jobs into Iceberg bronze, run by Airflow |
| Match the same asset across sources | Spark entity resolution (normalised keys + fuzzy matching) |
| Risk score per asset, kept as history | Spark scoring → SCD Type 2 `MERGE INTO` on Iceberg |
| Real-time event processing | Redpanda → Flink (watermarks, dedupe, broadcast-state enrichment, session windows) |
| Privacy | Flink redaction before anything is persisted |
| No lost or duplicated rows | Flink checkpoints + Iceberg sink (exactly-once) |
| Analytics | Spark SQL gold marts → Trino → Superset |
| Investigation using past scores | As-of joins on SCD2 + Iceberg time travel |
| Tamper-evident audit | SHA-256 hash-chained Iceberg table + nightly verification |
| Operations | Airflow DAGs for ingestion, marts, compaction, health checks and audit verification |

### High-level architecture

```mermaid
flowchart LR
    subgraph Sources["AI asset sources"]
        CUR[Curated catalog<br/>services, agents, extensions,<br/>MCP servers, local models]
        PKG[PyPI / npm]
        OSV[OSV.dev]
    end

    SIM[Fleet Simulator<br/>500 laptops] -->|AI events| RP[(Redpanda<br/>event bus)]

    Sources --> SPARK[Apache Spark<br/>batch: ingest, match, score]
    SPARK -->|changed risk scores| RP
    RP --> FLINK[Apache Flink<br/>real-time: redact, enrich,<br/>decide, sessionize]

    SPARK <--> ICE[(Apache Iceberg<br/>on SeaweedFS S3)]
    FLINK --> ICE
    FLINK -->|allow / warn / block| RP

    FLINK -->|warn / block, hash-chained| AUD[(Audit log)]
    AUD --- ICE

    AF[Apache Airflow] -. schedules .-> SPARK
    AF -. health checks, resubmit .-> FLINK

    ICE --> TRINO[Trino] --> SUP[Superset dashboards]
    ICE --> OCSF[OCSF findings] --> SIEM[SIEM]
```

## Documentation

| Doc | What's inside |
|---|---|
| [01 – Problem & Solution](docs/01-problem-and-solution.md) | The problem in more depth, goals, non-goals, success criteria |
| [02 – Architecture](docs/02-architecture.md) | Full architecture, component diagrams, data flows, design decisions |
| [03 – Data Model](docs/03-data-model.md) | Event schemas, Redpanda topics, every Iceberg table |
| [04 – Pipelines](docs/04-pipelines.md) | Each Flink job, Spark job and Airflow DAG in detail |
| [05 – Risk Scoring](docs/05-risk-scoring.md) | The Tracewell risk scoring method and the policy engine |
| [06 – Setup & Operations](docs/06-setup-and-operations.md) | Local environment, versions, how to run, monitoring, troubleshooting |
| [07 – Roadmap](docs/07-roadmap.md) | Phased build plan with acceptance criteria |
| [Benchmarks](docs/benchmarks.md) | Throughput, latency, recovery, table health and memory, measured on the laptop stack |

## Project status
| Phase | Status |
|---|---|
| 0 · Environment | ✅ Done: 13-service Docker stack; smoke test passes (Spark + Flink + Airflow write one Iceberg table, Trino/Superset read it) |
| 1 · Risk catalog | ✅ Done: 20,011 AI assets (curated catalog + npm/PyPI discovery + OSV) resolved, scored, versioned (SCD2) and published to Redpanda by the `catalog_daily` DAG |
| 2 · Streaming pipeline | ✅ Done: 500 simulated laptops → Redpanda → Flink (redact, dedupe, enrich from the live catalog, allow/warn/block) → Iceberg. p95 latency 229 ms; catalog updates apply in seconds without a restart; exactly-once verified through crashes |
| 3 · Dashboards | ✅ Done: hourly Airflow `gold_marts` DAG (compaction → 6 Spark marts → 17 DQ checks) and 4 Superset dashboards built as code: Shadow AI (91% of shadow AI is invisible to network-only tools), Risk Posture with OWASP/NIST/EU AI Act evidence, AI ROI ($5.3k/month idle seats), Pipeline Health |
| 4 · Sessions + investigation | ✅ Done: Flink event-time session windows with a late-event side output; an hourly Spark backfill repairs sessions atomically (a laptop flushing 12 h of activity gives exactly the expected sessions); `investigate.py` puts the risk Flink enforced, the SCD2 as-of version, Iceberg time travel and today's profile side by side, plus blast radius; Investigation dashboard |
| 5 · Hardening & demos | ✅ Done: SHA-256 hash-chained audit log with nightly verification (`demo-tamper` catches an edited row); exactly-once proven through a TaskManager kill (`demo-crash`: 6,825 sent = 6,825 stored); schema change under the running job (`demo-schema`); nightly Iceberg maintenance (−94% files); Flink health checks with auto-resubmit; OCSF export for SIEMs; CI; [benchmarks](docs/benchmarks.md) |

### Quick start
```bash
cp .env.example .env
```
```bash
./tracewell.sh up
```
```bash
./tracewell.sh smoke
```
```bash
./tracewell.sh catalog
```
```bash
./tracewell.sh init && ./tracewell.sh build-flink && ./tracewell.sh submit-flink
```
```bash
./tracewell.sh simulate --rate 50 --duration 300 --scenarios rogue_mcp,finance_upload,secret_paste,poisoned_package
```
```bash
./tracewell.sh stream-check
```
```bash
./tracewell.sh gold && ./tracewell.sh dashboards
```
```bash
./tracewell.sh backfill --full && ./tracewell.sh session-check
```
```bash
./tracewell.sh investigate --session <session_id>
```
```bash
./tracewell.sh demo-tamper && ./tracewell.sh demo-crash && ./tracewell.sh demo-schema
```
```bash
./tracewell.sh bench 50 200 350 500
```
`./tracewell.sh up` starts the always-on stack (streaming + batch); Trino and Superset start on demand, or use `./tracewell.sh up all`.

UIs: Airflow http://localhost:8088 · Flink http://localhost:8081 · Spark http://localhost:8082 · Redpanda Console http://localhost:8080 · Trino http://localhost:8085 · Superset http://localhost:8089 · SeaweedFS filer http://localhost:8888
