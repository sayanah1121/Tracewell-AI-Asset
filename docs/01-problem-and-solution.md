# 01 – Problem & Solution

## 1. Problem statement

Companies have lost track of the AI running inside them. AI arrives through four doors:

| Door | Examples | Can a traditional network/security tool see it? |
|---|---|---|
| **Browser AI apps** | ChatGPT, Claude, Gemini, Perplexity | Partly: it sees the website, not the prompt or the file |
| **Desktop / IDE agents** | Cursor, Copilot, Cline, Windsurf, CLI agents | Barely: it looks like a normal app |
| **MCP servers & skills** | `mcp-postgres`, `mcp-filesystem`, Slack/Drive connectors | No: they run locally and act on behalf of the AI |
| **Local models** | Llama or Mistral running on the laptop | No: no network traffic at all |

As a result, security, compliance and finance teams can't answer:

1. **Inventory:** *What AI is in use, by whom, on which devices?*
2. **Risk:** *Which of those tools are dangerous, and why?*
3. **Data exposure:** *Is sensitive data (PII, PCI, PHI, secrets) going into AI tools?*
4. **Control:** *Can we stop a risky action **before** it completes?*
5. **Evidence:** *After an incident, can we rebuild exactly what happened and prove the record wasn't altered?*
6. **Value:** *Are the AI licences we pay for actually used?*

## 2. Why it is important

| Driver | Impact |
|---|---|
| **Data leakage** | Prompts and uploads are a new way out for data. A single paste of customer data or credentials can become a breach. |
| **Autonomous agents** | Agents run code, query databases and call APIs. A malicious or misconfigured MCP server gives an outsider a way in. |
| **AI supply-chain attacks** | Poisoned AI packages and plug-ins (e.g. the 2026 LiteLLM PyPI compromise) spread to thousands of machines within minutes. |
| **Regulation** | The EU AI Act, ISO/IEC 42001 and NIST AI RMF expect organisations to inventory, risk-assess and monitor the AI they use. |
| **Cost** | AI licences are bought separately by teams, with overlap and idle seats. |
| **Forensic gap** | SIEM and EDR tools don't record AI-specific context, so investigations stall. |

## 3. Solution summary

**Tracewell** is a data platform with two engines that share one lakehouse:

- **Batch intelligence (Spark + Airflow):** builds and maintains an **AI Asset Risk Catalog** from public sources and gives every asset a 0–10 risk score on three axes (Capability, Exposure, Governance), keeping the full history.
- **Real-time decisions (Flink + Redpanda):** processes every AI event from the fleet as it happens. It redacts sensitive data, enriches the event with the asset's risk, applies policy (allow/warn/block), and groups events into sessions.
- **Lakehouse (Iceberg):** one source of truth for raw, clean and aggregated data, with time travel for investigations.
- **Evidence:** a hash-chained, tamper-evident audit log.
- **Insight:** dashboards for shadow AI, risk posture, AI ROI and incident investigation.

## 4. Goals

| # | Goal | Measured by |
|---|---|---|
| G1 | Build a catalog of real AI assets with risk scores | ≥ 15,000 assets from 4 sources (curated catalog, npm, PyPI, OSV), all scored and explained |
| G2 | Make a real-time verdict for every event | p95 time from event to verdict < 1 s on a laptop |
| G3 | Never store raw sensitive data | 0 PII/PCI matches in silver/gold (automated check) |
| G4 | Handle late and duplicate events correctly | 0 duplicates; 100% of late events either merged or routed to the late topic |
| G5 | Exactly-once delivery into the lakehouse | Row counts match after a forced TaskManager crash |
| G6 | Correct historical investigation | As-of query returns the score that was valid at event time |
| G7 | Tamper-evident audit | The verifier detects a manually edited row |
| G8 | Useful dashboards | 4 dashboards: Shadow AI, Risk Posture, AI ROI, Investigation |

## 5. Non-goals

- Building a real endpoint agent or browser extension. Telemetry is **simulated**.
- Production-grade ML classification of prompts. Redaction and detection are rule-based.
- Multi-tenant SaaS, authentication or a UI beyond Superset.
- Competing with or copying any vendor's proprietary scoring. Tracewell is a transparent, documented approach of its own.

## 6. Users and the questions they ask

| User | Question | Where it's answered |
|---|---|---|
| Security analyst | "Which unapproved MCP servers appeared this week?" | Shadow AI dashboard |
| Security analyst | "Reconstruct session S-123 and show the blast radius" | Investigation dashboard / Spark job |
| CISO | "Which department carries the most AI risk?" | Risk Posture dashboard |
| Compliance officer | "Give me evidence per asset mapped to the frameworks" | `gold.compliance_evidence` export |
| IT / Finance | "Which AI licences are idle or duplicated?" | AI ROI dashboard |

## 7. Assumptions

- Devices emit events with a stable `event_id`, an event timestamp and identity information.
- Devices can be offline for up to 24 hours and then sync (so late data is expected).
- Public sources have rate limits, so ingestion is incremental and cached.
- All personal data in the simulator is fake (Faker), and card numbers are test numbers that pass the Luhn check.
