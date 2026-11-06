# 05 – Risk Scoring (Tracewell) & Policy Engine

Tracewell gives every AI asset a **0–10 risk score**, where higher means riskier. It's built from three axes, inspired by publicly described approaches (capability, exposure, governance). The weights and rules below are this project's own and are fully transparent.

## 1. Plain-language version

| Axis | The question it answers | Analogy |
|---|---|---|
| **Capability** | *How much damage could this tool do?* | A kitchen knife vs. a chainsaw |
| **Exposure** | *How widely is it used, and how close is it to sensitive data?* | Kept in a locked drawer vs. left in a school hallway |
| **Governance** | *Can we trust where it came from?* | A licensed, inspected product vs. something from an unknown seller |

## 2. Signals per axis (as implemented in `spark-jobs/catalog/transforms.py`)

Every rule is a Spark column expression (no Python UDFs), so scoring runs inside the JVM and is unit-tested in `tests/catalog/test_transforms.py`.

### Capability: what it can do
Each category has a starting value, because some kinds of asset exist to give AI callable tools:

| Category | Base | Why |
|---|---|---|
| `mcp_server`, `agent` | 3.0 | Their purpose is to hand an AI agent tools |
| `model`, `ai_service`, `extension` | 2.0 | Generate content, read what users give them |
| `package` | 1.0 | Libraries, passive on their own |

Signals are keyword rules over the asset's text (name, description, keywords, tags, declared package names) or curated tags for seed assets. They don't apply to models.

| Signal (tag) | Points | Example trigger words |
|---|---|---|
| `code_exec`: executes code / shell | +4.0 | shell, terminal, execute, subprocess, ssh, docker, kubectl |
| `file_write`: writes or deletes data | +2.5 | write, delete, insert, upload, deploy, commit, create issues |
| `credentials`: credentials or payments | +2.0 | credential, secret, oauth, vault, api key, wallet, payment |
| `network`: outbound network access | +2.0 | http, fetch, browser, scrape, email, slack, webhook |
| `file_read`: reads files or data stores | +1.5 | file, database, sql, postgres, google drive, notion, s3 |
| `autonomy`: acts autonomously | +1.5 | agent, autonomous, automation, orchestrate |
| generative model | +1.0 | pipeline `text-generation`, `image-text-to-text`, `any-to-any` |

`capability = min(10, base + signals)`

### Exposure: how far it reaches
| Signal | Contribution |
|---|---|
| Popularity percentile within its category (npm monthly downloads; PyPI has no download counts in its API, so PyPI-only packages get 0), in 0.5 steps | 0 – 5.0 |
| Curated prior for seed assets (`popularity_hint`) | replaces the percentile |
| Hosted remotely by a third party (curated `is_remote`, e.g. Sentry and Atlassian remote MCP servers) | +1.5 |

Percentiles are bucketed into 0.5 steps so day-to-day download noise doesn't create new SCD2 versions.
**Phase 2** adds the *dynamic* part from fleet telemetry (devices, identities, sensitive-data hits).

### Governance: how trustworthy (higher = worse); starts at 1.0, clamped to 0–10
| Signal | Points |
|---|---|
| Known-malicious version (OSV `MAL-*` advisory) | +5.0 |
| Open HIGH or CRITICAL advisory on the version in use | +4.0 |
| Any other open advisory on the version in use | +2.5 |
| No or unclear licence (missing, "other", "proprietary", "see license") | +2.0 |
| Restrictive or non-commercial licence (GPL, CC-BY-NC, research-only) | +1.0 |
| No provenance: MCP server or package with no linked repo; model or dataset from a publisher not on the trusted list | +2.0 |
| Archived, deprecated, or not updated for 12+ months | +1.5 |
| Not on the company allow-list (`seed/allow_list.csv`, matched on asset id or any alias) | +2.0 |
| Trusted publisher **and** permissive licence **and** actively maintained | −2.0 |

## 3. Composite score

```
risk_score = round( 0.35 × capability + 0.25 × exposure + 0.40 × governance , 1 )
```

Governance carries the largest weight. For third-party AI that the company didn't build, whether it can be trusted is the deciding factor.

**Floors (overrides), checked in order:**
1. Known-malicious version, or an open CRITICAL advisory → at least **9.0** (Critical)
2. Unsanctioned **and** capability ≥ 7 → at least **7.0** (High): shadow agents that can run code
3. Unsanctioned **and** governance ≥ 7 **and** capability ≥ 4 → at least **5.0** (Medium): an unvetted tool with real capability

**Bands:** `Critical ≥ 9.0` · `High 7.0–8.9` · `Medium 5.0–6.9` · `Low < 5.0`

**Explainability:** each asset stores `score_reasons`, every signal that fired, sorted by weight, e.g. `["can execute code or shell commands (+4)", "not on the company allow-list (+2)", ...]`.

### Calibration note
The first version used 0.40/0.35/0.25 weights and gave MCP servers a base capability of 1.0. On the real catalog that put **99.2% of assets in Low**: registry descriptions are short, so keyword signals rarely fire. Raising the base for tool-providing categories, weighting governance most, and adding floor 3 gave the distribution below. The re-score itself was a useful SCD2 test: 10,008 assets got a new version and the old ones were closed.

## 4. Results on the real catalog (25 Oct 2026, 22,063 assets)

| Category | Assets | Avg score | Low | Medium | High | Critical |
|---|---|---|---|---|---|---|
| mcp_server | 17,998 | 4.2 | 13,934 | 3,294 | 770 | 0 |
| model | 3,000 | 3.8 | 2,928 | 72 | 0 | 0 |
| dataset | 1,000 | 3.6 | 994 | 6 | 0 | 0 |
| package | 42 | 2.0 | 40 | 1 | 0 | 1 |
| ai_service | 12 | 5.0 | 7 | 3 | 2 | 0 |
| agent | 8 | 6.7 | 0 | 2 | 6 | 0 |
| extension | 3 | 4.6 | 3 | 0 | 0 | 0 |

Examples:

| Asset | Score | Band | Top reasons |
|---|---|---|---|
| `pkg:pypi/chromadb` | 9.0 | Critical | open high/critical advisory on the version in use; not on the allow-list; unclear licence |
| `agent:claude-code` (unsanctioned) | 7.0 | High | executes shell commands; writes data; not on the allow-list |
| `agent:cursor` (sanctioned) | 5.8 | Medium | executes shell commands; writes data; network access |

## 5. Policy engine

Policies are YAML files in `policies/`, published to the compacted `policy-updates` topic and held in Flink broadcast state. Edits take effect within seconds, with no redeploy.

The rule set in [`policies/default.yaml`](../policies/default.yaml):

| ID | Rule | Action |
|---|---|---|
| P001 | Payment-card data sent to an unsanctioned asset | block |
| P002 | Any use of a Critical-band asset | block |
| P003 | Secrets sent to an unsanctioned asset | block |
| P004 | Any sensitive data (PCI, PII, SECRET, GOV_ID) sent to AI | warn |
| P005 | MCP tool call that touches data (`read_file`, `query`, `execute`, …) on a High/Critical asset | warn |
| P006 | Spreadsheet (`xlsx`, `xls`, `csv`) uploaded to an unsanctioned asset | warn |
| P007 | Asset not found in the risk catalog | warn |

```yaml
- policy_id: P001
  name: Block payment data to unsanctioned AI
  priority: 10
  when:
    sensitive_types_any: [PCI]
    asset_sanctioned: false
  action: block
```

Conditions: `sensitive_types_any`, `asset_sanctioned`, `asset_band_in`, `asset_known`, `event_type_in`, `tool_name_in`, `file_ext_in`. All the conditions that are set must hold. Unit tests: `flink-jobs/tracewell-stream/src/test/java/ai/tracewell/stream/PolicyRuleTest.java`.

Rules are evaluated in `priority` order, the first match wins, and the default is `allow`. Each verdict records the `policy_id` that fired. Publish edits with `./tracewell.sh policies`; removed rules are deleted with a tombstone.

**Observed on simulated traffic** (17,329 events): allow 15,717 · P005 619 · P006 453 · P004 244 · P007 137 · P003 89 · P001 49 · P002 21.

## 6. Framework crosswalk (for `gold.compliance_evidence`)

Each score reason an asset carries is mapped to framework controls (code: `CROSSWALK` in [`spark-jobs/gold/marts.py`](../spark-jobs/gold/marts.py)). Fleet telemetry adds one more: sensitive data seen in the asset's events in the last 30 days.

| Signal (score reason or telemetry) | OWASP LLM Top 10 (2025) | NIST AI RMF | EU AI Act theme |
|---|---|---|---|
| can execute code or shell commands · can write or delete data · acts autonomously | LLM06 Excessive Agency | MANAGE | Human oversight |
| handles credentials or payments · hosted remotely by a third party | LLM02 Sensitive Information Disclosure | MEASURE / MAP | Data governance |
| sensitive data in the asset's events (last 30 days) | LLM02 Sensitive Information Disclosure | MEASURE | Data governance |
| has outbound network access (reads external content) | LLM01 Prompt Injection | MEASURE | Robustness & cybersecurity |
| known-malicious version · open advisory on the version in use | LLM03 Supply Chain | MAP | Robustness & cybersecurity |
| unverified publisher · no/unclear or restrictive licence · archived or stale | LLM03 Supply Chain | GOVERN | Transparency |
| not on the company allow-list | – | GOVERN | Human oversight |

> The mappings are a documented, best-effort learning aid, not legal advice.
