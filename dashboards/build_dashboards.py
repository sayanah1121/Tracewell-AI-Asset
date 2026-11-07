"""Create (or update) the Tracewell Superset dashboards through the Superset REST API.

Dashboards as code: the Trino connection, the datasets, every chart and the four dashboards are defined
here, so `./tracewell.sh dashboards` rebuilds them identically on any machine. Objects are matched by name and
updated in place, so rerunning is safe. Charts query gold tables live through Trino; the hourly
`gold_marts` DAG is what keeps them current.

Runs inside the superset container (it needs `requests` and SUPERSET_ADMIN_PASSWORD).
"""
from __future__ import annotations

import json
import os
import sys

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")      # inside the container
PUBLIC = os.environ.get("SUPERSET_PUBLIC_URL", "http://localhost:8089")  # the host port from docker-compose
DB_NAME = "Tracewell Trino"
DB_URI = "trino://superset@trino:8080/tracewell"


# ───────────────────────────── datasets ─────────────────────────────
# Physical tables, plus virtual (SQL) datasets for "latest day / month" views and for unnesting arrays.

DATASETS = {
    "shadow_ai": None,
    "fleet_risk_posture": None,
    "ai_roi": None,
    "pipeline_health": None,
    "investigations": None,
    "incident_timeline": None,
    "incident_blast_radius": None,
    "v_sessions_recent": """
        SELECT session_id, identity, device_id, department, session_start, session_end, duration_s, event_count,
               max_band, max_risk_score, blocks, warns, sensitive_events, unknown_assets, mb_out, source,
               array_join(assets_used, ', ') AS assets_used, array_join(event_types, ', ') AS event_types
        FROM silver.fct_sessions WHERE session_end >= current_timestamp - INTERVAL '2' DAY""",
    "v_shadow_ai_latest": """
        SELECT day, asset_id, asset_name, category, current_band, current_risk_score, is_new, first_seen,
               devices, identities, events, sensitive_events, blocks, warns, mb_out,
               array_join(departments, ', ') AS departments, array_join(files_touched, ', ') AS files_touched,
               CASE WHEN seen_only_on_device THEN 'Endpoint only' ELSE 'Endpoint + network' END AS visibility
        FROM gold.shadow_ai WHERE day = (SELECT max(day) FROM gold.shadow_ai)""",
    "v_policy_hits_latest": """
        SELECT policy_id, policy_name, policy_id || ' ' || policy_name AS policy, action, department, asset_name,
               hits, devices, people, first_hit, last_hit, mb_out,
               array_join(event_types, ', ') AS event_types, array_join(files, ', ') AS files,
               array_join(sensitive_types, ', ') AS sensitive_types
        FROM gold.policy_hits WHERE day = (SELECT max(day) FROM gold.policy_hits)""",
    "v_posture_latest": """
        SELECT * FROM gold.fleet_risk_posture WHERE day = (SELECT max(day) FROM gold.fleet_risk_posture)""",
    "v_roi_latest": """
        SELECT *, CASE WHEN overlap THEN array_join(overlapping_tools, ' + ') ELSE '' END AS overlap_with
        FROM gold.ai_roi WHERE month = (SELECT max(month) FROM gold.ai_roi)""",
    "v_compliance_controls": """
        SELECT c.asset_id, c.asset_name, c.category, c.band, c.devices_30d, t.control
        FROM gold.compliance_evidence c CROSS JOIN UNNEST(c.owasp_llm) AS t(control)
        WHERE c.in_use_30d""",
    "v_compliance_in_use": """
        SELECT asset_name, category, band, risk_score, sanctioned, devices_30d, sensitive_events_30d,
               array_join(owasp_llm, ', ') AS owasp_llm, array_join(nist_ai_rmf, ', ') AS nist_ai_rmf,
               array_join(eu_ai_act, ', ') AS eu_ai_act, array_join(evidence, '  •  ') AS evidence
        FROM gold.compliance_evidence WHERE in_use_30d""",
    "v_dq_latest": """
        SELECT check_name, CASE WHEN passed THEN 'pass' ELSE 'FAIL / warn' END AS result, detail, run_at
        FROM ops.dq_results
        WHERE suite = 'gold' AND run_at = (SELECT max(run_at) FROM ops.dq_results WHERE suite = 'gold')""",
}


# ───────────────────────────── chart helpers ─────────────────────────────

def m(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def big(ds, metric, subheader="", fmt="SMART_NUMBER"):
    return ds, {"viz_type": "big_number_total", "metric": metric, "subheader": subheader,
                "y_axis_format": fmt, "header_font_size": 0.3, "subheader_font_size": 0.125, "adhoc_filters": []}


def trend(ds, metric, x="hour", grain="PT1H", fmt="SMART_NUMBER", subheader=""):
    """Big number with a sparkline: the latest value of a time series."""
    return ds, {"viz_type": "big_number", "metric": metric, "x_axis": x, "time_grain_sqla": grain,
                "y_axis_format": fmt, "subheader": subheader, "show_trend_line": True, "start_y_axis_at_zero": True,
                "header_font_size": 0.3, "subheader_font_size": 0.125,
                "adhoc_filters": []}


def bar(ds, x, metrics, groupby=(), horizontal=False, stack=False, sort_by_metric=True, fmt="SMART_NUMBER",
        x_title="", y_title="", grain=None):
    return ds, {"viz_type": "echarts_timeseries_bar", "x_axis": x, "metrics": list(metrics),
                "time_grain_sqla": grain, "xAxisLabelRotation": 0 if horizontal or grain else 30,
                "groupby": list(groupby), "orientation": "horizontal" if horizontal else "vertical",
                "stack": "Stack" if stack else None, "x_axis_sort_series": "sum", "x_axis_sort_asc": False,
                "x_axis_sort": metrics[0]["label"] if sort_by_metric and not groupby else None,
                "row_limit": 1000, "show_legend": True, "legendOrientation": "top", "rich_tooltip": True,
                "y_axis_format": fmt, "x_axis_title": x_title, "y_axis_title": y_title,
                "x_axis_title_margin": 30 if x_title else 15, "y_axis_title_margin": 40 if y_title else 15,
                "truncateXAxis": True, "adhoc_filters": []}


def line(ds, metrics, x="hour", grain="PT1H", groupby=(), fmt="SMART_NUMBER", y_title="", log=False):
    return ds, {"viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": grain, "logAxis": log,
                "metrics": list(metrics), "groupby": list(groupby), "row_limit": 10000, "show_legend": True,
                "legendOrientation": "top", "rich_tooltip": True, "markerEnabled": True, "y_axis_format": fmt,
                "y_axis_title": y_title, "y_axis_title_margin": 40 if y_title else 15, "adhoc_filters": []}


def pie(ds, groupby, metric):
    return ds, {"viz_type": "pie", "groupby": [groupby], "metric": metric, "donut": True, "show_labels": True,
                "label_type": "key_percent", "labels_outside": True, "show_legend": True,
                "legendOrientation": "bottom", "row_limit": 20, "adhoc_filters": []}


def table(ds, columns, order_by, limit=50, descending=True, where=None):
    return ds, {"viz_type": "table", "query_mode": "raw", "all_columns": list(columns),
                "order_by_cols": [json.dumps([order_by, not descending])], "row_limit": limit,
                "server_pagination": False, "include_search": True, "show_cell_bars": True,
                "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": where}] if where else []}


# ───────────────────────────── dashboards ─────────────────────────────
# Each dashboard: slug, title, intro markdown, and rows of (chart name, chart spec, width out of 12, height).

EVENTS = m("SUM(events)", "Events")
SHADOW = {
    "slug": "shadow-ai", "title": "Tracewell · Shadow AI",
    "intro": ("### Shadow AI: AI in use that nobody approved\n"
              "Assets on the **latest day** that are not on the allow-list or not in the catalog at all. "
              "*Endpoint only* = no network sensor saw it, so a proxy or CASB alone would have missed it. "
              "Rebuilt hourly by Airflow `gold_marts` from `silver.fct_ai_events`."),
    "rows": [
        [("Shadow AI assets (latest day)", big("v_shadow_ai_latest", m("COUNT(DISTINCT asset_id)", "Assets"), "unsanctioned or unknown"), 3, 30),
         ("New today", big("v_shadow_ai_latest", m("COUNT_IF(is_new)", "New"), "first seen in the fleet today"), 3, 30),
         ("Seen only on the endpoint", big("v_shadow_ai_latest", m("1.0 * COUNT_IF(visibility = 'Endpoint only') / COUNT(*)", "Endpoint only"), "invisible to network-only tools", ".0%"), 3, 30),
         ("Unknown to the catalog", big("v_shadow_ai_latest", m("COUNT_IF(category = 'unknown')", "Unknown"), "provisionally High until profiled"), 3, 30)],
        [("Shadow AI by category and band", bar("v_shadow_ai_latest", "category", [m("COUNT(DISTINCT asset_id)", "Assets")], ["current_band"], stack=True, y_title="assets"), 6, 50),
         ("Visibility of shadow AI", pie("v_shadow_ai_latest", "visibility", m("COUNT(*)", "Assets")), 3, 50),
         ("Shadow AI events by day", bar("shadow_ai", "day", [m("SUM(events)", "Events"), m("SUM(blocks + warns)", "Blocked or warned")], sort_by_metric=False, grain="P1D"), 3, 50)],
        [("AI the catalog has never seen", table("v_shadow_ai_latest", ["asset_name", "devices", "events", "warns", "mb_out", "files_touched", "visibility", "departments", "first_seen"], "first_seen", where="category = 'unknown'"), 12, 45)],
        [("Most widespread shadow AI", table("v_shadow_ai_latest", ["asset_name", "category", "current_band", "current_risk_score", "devices", "events", "sensitive_events", "blocks", "warns", "mb_out", "visibility", "departments"], "devices", 300, where="category <> 'unknown'"), 12, 70)],
    ],
}
POSTURE = {
    "slug": "risk-posture", "title": "Tracewell · Fleet Risk Posture",
    "intro": ("### How risky is the AI each department actually uses?\n"
              "Per department on the **latest day**: assets by risk band (each asset counted once at its worst "
              "band), every block and warn the stream issued (`gold.policy_hits`), and the framework controls behind "
              "the risk (`gold.compliance_evidence`; a best-effort learning aid, not legal advice)."),
    "rows": [
        [("Events on High / Critical / unknown AI", big("v_posture_latest", m("1.0 * SUM(high_risk_events) / SUM(events)", "High-risk share"), "share of the day's AI events", ".1%"), 3, 30),
         ("Blocked (latest day)", big("v_posture_latest", m("SUM(blocks)", "Blocks"), "policy verdicts"), 3, 30),
         ("Warned (latest day)", big("v_posture_latest", m("SUM(warns)", "Warns"), "policy verdicts"), 3, 30),
         ("Events with sensitive data", big("v_posture_latest", m("SUM(sensitive_events)", "Sensitive"), "redacted before storage"), 3, 30)],
        [("AI assets in use by band", bar("v_posture_latest", "department", [m("SUM(assets_critical)", "Critical"), m("SUM(assets_high)", "High"), m("SUM(assets_unknown)", "Unknown"), m("SUM(assets_medium)", "Medium"), m("SUM(assets_low)", "Low")], stack=True, y_title="distinct assets"), 6, 55),
         ("Policy verdicts by department", bar("v_posture_latest", "department", [m("SUM(blocks)", "Blocks"), m("SUM(warns)", "Warns"), m("SUM(sensitive_events)", "Sensitive events")]), 6, 55)],
        [("Policy hits by rule (latest day)", bar("v_policy_hits_latest", "policy", [m("SUM(hits)", "Hits")], ["action"], horizontal=True, stack=True), 5, 55),
         ("Newest policy hits", table("v_policy_hits_latest", ["first_hit", "policy_id", "action", "department", "asset_name", "hits", "devices", "event_types", "files", "sensitive_types", "mb_out"], "first_hit", 500), 7, 55)],
        [("Mean risk score of assets in use", bar("v_posture_latest", "department", [m("MAX(mean_risk_score)", "Mean risk"), m("MAX(max_risk_score)", "Max risk")], y_title="Risk score (0-10)"), 6, 50),
         ("In-use assets per OWASP LLM risk", bar("v_compliance_controls", "control", [m("COUNT(DISTINCT asset_id)", "Assets in use")], horizontal=True), 6, 50)],
        [("Compliance evidence for assets in use", table("v_compliance_in_use", ["asset_name", "category", "band", "risk_score", "sanctioned", "devices_30d", "sensitive_events_30d", "owasp_llm", "nist_ai_rmf", "eu_ai_act", "evidence"], "risk_score", 500), 12, 75)],
    ],
}
ROI = {
    "slug": "ai-roi", "title": "Tracewell · AI ROI",
    "intro": ("### Are we paying for AI seats nobody uses?\n"
              "Licences (simulated purchases in `silver.dim_licence`) against the people in the HR directory who "
              "actually used each tool in the **latest month**. *Overlap* = a department pays for two tools that do the same job."),
    "rows": [
        [("Monthly AI licence spend", big("v_roi_latest", m("SUM(monthly_cost)", "Spend"), "USD per month", "$,.0f"), 3, 30),
         ("Idle seat cost", big("v_roi_latest", m("SUM(idle_cost)", "Idle cost"), "USD per month on unused seats", "$,.0f"), 3, 30),
         ("Seat utilisation", big("v_roi_latest", m("1.0 * SUM(active_users) / SUM(seats)", "Utilisation"), "active users / seats bought", ".0%"), 3, 30),
         ("Overlapping licences", big("v_roi_latest", m("COUNT_IF(overlap)", "Overlaps"), "tool × department rows with a same-job tool"), 3, 30)],
        [("Seat utilisation by tool and department", bar("v_roi_latest", "department", [m("1.0 * SUM(active_users) / SUM(seats)", "Utilisation")], ["tool_name"], fmt=".0%", y_title="active / seats"), 7, 55),
         ("Idle cost by tool", bar("v_roi_latest", "tool_name", [m("SUM(idle_cost)", "Idle cost")], horizontal=True, fmt="$,.0f"), 5, 55)],
        [("Licences vs usage", table("v_roi_latest", ["tool_name", "department", "seats", "active_users", "utilisation_pct", "idle_seats", "idle_cost", "users_without_seat", "overlap_with", "monthly_cost"], "idle_cost"), 12, 60)],
    ],
}
HEALTH = {
    "slug": "pipeline-health", "title": "Tracewell · Pipeline Health",
    "intro": ("### Is the real-time path healthy?\n"
              "Hourly from `gold.pipeline_health` (bronze by write time, silver by verdict time, DLQ by Kafka time). "
              "SLOs: p95 verdict latency < 1 s · late < 5% · unknown assets < 2% · DLQ < 0.1%. "
              "The table shows the latest `dq_gold` run."),
    "rows": [
        [("Verdict latency p95", trend("pipeline_health", m("MAX(latency_p95_ms)", "p95 ms"), subheader="ms, device send → verdict"), 3, 34),
         ("Events processed per hour", trend("pipeline_health", m("SUM(events_out)", "Events")), 3, 34),
         ("Late events", trend("pipeline_health", m("1.0 * SUM(late_events) / NULLIF(SUM(events_out), 0)", "Late"), fmt=".1%", subheader="> 5 min behind device time"), 3, 34),
         ("Unknown-asset events", trend("pipeline_health", m("1.0 * SUM(unknown_asset_events) / NULLIF(SUM(events_out), 0)", "Unknown"), fmt=".1%"), 3, 34)],
        [("Verdict latency percentiles", line("pipeline_health", [m("MAX(latency_p50_ms)", "p50"), m("MAX(latency_p95_ms)", "p95"), m("MAX(latency_p99_ms)", "p99")], y_title="ms (log scale)", log=True), 6, 50),
         ("Throughput per hour", line("pipeline_health", [m("SUM(records_in)", "Records in (bronze)"), m("SUM(events_out)", "Events out (silver)"), m("SUM(resends_dropped)", "Resends dropped")]), 6, 50)],
        [("Lateness, unknown assets and DLQ", line("pipeline_health", [m("MAX(late_pct)", "Late %"), m("MAX(unknown_asset_pct)", "Unknown asset %"), m("MAX(dlq_pct)", "DLQ %")], y_title="%"), 6, 50),
         ("Data-quality checks (latest run)", table("v_dq_latest", ["result", "check_name", "detail"], "result", descending=False), 6, 50)],
    ],
}
INVESTIGATION = {
    "slug": "investigation", "title": "Tracewell · Investigation",
    "intro": ("### What happened in this session, and how risky was each tool *then*?\n"
              "Pick an investigation (newest first; create one with `./tracewell.sh investigate --session <id>`). Each event shows the "
              "risk Flink **enforced**, the SCD2 **as-of** version, the Iceberg **time-travel** view of the table at that moment, "
              "and **today's** profile. Blast radius = everyone else who used the session's risky assets in 30 days."),
    "filter": {"name": "Investigation", "dataset": "investigations", "column": "investigation_id",
               "datasets": {"investigations", "incident_timeline", "incident_blast_radius"},
               "default_sql": "SELECT max(investigation_id) FROM gold.investigations"},
    "rows": [
        [("Events in the session", big("investigations", m("MAX(events)", "Events"), "deduplicated, redacted"), 3, 30),
         ("Assets riskier or safer since", big("investigations", m("MAX(assets_changed_since)", "Changed"), "band or allow-list changed after the event"), 3, 30),
         ("Time travel agrees with SCD2", big("investigations", m("MAX(tt_agreement_pct) / 100.0", "Agreement"), "rest = commit-lag window (explained)", ".1%"), 3, 30),
         ("Unexplained mismatches", big("investigations", m("MAX(tt_unexplained)", "Unexplained"), "must be 0"), 3, 30)],
        [("Risk score then vs now", line("incident_timeline", [m("MAX(score_recorded)", "Enforced by Flink"), m("MAX(score_asof)", "SCD2 as-of"), m("MAX(score_tt)", "Time travel"), m("MAX(score_today)", "Today")], x="ts_event", grain="PT1M", y_title="max Tracewell risk score"), 7, 50),
         ("Blast radius: devices exposed", bar("incident_blast_radius", "asset_name", [m("MAX(devices)", "Devices"), m("MAX(people)", "People")], horizontal=True), 5, 50)],
        [("Session timeline", table("incident_timeline", ["seq", "ts_event", "sensor", "event_type", "asset_name", "action", "policy_id", "band_recorded", "band_asof", "band_tt", "band_today", "changed_since_event", "mismatch_reason", "file_name", "dest_host"], "seq", 1000, descending=False), 12, 80)],
        [("Blast radius", table("incident_blast_radius", ["asset_name", "band_today", "score_today", "devices", "people", "departments", "events", "sensitive_events", "blocks", "first_seen", "last_seen"], "devices"), 12, 40)],
        [("Sessions worth investigating (last 2 days)", table("v_sessions_recent", ["session_id", "identity", "device_id", "session_start", "duration_s", "event_count", "max_band", "blocks", "warns", "sensitive_events", "unknown_assets", "mb_out", "source", "assets_used"], "blocks", 200), 12, 60)],
        [("Past investigations", table("investigations", ["investigation_id", "created_at", "session_id", "identity", "device_id", "session_start", "events", "assets_changed_since", "tt_agreement_pct", "tt_unexplained", "max_blast_devices"], "created_at", 100), 12, 40)],
    ],
}
DASHBOARDS = [SHADOW, POSTURE, ROI, HEALTH, INVESTIGATION]
LABEL_COLORS = {  # the same meaning gets the same colour on every chart
    "Critical": "#B91C1C", "High": "#EA580C", "Medium": "#CA8A04", "Low": "#16A34A", "Unknown": "#7C3AED",
    "block": "#DC2626", "Blocks": "#DC2626", "warn": "#F59E0B", "Warns": "#F59E0B", "Sensitive events": "#7C3AED",
    "Endpoint only": "#0EA5E9", "Endpoint + network": "#64748B",
}


# ───────────────────────────── Superset API ─────────────────────────────

class Superset:
    def __init__(self, base: str, user: str, password: str):
        self.base, self.s = base, requests.Session()
        r = self.s.post(f"{base}/api/v1/security/login",
                        json={"username": user, "password": password, "provider": "db", "refresh": True})
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        self.s.headers["X-CSRFToken"] = self.call("GET", "/api/v1/security/csrf_token/")["result"]
        self.s.headers["Referer"] = base

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        r = self.s.request(method, f"{self.base}{path}", json=body)
        if r.status_code >= 400:
            sys.exit(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
        return r.json() if r.content else {}

    def find(self, kind: str, col: str, value: str) -> int | None:
        q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}], "page_size": 100})
        rows = self.call("GET", f"/api/v1/{kind}/?q={requests.utils.quote(q)}")["result"]
        return rows[0]["id"] if rows else None

    def upsert(self, kind: str, col: str, value: str, body: dict, update: dict | None = None) -> int:
        existing = self.find(kind, col, value)
        if existing:
            if update != {}:  # {} = nothing to change (a physical dataset)
                self.call("PUT", f"/api/v1/{kind}/{existing}", update if update is not None else body)
            return existing
        return self.call("POST", f"/api/v1/{kind}/", body)["id"]


def query_one(sql: str):
    """Run a scalar query on Trino directly (the trino driver ships in the Superset image)."""
    import trino
    cur = trino.dbapi.connect(host="trino", port=8080, user="superset", catalog="tracewell").cursor()
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None


def native_filters(dash: dict, datasets: dict[str, int], chart_ds: dict[int, str]) -> list[dict]:
    """A single-select filter whose default is the newest investigation at build time.

    (Superset's "default to first item" fills the box but does not filter the charts on first load, so the
    default is set explicitly; `./tracewell.sh investigate` re-runs this for the Investigation dashboard.)
    Charts on datasets without the filter column are excluded from its scope.
    """
    f = dash.get("filter")
    if not f:
        return []
    value = query_one(f["default_sql"])
    mask = ({"extraFormData": {"filters": [{"col": f["column"], "op": "IN", "val": [value]}]},
             "filterState": {"value": [value], "label": value}, "ownState": {}}
            if value else {"extraFormData": {}, "filterState": {}, "ownState": {}})
    return [{
        "id": f"NATIVE_FILTER-{f['column']}", "type": "NATIVE_FILTER", "name": f["name"], "filterType": "filter_select",
        "targets": [{"datasetId": datasets[f["dataset"]], "column": {"name": f["column"]}}],
        "controlValues": {"enableEmptyFilter": True, "defaultToFirstItem": False, "multiSelect": False,
                          "searchAllOptions": False, "inverseSelection": False, "sortAscending": False},
        "defaultDataMask": mask,
        "cascadeParentIds": [], "description": "",
        "scope": {"rootPath": ["ROOT_ID"], "excluded": [cid for cid, ds in chart_ds.items() if ds not in f["datasets"]]},
        "chartsInScope": [cid for cid, ds in chart_ds.items() if ds in f["datasets"]], "tabsInScope": [],
    }]


def layout(dash: dict, chart_ids: dict[str, int]) -> dict:
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
           "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": dash["title"]}}}

    def add_row(i: int, items: list[dict]) -> None:
        row = f"ROW-{i}"
        pos["GRID_ID"]["children"].append(row)
        pos[row] = {"type": "ROW", "id": row, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for item in items:
            item["parents"] = ["ROOT_ID", "GRID_ID", row]
            pos[row]["children"].append(item["id"])
            pos[item["id"]] = item

    add_row(0, [{"type": "MARKDOWN", "id": "MARKDOWN-intro", "children": [],
                 "meta": {"width": 12, "height": 22, "code": dash["intro"]}}])
    for i, row in enumerate(dash["rows"], start=1):
        add_row(i, [{"type": "CHART", "id": f"CHART-{chart_ids[name]}", "children": [],
                     "meta": {"width": w, "height": h, "chartId": chart_ids[name], "sliceName": name}}
                    for name, _, w, h in row])
    return pos


def main() -> None:
    api = Superset(BASE, "admin", os.environ["SUPERSET_ADMIN_PASSWORD"])
    db = api.upsert("database", "database_name", DB_NAME,
                    {"database_name": DB_NAME, "sqlalchemy_uri": DB_URI, "expose_in_sqllab": True})
    print(f"database '{DB_NAME}' id={db}")

    datasets = {}
    for name, sql in DATASETS.items():
        body = {"database": db, "schema": "gold", "table_name": name, **({"sql": sql.strip()} if sql else {})}
        datasets[name] = api.upsert("dataset", "table_name", name, body,
                                    update={"sql": sql.strip()} if sql else {})
        api.call("PUT", f"/api/v1/dataset/{datasets[name]}/refresh")  # pick up new or changed columns
    print(f"{len(datasets)} datasets: {', '.join(datasets)}")

    only = set(sys.argv[1:])
    for dash in [d for d in DASHBOARDS if not only or d["slug"] in only]:
        dash_id = api.upsert("dashboard", "slug", dash["slug"],
                             {"dashboard_title": dash["title"], "slug": dash["slug"], "published": True})
        chart_ids, chart_ds = {}, {}
        for row in dash["rows"]:
            for name, (ds, params), _, _ in row:
                chart_ds[name] = ds
                body = {"slice_name": name, "viz_type": params["viz_type"], "datasource_id": datasets[ds],
                        "datasource_type": "table", "dashboards": [dash_id],
                        "params": json.dumps({**params, "datasource": f"{datasets[ds]}__table"})}
                chart_ids[name] = api.upsert("chart", "slice_name", name, body)
        chart_ds = {chart_ids[n]: d for n, d in chart_ds.items()}
        api.call("PUT", f"/api/v1/dashboard/{dash_id}", {
            "dashboard_title": dash["title"], "published": True,
            "position_json": json.dumps(layout(dash, chart_ids)),
            "json_metadata": json.dumps({"refresh_frequency": 600, "color_scheme": "supersetColors",
                                         "label_colors": LABEL_COLORS,
                                         "native_filter_configuration": native_filters(dash, datasets, chart_ds),
                                         "timed_refresh_immune_slices": [], "expanded_slices": {}})})
        print(f"dashboard {dash['title']}: {len(chart_ids)} charts -> {PUBLIC}/superset/dashboard/{dash['slug']}/")


if __name__ == "__main__":
    main()
