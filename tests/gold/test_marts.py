from datetime import date, datetime

import pytest

import json

from marts import (ai_roi, compliance_evidence, fleet_risk_posture, ocsf_findings, pipeline_health, policy_hits,
                   shadow_ai)

EVENT_SCHEMA = ("event_id string, ts_event timestamp, ts_sent timestamp, processed_at timestamp, device_id string, "
                "identity string, department string, sensor string, asset_id string, asset_known boolean, "
                "asset_category string, risk_score_at_event double, band_at_event string, sanctioned_at_event boolean, "
                "event_type string, file_name string, bytes_out bigint, sensitive_types array<string>, sensitive_count int, "
                "policy_id string, action string, latency_ms bigint")
DIM_SCHEMA = ("asset_id string, asset_name string, category string, publisher string, licence string, band string, "
              "risk_score double, capability double, exposure double, governance double, sanctioned boolean, "
              "score_reasons array<string>, valid_from timestamp")
T = datetime(2026, 10, 26, 10, 0)


def ev(event_id, asset_id="a:x", *, ts=T, processed=None, device="WX-1", who="ann@corp.example", dept="finance",
       sensor="browser", known=True, category="ai_service", score=5.0, band="Medium", sanctioned=False,
       event_type="prompt", file=None, bytes_out=None, sensitive=0, sensitive_types=None, policy=None,
       action="allow", latency=100):
    return (event_id, ts, ts, processed or ts, device, who, dept, sensor, asset_id, known, category, score, band,
            sanctioned, event_type, file, bytes_out, sensitive_types, sensitive, policy, action, latency)


def events(spark, *rows):
    return spark.createDataFrame(list(rows), EVENT_SCHEMA)


def dim(spark, *rows):
    return spark.createDataFrame(list(rows), DIM_SCHEMA)


def asset(asset_id, band="Medium", sanctioned=False, reasons=()):
    return (asset_id, asset_id.split(":")[1].title(), "ai_service", "acme", "mit", band, 5.0, 5.0, 5.0, 5.0,
            sanctioned, list(reasons), T)


# ───────────────────────────── shadow AI ─────────────────────────────

def test_shadow_ai_keeps_unsanctioned_and_unknown_only(spark):
    evs = events(spark,
                 ev("1", "s:approved", sanctioned=True),
                 ev("2", "s:rogue", device="WX-1"), ev("3", "s:rogue", device="WX-2", sensor="network"),
                 ev("4", "mcp:unknown-z", known=False, category="unknown", band="High", sensor="device", action="warn",
                    file="customers.csv"),
                 ev("5", "mcp:unknown-z", known=False, category="unknown", band="High", sensor="device",
                    bytes_out=48_000_000))
    current = dim(spark, asset("s:approved", sanctioned=True), asset("s:rogue", band="High"))
    first_seen = spark.createDataFrame([("s:rogue", datetime(2026, 10, 1)), ("mcp:unknown-z", T)],
                                       "asset_id string, first_seen timestamp")
    rows = {r.asset_id: r for r in shadow_ai(evs, current, first_seen).collect()}

    assert set(rows) == {"s:rogue", "mcp:unknown-z"}
    rogue, unknown = rows["s:rogue"], rows["mcp:unknown-z"]
    assert (rogue.devices, rogue.events, rogue.current_band, rogue.is_new) == (2, 2, "High", False)
    assert rogue.seen_only_on_device is False           # the network sensor saw it too
    assert (unknown.category, unknown.asset_name, unknown.current_band) == ("unknown", "mcp:unknown-z", "High")
    assert unknown.is_new and unknown.seen_only_on_device and unknown.warns == 1
    assert (unknown.mb_out, unknown.files_touched, rogue.mb_out) == (48.0, ["customers.csv"], 0.0)


# ───────────────────────────── policy hits ─────────────────────────────

def test_policy_hits_group_blocks_and_warns_with_their_evidence(spark):
    evs = events(spark,
                 ev("1", "s:deepseek", action="warn", policy="P006", event_type="file_upload", file="finance_q3.xlsx"),
                 ev("2", "s:deepseek", action="warn", policy="P006", event_type="file_upload", file="customers.csv",
                    device="WX-2", who="cat@corp.example"),
                 ev("3", "s:deepseek", action="block", policy="P001", sensitive=1, sensitive_types=["PCI"]),
                 ev("4", "s:deepseek"))                                                     # allow: not a hit
    current = dim(spark, asset("s:deepseek"))
    policies = spark.createDataFrame([("P006", "Warn on spreadsheets"), ("P001", "Block payment data")],
                                     "policy_id string, policy_name string")
    rows = {r.policy_id: r for r in policy_hits(evs, current, policies).collect()}

    assert set(rows) == {"P006", "P001"}
    p006, p001 = rows["P006"], rows["P001"]
    assert (p006.hits, p006.devices, p006.people, p006.policy_name) == (2, 2, 2, "Warn on spreadsheets")
    assert p006.files == ["customers.csv", "finance_q3.xlsx"] and p006.event_types == ["file_upload"]
    assert (p001.action, p001.sensitive_types, p001.asset_name) == ("block", ["PCI"], "Deepseek")


# ───────────────────────────── fleet risk posture ─────────────────────────────

def test_posture_counts_each_asset_once_at_its_worst_band(spark):
    evs = events(spark,
                 ev("1", "a:x", band="Medium", score=5.0), ev("2", "a:x", band="High", score=7.5, action="block"),
                 ev("3", "a:y", band="Low", score=2.0, sanctioned=True, sensitive=1),
                 ev("4", "mcp:u", known=False, band="High", score=7.0, who="bob@corp.example"))
    r = fleet_risk_posture(evs).collect()[0]
    assert (r.events, r.active_people, r.distinct_assets) == (4, 2, 3)
    assert (r.assets_high, r.assets_medium, r.assets_low, r.assets_unknown) == (1, 0, 1, 1)
    assert r.mean_risk_score == pytest.approx((7.5 + 2.0 + 7.0) / 3, abs=0.01)
    assert r.max_risk_score == 7.5
    assert r.high_risk_events == 2 and r.high_risk_event_pct == 50.0
    assert (r.unsanctioned_events, r.sensitive_events, r.blocks) == (3, 1, 1)


# ───────────────────────────── AI ROI ─────────────────────────────

def test_ai_roi_seats_overlap_and_directory(spark):
    licences = spark.createDataFrame([
        ("t:chat", "Chat", "chat_assistant", "finance", 3, 60.0),
        ("t:copilot", "Copilot", "chat_assistant", "finance", 1, 30.0),
        ("t:code", "Code", "coding_assistant", "engineering", 2, 19.0),
    ], "tool_id string, tool_name string, function string, department string, seats int, cost_per_seat_month double")
    people = spark.createDataFrame([("ann@corp.example", "finance"), ("cat@corp.example", "finance")],
                                   "identity string, department string")
    evs = events(spark,
                 ev("1", "t:chat", who="ann@corp.example"), ev("2", "t:chat", who="ann@corp.example"),
                 ev("3", "t:chat", who="contractor@gmail.example"),          # not in the directory: no seat
                 ev("4", "t:copilot", who="ann@corp.example"), ev("5", "t:copilot", who="cat@corp.example"))
    months = spark.createDataFrame([(date(2026, 10, 1),)], "month date")
    rows = {r.tool_id: r for r in ai_roi(evs, licences, people, months).collect()}

    chat, copilot, code = rows["t:chat"], rows["t:copilot"], rows["t:code"]
    assert (chat.active_users, chat.idle_seats, chat.idle_cost, chat.utilisation_pct) == (1, 2, 120.0, 33.33)
    assert (copilot.active_users, copilot.idle_seats, copilot.users_without_seat) == (2, 0, 1)
    assert chat.overlap and copilot.overlap and chat.overlapping_tools == ["Chat", "Copilot"]
    assert (code.active_users, code.idle_seats, code.overlap) == (0, 2, False)   # licence with no usage


# ───────────────────────────── compliance evidence ─────────────────────────────

def test_compliance_maps_reasons_and_sensitive_data(spark):
    current = dim(spark,
                  asset("a:agent", band="High", reasons=["can execute code or shell commands (+4)",
                                                         "not on the company allow-list (+2)"]),
                  asset("a:pkg", reasons=["open high/critical advisory on the version in use (+4)"]),
                  asset("a:plain", band="Low"))
    recent = events(spark, ev("1", "a:agent", sensitive=2), ev("2", "a:agent", device="WX-2"))
    rows = {r.asset_id: r for r in compliance_evidence(current, recent).collect()}

    agent = rows["a:agent"]
    assert agent.owasp_llm == ["LLM02 Sensitive Information Disclosure", "LLM06 Excessive Agency"]
    assert agent.nist_ai_rmf == ["GOVERN", "MANAGE", "MEASURE"]
    assert (agent.in_use_30d, agent.devices_30d, agent.sensitive_events_30d) == (True, 2, 1)
    assert any(e.endswith("sensitive data in 1 events (last 30 days)") for e in agent.evidence)
    assert rows["a:pkg"].owasp_llm == ["LLM03 Supply Chain"] and not rows["a:pkg"].in_use_30d
    assert rows["a:plain"].evidence == [] and rows["a:plain"].owasp_llm == []


# ───────────────────────────── pipeline health ─────────────────────────────

def test_pipeline_health_resends_lateness_dlq(spark):
    bronze = spark.createDataFrame([("1", T), ("1", T), ("2", T), ("3", T)], "event_id string, ingested_at timestamp")
    silver = events(spark,
                    ev("1", latency=100), ev("2", latency=200, known=False),
                    ev("3", ts=datetime(2026, 10, 26, 9, 40), processed=T, latency=300))   # 20 min late
    dlq = spark.createDataFrame([(T,)], "timestamp timestamp")
    r = pipeline_health(bronze, silver, dlq).collect()[0]
    assert (r.records_in, r.resends_dropped, r.events_out) == (4, 1, 3)
    assert (r.late_events, r.unknown_asset_events, r.dlq_records) == (1, 1, 1)
    assert r.dlq_pct == 20.0 and r.latency_p50_ms == 200 and r.latency_max_ms == 300


def test_pipeline_health_reports_idle_hours(spark):
    bronze = spark.createDataFrame([("1", T)], "event_id string, ingested_at timestamp")
    silver = events(spark, ev("1"))
    dlq = spark.createDataFrame([], "timestamp timestamp")
    rows = pipeline_health(bronze, silver, dlq, until=datetime(2026, 10, 26, 13, 0)).orderBy("hour").collect()
    assert [r.hour.hour for r in rows] == [10, 11, 12, 13]
    assert [r.events_out for r in rows] == [1, 0, 0, 0] and rows[-1].latency_p95_ms is None


# ───────────────────────────── OCSF export ─────────────────────────────

def test_ocsf_findings_one_per_warn_or_block_with_severity(spark):
    rows = events(spark,
                  ev("1", "s:ok"),                                                            # allow: no finding
                  ev("2", "s:grok", band="Low", sensitive=1, sensitive_types=["PCI"], policy="P001", action="block"),
                  ev("3", "pkg:x", band="Critical", category="package", policy="P002", action="block", score=9.5),
                  ev("4", "s:gem", sensitive=1, sensitive_types=["PII"], policy="P004", action="warn"),
                  ev("5", "mcp:u", known=False, category=None, band=None, policy="P007", action="warn"))
    policies = spark.createDataFrame([("P001", "Block payment data to unsanctioned AI")], "policy_id string, policy_name string")
    out = {r.event_id: r for r in ocsf_findings(rows, policies).collect()}
    assert sorted(out) == ["2", "3", "4", "5"]
    assert {k: r.severity_id for k, r in out.items()} == {"2": 4, "3": 5, "4": 3, "5": 2}

    f = json.loads(out["2"].ocsf)
    assert (f["class_uid"], f["category_uid"], f["type_uid"], f["action_id"], f["action"]) == (2004, 2, 200401, 2, "Denied")
    assert f["finding_info"]["title"] == "Block payment data to unsanctioned AI"
    assert f["finding_info"]["analytic"] == {"uid": "P001", "name": "Block payment data to unsanctioned AI",
                                             "type_id": 1, "type": "Rule"}
    assert (f["actor"]["user"]["email_addr"], f["device"]["uid"]) == ("ann@corp.example", "WX-1")
    assert f["time"] == int(T.timestamp() * 1000) and f["unmapped"]["sensitive_types"] == ["PCI"]
    assert (f["risk_level"], f["risk_level_id"], f["risk_score"]) == ("Low", 1, 50)
    assert json.loads(out["4"].ocsf)["action_id"] == 1                       # warn: allowed, but reported
    assert json.loads(out["5"].ocsf)["resources"][0]["type"] == "unknown"     # never-seen asset still reported
