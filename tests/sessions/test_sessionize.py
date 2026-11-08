import hashlib
from datetime import datetime, timedelta

from sessionize import sessionize

SCHEMA = ("event_id string, ts_event timestamp, device_id string, identity string, department string, sensor string, "
          "event_type string, asset_id string, asset_known boolean, risk_score_at_event double, band_at_event string, "
          "sensitive_count int, action string, bytes_out bigint")
T0 = datetime(2026, 10, 26, 9, 0)          # 1793005200000 ms
GAP_MS = 10 * 60_000


def ev(i, minutes, device="WX-1", who="ann@corp.example", asset="service:chatgpt", known=True, score=4.4,
       band="Low", sensitive=0, action="allow", bytes_out=None):
    return (f"e{i}", T0 + timedelta(minutes=minutes), device, who, "finance", "browser", "prompt", asset, known,
            score, band, sensitive, action, bytes_out)


def test_gap_splits_sessions_like_flink(spark):
    rows = [ev(1, 0), ev(2, 5), ev(3, 14.99),          # 9.99 min apart: same session
            ev(4, 24.99),                              # exactly 10 min later: new session (gap >= 10 min)
            ev(5, 26, device="WX-2")]                  # other device: its own session
    out = {(r.device_id, r.session_start): r for r in sessionize(spark.createDataFrame(rows, SCHEMA), GAP_MS).collect()}
    assert len(out) == 3
    first = out[("WX-1", T0)]
    assert (first.event_count, first.duration_s, first.source) == (3, 899, "backfill")
    assert out[("WX-1", T0 + timedelta(minutes=24.99))].event_count == 1


def test_session_id_and_aggregates_match_the_flink_job(spark):
    rows = [ev(1, 0), ev(2, 1, asset="mcp:unknown-x", known=False, score=7.0, band="High", action="warn"),
            ev(3, 2, asset="agent:cursor", score=5.8, band="Medium", action="block", sensitive=1, bytes_out=1_234_567)]
    r = sessionize(spark.createDataFrame(rows, SCHEMA), GAP_MS).collect()[0]
    # same value as SessionsTest.sessionIdMatchesTheSparkFormula (Java)
    assert r.session_id == "4df2cccb30098f73" == hashlib.sha256(b"WX-1|ann@corp.example|1793005200000").hexdigest()[:16]
    assert r.assets_used == ["agent:cursor", "mcp:unknown-x", "service:chatgpt"] and r.unknown_assets == 1
    assert (r.max_risk_score, r.max_band, r.blocks, r.warns, r.sensitive_events, r.mb_out) == (7.0, "High", 1, 1, 1, 1.23)
