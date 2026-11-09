import hashlib
from datetime import datetime, timedelta, timezone

from verify_audit_chain import GENESIS, broken_links, missing_anchors

SCHEMA = ("chain_id int, seq_no bigint, event_id string, ts_event timestamp, identity string, device_id string, "
          "asset_id string, action string, policy_id string, band_at_event string, sensitive_types array<string>, "
          "prev_hash string, record_hash string")
T0 = datetime(2026, 10, 26, 9, 0, tzinfo=timezone.utc)   # 1793005200000 ms


def chain(n, chain_id=2):
    """n links built exactly like the Flink job's AuditChain."""
    rows, prev = [], GENESIS
    for i in range(1, n + 1):
        ts = T0 + timedelta(seconds=i - 1)
        fields = [str(i), str(chain_id), f"e{i}", str(int(ts.timestamp() * 1000)), "ann@corp.example", "WX-1",
                  "service:grok", "block", "P001", "Low", "PCI,PII"]
        h = hashlib.sha256((prev + "|".join(fields)).encode()).hexdigest()
        rows.append([chain_id, i, f"e{i}", ts, "ann@corp.example", "WX-1", "service:grok", "block", "P001", "Low",
                     ["PCI", "PII"], prev, h])
        prev = h
    return rows


def problems(spark, rows):
    return sorted((r.seq_no, r.problem) for r in broken_links(spark.createDataFrame(rows, SCHEMA)).collect())


def test_same_vector_as_the_flink_job(spark):
    # AuditChainTest.canonicalRecordAndHashMatchTheSparkVerifier (Java) asserts this hash too
    assert chain(1)[0][-1] == "06a14ad7f39277269df4fcd726f6d4b8462d0bb4891e4ee6b1490200e7c6ed64"
    assert problems(spark, chain(5)) == []


def test_edited_row_is_detected(spark):
    rows = chain(5)
    rows[2][7] = "allow"                                  # someone turns a block into an allow
    assert problems(spark, rows) == [(3, "record_hash does not match its content")]


def test_edit_with_recomputed_hash_breaks_the_next_link(spark):
    rows = chain(5)
    rows[2][7] = "allow"
    fields = ["3", "2", "e3", str(int(rows[2][3].timestamp() * 1000)), "ann@corp.example", "WX-1", "service:grok",
              "allow", "P001", "Low", "PCI,PII"]
    rows[2][-1] = hashlib.sha256((rows[2][-2] + "|".join(fields)).encode()).hexdigest()
    assert problems(spark, rows) == [(4, "prev_hash or seq_no does not follow the previous row")]


def test_deleted_row_is_detected(spark):
    rows = chain(5)
    del rows[1]
    assert [s for s, _ in problems(spark, rows)] == [3]


def test_truncated_tail_is_caught_by_the_anchor(spark):
    rows = chain(5)
    anchors = spark.createDataFrame([(2, 5, rows[4][-1])], "chain_id int, seq_no bigint, record_hash string")
    intact = missing_anchors(spark.createDataFrame(rows, SCHEMA), anchors)
    truncated = missing_anchors(spark.createDataFrame(rows[:3], SCHEMA), anchors)
    assert intact.count() == 0
    assert [r.problem for r in truncated.collect()] == ["anchored head row is missing (log truncated)"]
    assert problems(spark, rows[:3]) == []                # the chain itself still looks fine without the anchor
