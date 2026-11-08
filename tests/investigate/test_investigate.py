from datetime import datetime

from pyspark.sql import functions as F

from investigate import as_of, blast_radius, compare, snapshot_at

FAR = datetime(9999, 12, 31)
V1, V2, V3 = datetime(2026, 10, 25, 15, 0), datetime(2026, 10, 26, 6, 18, 52), datetime(2026, 10, 26, 6, 46, 30)
DIM = "asset_id string, valid_from timestamp, valid_to timestamp, risk_score double, band string, sanctioned boolean, is_current boolean"


def dim(spark):
    # Cursor: Medium/sanctioned, then High/unsanctioned for half an hour, then back (the real history)
    return spark.createDataFrame([("agent:cursor", V1, V2, 5.8, "Medium", True, False),
                                  ("agent:cursor", V2, V3, 7.0, "High", False, False),
                                  ("agent:cursor", V3, FAR, 5.8, "Medium", True, True)], DIM)


def events(spark, *times):
    return spark.createDataFrame([(f"e{i}", "agent:cursor", t) for i, t in enumerate(times)],
                                 "event_id string, asset_id string, ts_event timestamp")


def test_as_of_picks_the_version_valid_at_the_event(spark):
    evs = events(spark, datetime(2026, 10, 26, 6, 0), datetime(2026, 10, 26, 6, 30), V3, datetime(2026, 10, 26, 9, 0))
    got = [(r.ts_event, r.band_asof) for r in as_of(evs, dim(spark)).orderBy("ts_event").collect()]
    assert [b for _, b in got] == ["Medium", "High", "Medium", "Medium"]       # valid_to is exclusive


def test_snapshot_at_takes_the_last_commit_before_the_event(spark):
    snaps = spark.createDataFrame([(1, datetime(2026, 10, 25, 15, 1)), (2, datetime(2026, 10, 26, 6, 20, 16)),
                                   (3, datetime(2026, 10, 26, 6, 47, 59))], "snapshot_id long, committed_at timestamp")
    evs = events(spark, datetime(2026, 10, 25, 14, 0), datetime(2026, 10, 26, 6, 19), datetime(2026, 10, 26, 6, 30))
    got = {r.event_id: r.tt_snapshot_id for r in snapshot_at(evs, snaps).collect()}
    assert got == {"e0": None, "e1": 1, "e2": 2}     # before the table existed: no snapshot


def test_compare_explains_mismatches_inside_the_commit_lag(spark):
    rows = [  # event, asof version, as-of view, time-travel view
        ("e1", datetime(2026, 10, 26, 6, 19), V2, 7.0, "High", False, 5.8, "Medium", True),     # in the 84 s lag
        ("e2", datetime(2026, 10, 26, 6, 30), V2, 7.0, "High", False, 7.0, "High", False),      # agree
        ("e3", datetime(2026, 10, 26, 9, 0), V3, 5.8, "Medium", True, 7.0, "High", False),      # real disagreement
    ]
    t = spark.createDataFrame([("agent:cursor",) + r for r in rows],
                              "asset_id string, event_id string, ts_event timestamp, asof_valid_from timestamp, "
                              "score_asof double, band_asof string, sanctioned_asof boolean, "
                              "score_tt double, band_tt string, sanctioned_tt boolean")
    commits = spark.createDataFrame([("agent:cursor", V2, datetime(2026, 10, 26, 6, 20, 16)),
                                     ("agent:cursor", V3, datetime(2026, 10, 26, 6, 47, 59))],
                                    "asset_id string, asof_valid_from timestamp, version_committed_at timestamp")
    got = {r.event_id: (r.tt_agrees, r.mismatch_reason) for r in compare(t, commits).collect()}
    assert got["e1"] == (False, "event between valid_from and commit of this version (84 s lag)")
    assert got["e2"] == (True, None)
    assert got["e3"] == (False, "unexplained")


def test_blast_radius_counts_everyone_else_exposed(spark):
    usage = spark.createDataFrame([
        ("agent:cursor", "WX-1", "ann@corp.example", "engineering", datetime(2026, 10, 26, 6, 0), 0, "allow"),
        ("agent:cursor", "WX-2", "bob@corp.example", "engineering", datetime(2026, 10, 26, 7, 0), 1, "block"),
        ("agent:cursor", "WX-3", "cat@corp.example", "finance", datetime(2026, 10, 26, 8, 0), 0, "allow"),
        ("service:chatgpt", "WX-4", "dan@corp.example", "sales", datetime(2026, 10, 26, 8, 0), 0, "allow"),
    ], "asset_id string, device_id string, identity string, department string, ts_event timestamp, "
       "sensitive_count int, action string")
    assets = spark.createDataFrame([("agent:cursor", "policy hit")], "asset_id string, reason string")
    r = blast_radius(usage, assets, dim(spark).where("is_current").withColumn("asset_name", F.lit("Cursor"))).collect()
    assert len(r) == 1
    assert (r[0].devices, r[0].people, r[0].departments, r[0].sensitive_events, r[0].blocks, r[0].band_today) == \
        (3, 3, ["engineering", "finance"], 1, 1, "Medium")
