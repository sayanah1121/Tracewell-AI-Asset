"""Verify the tamper-evident audit log (silver.audit_log), which Flink writes as SHA-256 hash chains.

For every chain, ordered by seq_no, each row must satisfy three rules (checked in one pass with window
functions, no row-by-row loop):

  hash   record_hash = sha256(prev_hash || canonical record)     an edited field breaks this
  link   prev_hash   = record_hash of the row before (64 zeros for seq_no 1)
  seq    seq_no      = previous seq_no + 1, starting at 1        a deleted or inserted row breaks link + seq

A chain alone cannot show that its newest rows were cut off, so each successful run stores the head of
every chain in ops.audit_anchors; the next run checks that the last anchored head is still there with the
same hash. Results go to ops.dq_results (suite 'audit'); any broken row fails the job (a critical alert).
"""
import argparse
import sys

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from common import log, spark_session

AUDIT = "tracewell.silver.audit_log"
ANCHORS = "tracewell.ops.audit_anchors"
GENESIS = "0" * 64


def canonical(df: DataFrame) -> Column:
    """The same '|'-separated string as AuditChain.canonical() in the Flink job; NULL becomes ''."""
    def s(c):
        return F.coalesce(F.col(c).cast("string"), F.lit(""))
    return F.concat_ws("|", s("seq_no"), s("chain_id"), s("event_id"), F.unix_millis("ts_event").cast("string"),
                       s("identity"), s("device_id"), s("asset_id"), s("action"), s("policy_id"), s("band_at_event"),
                       F.array_join(F.coalesce("sensitive_types", F.array().cast("array<string>")), ","))


def broken_links(audit: DataFrame) -> DataFrame:
    """Rows that break the chain, with the rule(s) they break. Empty when the log is intact."""
    w = Window.partitionBy("chain_id").orderBy("seq_no")
    prev_hash = F.lag("record_hash").over(w)
    prev_seq = F.lag("seq_no").over(w)
    checked = audit.select(
        "chain_id", "seq_no", "event_id", "action", "record_hash",
        (F.sha2(F.concat(F.col("prev_hash"), canonical(audit)), 256) == F.col("record_hash")).alias("hash_ok"),
        F.when(prev_hash.isNull(), (F.col("prev_hash") == GENESIS) & (F.col("seq_no") == 1))
         .otherwise((F.col("prev_hash") == prev_hash) & (F.col("seq_no") == prev_seq + 1)).alias("link_ok"))
    reasons = F.array_compact(F.array(F.when(~F.col("hash_ok"), F.lit("record_hash does not match its content")),
                                      F.when(~F.col("link_ok"), F.lit("prev_hash or seq_no does not follow the previous row"))))
    return checked.where(~F.col("hash_ok") | ~F.col("link_ok")).select(
        "chain_id", "seq_no", "event_id", "action", F.array_join(reasons, "; ").alias("problem"))


def missing_anchors(audit: DataFrame, anchors: DataFrame) -> DataFrame:
    """The last anchored head of each chain must still be in the log with the same hash (catches truncation)."""
    last = anchors.withColumn("_rn", F.row_number().over(Window.partitionBy("chain_id").orderBy(F.desc("seq_no")))) \
        .where("_rn = 1").select("chain_id", "seq_no", F.col("record_hash").alias("anchored_hash"))
    return (last.join(audit.select("chain_id", "seq_no", "record_hash"), ["chain_id", "seq_no"], "left")
            .where(F.col("record_hash").isNull() | (F.col("record_hash") != F.col("anchored_hash")))
            .select("chain_id", "seq_no", F.lit(None).cast("string").alias("event_id"), F.lit(None).cast("string").alias("action"),
                    F.when(F.col("record_hash").isNull(), F.lit("anchored head row is missing (log truncated)"))
                     .otherwise(F.lit("anchored head row was changed")).alias("problem")))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", type=int, default=10, help="broken rows to print")
    args = ap.parse_args()
    spark = spark_session("verify-audit-chain")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {ANCHORS} (
                    verified_at TIMESTAMP, chain_id INT, seq_no BIGINT, record_hash STRING) USING iceberg""")

    audit = spark.table(AUDIT).localCheckpoint()
    heads = (audit.groupBy("chain_id").agg(F.count("*").alias("rows"), F.max("seq_no").alias("seq_no"))
             .join(audit.select("chain_id", "seq_no", "record_hash"), ["chain_id", "seq_no"]).orderBy("chain_id").collect())
    bad = broken_links(audit).unionByName(missing_anchors(audit, spark.table(ANCHORS))).orderBy("chain_id", "seq_no").cache()
    n_bad = bad.count()
    total = sum(h.rows for h in heads)
    for h in heads:
        log(f"chain {h.chain_id}: {h.rows} rows, head seq_no {h.seq_no} {h.record_hash[:12]}…")

    detail = f"{total} rows in {len(heads)} chains, {n_bad} broken"
    spark.createDataFrame([("audit_chain_intact", n_bad == 0, detail)], "check_name string, passed boolean, detail string") \
        .select(F.current_timestamp().alias("run_at"), F.lit("audit").alias("suite"), "*") \
        .writeTo("tracewell.ops.dq_results").append()

    if n_bad:
        log(f"FAIL  audit chain broken: {detail}")
        for r in bad.limit(args.show).collect():
            log(f"  chain {r.chain_id} seq_no {r.seq_no} event {r.event_id} action={r.action}: {r.problem}")
        spark.stop()
        sys.exit(f"audit chain broken at {n_bad} row(s)")

    spark.createDataFrame([(h.chain_id, h.seq_no, h.record_hash) for h in heads],
                          "chain_id int, seq_no bigint, record_hash string") \
        .select(F.current_timestamp().alias("verified_at"), "*").writeTo(ANCHORS).append()
    log(f"PASS  audit chain intact: {detail}; heads anchored")
    spark.stop()


if __name__ == "__main__":
    main()
