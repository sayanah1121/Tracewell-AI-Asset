"""Publish new or changed risk scores to the compacted Redpanda topic `asset-risk-updates`.

Flink (Phase 2) keeps this topic in broadcast state, so a score change reaches the real-time path
within seconds. Only versions created since the last publish are sent (watermark on valid_from in
ops.pipeline_state). Values use the Confluent wire format (magic byte + schema id + Avro body), so
any Schema Registry-aware consumer can read them.
"""
import argparse
import json
import os
import struct
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro

from common import get_state, http_json, log, set_state, spark_session

TOPIC = "asset-risk-updates"
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "schemas", "asset_risk_update.avsc")
COMPONENT = "publish_risk_updates"


def ensure_compacted_topic(spark, bootstrap: str, partitions: int = 3) -> None:
    """Create the topic with cleanup.policy=compact if it does not exist (Kafka AdminClient via py4j)."""
    jvm = spark._jvm
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("request.timeout.ms", "15000")
    props.put("default.api.timeout.ms", "30000")
    seconds = jvm.java.util.concurrent.TimeUnit.SECONDS
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    try:
        if admin.listTopics().names().get(45, seconds).contains(TOPIC):
            return
        configs = jvm.java.util.HashMap()
        configs.put("cleanup.policy", "compact")
        topic = jvm.org.apache.kafka.clients.admin.NewTopic(
            # Replication factor left to the broker default: py4j turns a java Short into a Python int.
            TOPIC, jvm.java.util.Optional.of(jvm.java.lang.Integer.valueOf(partitions)),
            jvm.java.util.Optional.empty()).configs(configs)
        admin.createTopics(jvm.java.util.Collections.singletonList(topic)).all().get(45, seconds)
        log(f"created compacted topic {TOPIC}")
    finally:
        admin.close()


def register_schema(registry: str, schema: str) -> int:
    resp, _ = http_json(f"{registry}/subjects/{TOPIC}-value/versions", data={"schema": schema},
                        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    return resp["id"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
    ap.add_argument("--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081"))
    ap.add_argument("--full", action="store_true", help="republish every current asset")
    args = ap.parse_args()

    spark = spark_session("publish-risk-updates")
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema = json.dumps(json.load(f))
    ensure_compacted_topic(spark, args.bootstrap)
    schema_id = register_schema(args.registry, schema)

    # Watermark = time of the last change already published (a new version's valid_from or a retirement's valid_to).
    watermark = None if args.full else get_state(spark, COMPONENT, "last_valid_from")
    since = F.lit(watermark).cast("timestamp") if watermark else F.lit("1970-01-01").cast("timestamp")
    dim = spark.table("tracewell.silver.dim_asset_risk")
    current = dim.where("is_current")
    changes = current.where(F.col("valid_from") > since).localCheckpoint()
    # Retired assets (no current row any more) become tombstones so consumers drop them from state.
    retired = (dim.where(~F.col("is_current") & (F.col("valid_to") > since))
               .join(current.select("asset_id"), "asset_id", "left_anti")
               .groupBy("asset_id").agg(F.max("valid_to").alias("changed_at")).localCheckpoint())
    n, n_retired = changes.count(), retired.count()
    if n + n_retired == 0:
        log(f"no changes since {watermark}; nothing to publish")
        spark.stop()
        return

    record = F.struct(
        "asset_id", "asset_name", "category",
        *[F.col(c).cast("double").alias(c) for c in ("risk_score", "capability", "exposure", "governance")],
        "band", "sanctioned", "aliases", "score_reasons", "valid_from")
    header = bytearray(struct.pack(">bI", 0, schema_id))  # Confluent wire format: magic 0 + 4-byte id
    updates = changes.select(F.col("asset_id").cast("binary").alias("key"),
                             F.concat(F.lit(header), to_avro(record, schema)).alias("value"))
    tombstones = retired.select(F.col("asset_id").cast("binary").alias("key"), F.lit(None).cast("binary").alias("value"))
    (updates.unionByName(tombstones)
     .write.format("kafka")
     .option("kafka.bootstrap.servers", args.bootstrap)
     .option("topic", TOPIC)
     .option("kafka.acks", "all")
     .save())

    latest = max(t for t in (changes.agg(F.max("valid_from")).first()[0],
                             retired.agg(F.max("changed_at")).first()[0]) if t is not None)
    set_state(spark, COMPONENT, "last_valid_from", datetime.isoformat(latest))
    log(f"published {n} risk updates + {n_retired} tombstones to {TOPIC} (schema id {schema_id}); watermark -> {latest}")
    spark.stop()


if __name__ == "__main__":
    main()
