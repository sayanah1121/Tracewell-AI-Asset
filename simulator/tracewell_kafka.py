"""Shared Redpanda helpers: topic creation, Schema Registry, Confluent-framed Avro encode/decode."""
from __future__ import annotations

import io
import json
import os
import struct
import urllib.request

import fastavro
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
SCHEMAS = os.environ.get("SCHEMAS_DIR", "/opt/tracewell/schemas")

# name: (partitions, config)
TOPICS = {
    "ai-events-raw": (6, {"retention.ms": str(7 * 86400000)}),
    "verdicts": (6, {"retention.ms": str(7 * 86400000)}),
    "new-assets": (1, {"retention.ms": str(7 * 86400000)}),
    "ai-events-dlq": (1, {"retention.ms": str(14 * 86400000)}),
    "late-events": (3, {"retention.ms": str(7 * 86400000)}),   # too late for their session window -> Spark backfill
    "policy-updates": (1, {"cleanup.policy": "compact"}),
    "asset-risk-updates": (3, {"cleanup.policy": "compact"}),
}


def ensure_topics(names=None) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(n, num_partitions=p, replication_factor=1, config=cfg)
           for n, (p, cfg) in TOPICS.items() if (names is None or n in names) and n not in existing]
    for name, fut in admin.create_topics(new).items() if new else []:
        fut.result()
        print(f"created topic {name}")


def _http(url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def check_compatibility(subject: str, avsc_file: str) -> bool:
    """Would the registry accept this schema as the next version of the subject (its compatibility level)?"""
    with open(os.path.join(SCHEMAS, avsc_file), encoding="utf-8") as f:
        raw = json.load(f)
    return _http(f"{REGISTRY}/compatibility/subjects/{subject}/versions/latest", {"schema": json.dumps(raw)})["is_compatible"]


def register_schema(subject: str, avsc_file: str):
    with open(os.path.join(SCHEMAS, avsc_file), encoding="utf-8") as f:
        raw = json.load(f)
    schema_id = _http(f"{REGISTRY}/subjects/{subject}/versions", {"schema": json.dumps(raw)})["id"]
    return schema_id, fastavro.parse_schema(raw)


_writer_schemas: dict[int, dict] = {}


def encode(schema_id: int, parsed_schema, record: dict) -> bytes:
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def decode(value: bytes) -> dict:
    magic, schema_id = struct.unpack(">bI", value[:5])
    if magic != 0:
        raise ValueError("not Confluent wire format")
    if schema_id not in _writer_schemas:
        _writer_schemas[schema_id] = fastavro.parse_schema(
            json.loads(_http(f"{REGISTRY}/schemas/ids/{schema_id}")["schema"]))
    return fastavro.schemaless_reader(io.BytesIO(value[5:]), _writer_schemas[schema_id])


def read_compacted(topic: str, decoder=decode) -> dict:
    """Read a compacted topic from the beginning to its current end; return {key: latest value} (tombstones drop keys)."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"snapshot-{os.getpid()}",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    state, done = {}, set()
    while len(done) < len(parts):
        msg = c.poll(5.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == -191:  # _PARTITION_EOF
                done.add(msg.partition())
            continue
        key = msg.key().decode()
        if msg.value() is None:
            state.pop(key, None)
        else:
            state[key] = decoder(msg.value())
    c.close()
    return state
