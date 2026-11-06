"""Publish policies/*.yaml to the compacted topic `policy-updates` (JSON, key = policy_id).

Rules that disappeared from the YAML since the last publish are deleted with a tombstone, so the
topic always mirrors the files. Flink picks changes up within seconds.
"""
import glob
import json
import os

import yaml
from confluent_kafka import Producer

from tracewell_kafka import BOOTSTRAP, ensure_topics, read_compacted

POLICY_DIR = os.environ.get("POLICY_DIR", "/opt/tracewell/policies")


def main() -> None:
    ensure_topics(["policy-updates"])
    rules = {}
    for path in sorted(glob.glob(os.path.join(POLICY_DIR, "*.yaml"))):
        with open(path, encoding="utf-8") as f:
            for r in yaml.safe_load(f) or []:
                r.setdefault("enabled", True)
                rules[r["policy_id"]] = r
    current = read_compacted("policy-updates", decoder=lambda v: json.loads(v))
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for pid, rule in rules.items():
        p.produce("policy-updates", key=pid, value=json.dumps(rule))
    for pid in set(current) - set(rules):
        p.produce("policy-updates", key=pid, value=None)
        print(f"deleted policy {pid}")
    p.flush(10)
    for r in sorted(rules.values(), key=lambda r: r["priority"]):
        print(f"published {r['policy_id']} (priority {r['priority']}): {r['name']} -> {r['action']}")


if __name__ == "__main__":
    main()
