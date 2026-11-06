"""Create every Redpanda topic the platform uses (idempotent) and register the event schema."""
from tracewell_kafka import ensure_topics, register_schema

if __name__ == "__main__":
    ensure_topics()
    schema_id, _ = register_schema("ai-events-raw-value", "ai_event.avsc")
    print(f"schema ai-events-raw-value registered (id {schema_id})")
