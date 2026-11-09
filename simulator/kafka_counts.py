"""Exact record counts on a Redpanda topic, for the exactly-once crash demo (./tracewell.sh demo-crash).

  python kafka_counts.py offsets <topic>                 -> {"0": 1234, ...}  current end offset per partition
  python kafka_counts.py count <topic> '<offsets json>'  -> records committed since those offsets
  python kafka_counts.py lag <topic> <group>             -> records behind the group's committed offsets

`count` reads with isolation.level=read_committed, so records of aborted transactions (a Flink exactly-once
sink that crashed mid-checkpoint) and transaction markers are not counted: only what consumers can see.
"""
import json
import sys

from confluent_kafka import Consumer, TopicPartition

from tracewell_kafka import BOOTSTRAP


def consumer(group: str = "kafka-counts") -> Consumer:
    return Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False,
                     "isolation.level": "read_committed", "enable.partition.eof": True})


def end_offsets(c: Consumer, topic: str) -> dict[int, int]:
    parts = c.list_topics(topic, timeout=10).topics[topic].partitions
    return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}


def count_since(c: Consumer, topic: str, start: dict[int, int]) -> int:
    end = end_offsets(c, topic)
    todo = {p for p in end if end[p] > start.get(p, 0)}
    c.assign([TopicPartition(topic, p, start.get(p, 0)) for p in todo])
    n = 0
    while todo:
        msg = c.poll(10.0)
        if msg is None:
            raise TimeoutError(f"no progress reading {topic}; partitions left: {sorted(todo)}")
        if msg.error():
            if msg.error().code() == -191:  # _PARTITION_EOF: the committed end of this partition
                todo.discard(msg.partition())
            continue
        n += 1
    return n


def main() -> None:
    mode, topic = sys.argv[1], sys.argv[2]
    c = consumer(sys.argv[3] if mode == "lag" else "kafka-counts")
    if mode == "offsets":
        print(json.dumps({str(p): o for p, o in end_offsets(c, topic).items()}))
    elif mode == "lag":
        # Flink commits its consumer offsets on each checkpoint, so this is the backlog up to one checkpoint ago.
        end = end_offsets(c, topic)
        committed = c.committed([TopicPartition(topic, p) for p in end], timeout=10)
        print(sum(end[tp.partition] - (tp.offset if tp.offset >= 0 else 0) for tp in committed))
    elif mode == "count":
        print(count_since(c, topic, {int(p): o for p, o in json.loads(sys.argv[3]).items()}))
    else:
        sys.exit(__doc__)
    c.close()


if __name__ == "__main__":
    main()
