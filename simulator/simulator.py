"""Fleet simulator: AI activity from a fake company of laptops, sent to Redpanda `ai-events-raw` (Avro).

The assets each laptop "has installed" come from the live risk catalog (topic asset-risk-updates), so
every event refers to real models, MCP servers, packages and AI services. Realistic mess is added on
purpose: offline devices that flush hours-old events later, duplicate sends, clock skew, assets the
catalog has never seen, and prompts that contain fake personal data, test card numbers or API keys.

  python simulator.py --rate 50 --duration 600 --scenarios rogue_mcp,finance_upload,secret_paste,poisoned_package
  python simulator.py --duration 60 --scenarios offline_burst      # a laptop flushing 12 h of activity at once
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import string
import threading
import time
import uuid
from collections import Counter, deque

from confluent_kafka import Consumer, Producer
from faker import Faker

from tracewell_kafka import BOOTSTRAP, encode, ensure_topics, read_compacted, register_schema

fake = Faker()
DEPARTMENTS = {  # department: (share of people, preferred categories)
    "engineering": (0.35, ["agent", "mcp_server", "package", "model", "ai_service"]),
    "finance": (0.12, ["ai_service", "extension"]),
    "sales": (0.18, ["ai_service", "extension"]),
    "hr": (0.08, ["ai_service", "extension"]),
    "support": (0.17, ["ai_service", "extension", "mcp_server"]),
    "legal": (0.10, ["ai_service", "extension"]),
}
TOOLS = ["read_file", "write_file", "query", "search", "fetch", "list_issues", "send_message", "execute", "run_command"]
FILES = ["finance_q3.xlsx", "customers.csv", "roadmap.pptx", "meeting_notes.docx", "contract_draft.pdf",
         "salaries.xlsx", "architecture.png", "incident_report.md"]
HOSTS = ["api.openai.com", "api.anthropic.com", "generativelanguage.googleapis.com", "api.deepseek.com",
         "api.perplexity.ai", "api.mistral.ai", "api.groq.com", "api.cursor.sh"]
PROMPTS = [
    "Summarise this meeting transcript into five bullet points",
    "Write a polite follow-up email to the client about the delayed invoice",
    "Explain this stack trace and suggest a fix",
    "Refactor this function to be easier to test",
    "Draft a job description for a senior data engineer",
    "Translate this paragraph into German",
    "What are the key risks in this contract clause?",
    "Generate SQL to find customers who churned last quarter",
    "Create a project plan for migrating our ETL to Airflow",
    "Rewrite this paragraph in a more formal tone",
]
TEST_CARDS = ["4111 1111 1111 1111", "5555-5555-5555-4444", "378282246310005", "4012888888881881"]


def sensitive_snippet(kind: str) -> str:
    if kind == "email":
        return f"Contact {fake.name()} at {fake.user_name()}@{fake.free_email_domain()}"
    if kind == "phone":
        return f"call me on +91 {random.randint(70000, 99999)} {random.randint(10000, 99999)}"
    if kind == "card":
        return f"the corporate card is {random.choice(TEST_CARDS)} exp 09/29"
    if kind == "aws_key":
        return "use key AKIA" + "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
    if kind == "openai_key":
        return "my key is sk-" + "".join(random.choices(string.ascii_letters + string.digits, k=40))
    if kind == "pan":
        return "PAN " + "".join(random.choices(string.ascii_uppercase, k=5)) + f"{random.randint(1000, 9999)}" + random.choice(string.ascii_uppercase)
    if kind == "aadhaar":
        return f"Aadhaar {random.randint(2, 9)}{random.randint(100, 999)} {random.randint(1000, 9999)} {random.randint(1000, 9999)}"
    raise ValueError(kind)


# ───────────────────────────── fleet from the catalog ─────────────────────────────

def load_catalog() -> dict[str, list[dict]]:
    snap = read_compacted("asset-risk-updates")
    by_cat: dict[str, list[dict]] = {}
    for a in snap.values():
        by_cat.setdefault(a.get("category") or "unknown", []).append(a)
    print("catalog snapshot: " + ", ".join(f"{k}={len(v)}" for k, v in sorted(by_cat.items())), flush=True)
    return by_cat


def pick_assets(by_cat: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """A realistic company-sized subset: every curated service/agent/extension, some MCP servers (incl. risky ones)."""
    rng = random.Random(42)
    mcp = by_cat.get("mcp_server", [])
    risky = [a for a in mcp if a.get("band") in ("High", "Critical")]
    pool = {
        "ai_service": by_cat.get("ai_service", []),
        "agent": by_cat.get("agent", []),
        "extension": by_cat.get("extension", []),
        "package": rng.sample(by_cat.get("package", []), min(300, len(by_cat.get("package", [])))),
        "model": rng.sample(by_cat.get("model", []), min(40, len(by_cat.get("model", [])))),
        "mcp_server": rng.sample(mcp, min(90, len(mcp))) + rng.sample(risky, min(30, len(risky))),
    }
    return pool


class Fleet:
    def __init__(self, n_devices: int, n_people: int, pool: dict[str, list[dict]], seed: int = 7):
        self.rng = random.Random(seed)
        self.pool = pool
        # A seeded Faker of its own, so the same company (same names, same people) comes back on every run.
        names = Faker()
        names.seed_instance(seed)
        self.people, taken = [], set()
        for i in range(n_people):
            dept = self.rng.choices(list(DEPARTMENTS), [v[0] for v in DEPARTMENTS.values()])[0]
            first, last = names.first_name().lower(), names.last_name().lower()
            identity = f"{first}.{last}@corp.example"
            if identity in taken:                        # two people with the same name get distinct addresses
                identity = f"{first}.{last}{i}@corp.example"
            taken.add(identity)
            self.people.append({"identity": identity, "department": dept})
        self.devices = []
        for i in range(n_devices):
            person = self.people[i % n_people]
            prefs = DEPARTMENTS[person["department"]][1]
            installed = []
            for _ in range(self.rng.randint(3, 8)):
                cat = self.rng.choice(prefs)
                if pool.get(cat):
                    installed.append(self.rng.choice(pool[cat]))
            self.devices.append({
                "device_id": f"WX-{1000 + i}", "os": self.rng.choices(["windows", "macos", "linux"], [55, 35, 10])[0],
                **person, "installed": installed, "seen": set(),
                "skew_ms": self.rng.choice([0] * 32 + [self.rng.randint(-120_000, 120_000)]),
                "offline_until": 0.0, "buffer": [],
                # Working rhythm (a compressed day): active 4-15 min, then idle 12-40 min, at a random phase.
                # Idle periods longer than the 10-min session gap are what end a session.
                "active_s": self.rng.uniform(240, 900), "idle_s": self.rng.uniform(720, 2400),
                "phase_s": self.rng.uniform(0, 3300),
            })

    def ref_for(self, asset: dict) -> str:
        """Sensors see raw references (package names, repo slugs, ids), not always the canonical id."""
        aliases = [a for a in (asset.get("aliases") or []) if a]
        if aliases and self.rng.random() < 0.4:
            return self.rng.choice(aliases)
        return asset["asset_id"]


SCHEMA_V2 = False  # --schema-v2: send events with schemas/ai_event_v2.avsc (adds agent_parent_id)


def make_event(dev: dict, asset: dict | None, ref: str, event_type: str, sensor: str, now_ms: int, **extra) -> dict:
    ev = {
        "event_id": str(uuid.uuid4()), "ts_event": now_ms + dev["skew_ms"], "ts_sent": now_ms,
        "device_id": dev["device_id"], "os": dev["os"], "identity": dev["identity"], "department": dev["department"],
        "sensor": sensor, "event_type": event_type, "asset_ref": ref,
        "asset_version": None, "tool_name": None, "prompt_text": None, "file_name": None, "file_size_bytes": None,
        "dest_host": None, "bytes_out": None, "schema_version": 1,
    }
    ev.update(extra)
    if SCHEMA_V2:
        ev["schema_version"] = 2
        # tool calls and on-device prompts come from an agent run on that laptop
        agentic = event_type == "mcp_tool_call" or (sensor == "device" and event_type == "prompt")
        ev["agent_parent_id"] = f"run-{dev['device_id']}-{now_ms // 600_000}" if agentic else None
    return ev


def prompt_text(rng: random.Random, sensitive_rate: float) -> str:
    text = rng.choice(PROMPTS)
    if rng.random() < sensitive_rate:
        kind = rng.choice(["email", "phone", "card", "aws_key", "openai_key", "pan", "aadhaar"])
        text += ". " + sensitive_snippet(kind)
    return text


def events_for(fleet: Fleet, dev: dict, asset: dict, now_ms: int, sensitive_rate: float) -> list[dict]:
    rng, cat = fleet.rng, asset.get("category")
    ref = fleet.ref_for(asset)
    out = []
    if asset["asset_id"] not in dev["seen"]:
        dev["seen"].add(asset["asset_id"])
        out.append(make_event(dev, asset, ref, "asset_discovered", "device" if cat not in ("ai_service", "extension") else "browser", now_ms))
    if cat in ("ai_service", "extension"):
        r = rng.random()
        if r < 0.6:
            out.append(make_event(dev, asset, ref, "prompt", "browser", now_ms, prompt_text=prompt_text(rng, sensitive_rate)))
        elif r < 0.75:
            f = rng.choice(FILES)
            out.append(make_event(dev, asset, ref, "file_upload", "browser", now_ms, file_name=f, file_size_bytes=rng.randint(10_000, 4_000_000)))
        elif r < 0.9:
            out.append(make_event(dev, asset, ref, "paste", "browser", now_ms, prompt_text=prompt_text(rng, sensitive_rate * 2)))
        else:
            out.append(make_event(dev, asset, ref, "egress", "network", now_ms, dest_host=rng.choice(HOSTS), bytes_out=rng.randint(2_000, 900_000)))
    elif cat == "agent":
        r = rng.random()
        if r < 0.45:
            out.append(make_event(dev, asset, ref, "prompt", "device", now_ms, prompt_text=prompt_text(rng, sensitive_rate)))
        elif r < 0.75:
            out.append(make_event(dev, asset, ref, "mcp_tool_call", "device", now_ms, tool_name=rng.choice(TOOLS)))
        else:
            out.append(make_event(dev, asset, ref, "egress", "network", now_ms, dest_host=rng.choice(HOSTS), bytes_out=rng.randint(2_000, 400_000)))
    elif cat == "mcp_server":
        out.append(make_event(dev, asset, ref, "mcp_tool_call", "device", now_ms, tool_name=rng.choice(TOOLS)))
    elif cat == "model":
        out.append(make_event(dev, asset, ref, "model_inference", "device", now_ms))
    elif cat == "package":
        out.append(make_event(dev, asset, ref, "asset_discovered", "device", now_ms))
    return out


# ───────────────────────────── scenarios ─────────────────────────────

def is_working(dev: dict, t: float) -> bool:
    return (t + dev["phase_s"]) % (dev["active_s"] + dev["idle_s"]) < dev["active_s"]


OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")


def offline_burst(fleet: Fleet, now_ms: int) -> list[dict]:
    """A laptop that was offline for 12 h (a long flight) comes back and sends everything at once.

    Four work sessions with known boundaries, events 15-120 s apart inside a session and 40+ min of silence
    between sessions. Every event is far behind Flink's watermark, so all of it reaches the late-events
    side output and the session table only gets these sessions through the Spark backfill. The expected
    sessions are written to simulator/out/offline_burst_expected.json for checking.
    """
    person = next(p for p in fleet.people if p["department"] == "engineering")
    dev = {"device_id": f"WX-T{now_ms % 100000:05d}", "os": "macos", **person, "skew_ms": 0, "seen": set()}
    assets = [a for cat in ("agent", "ai_service", "mcp_server") for a in fleet.pool.get(cat, [])[:3]]
    rng = random.Random(now_ms)
    windows_h = [(12.0, 11.2), (9.0, 8.3), (5.0, 3.8), (1.5, 0.7)]       # (start, end) hours ago
    events, expected = [], []
    for start_h, end_h in windows_h:
        t = now_ms - int(start_h * 3600_000)
        stop = now_ms - int(end_h * 3600_000)
        first, last, n = t, t, 0
        while t <= stop:
            for ev in events_for(fleet, dev, rng.choice(assets), t, 0.04):
                ev["ts_sent"] = now_ms
                events.append(ev)
                n += 1
                last = ev["ts_event"]
            t += rng.randint(15_000, 120_000)
        expected.append({"session_start_ms": first, "session_end_ms": last, "event_count": n})
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "offline_burst_expected.json"), "w", encoding="utf-8") as f:
        json.dump({"device_id": dev["device_id"], "identity": dev["identity"], "sent_at_ms": now_ms,
                   "sessions": expected}, f, indent=2)
    print(f"  offline_burst: {dev['device_id']} ({dev['identity']}) flushes {len(events)} events from the last 12 h "
          f"in {len(expected)} sessions -> simulator/out/offline_burst_expected.json", flush=True)
    return events


def scenario_events(name: str, fleet: Fleet, now_ms: int) -> list[dict]:
    eng = [d for d in fleet.devices if d["department"] == "engineering"]
    fin = [d for d in fleet.devices if d["department"] == "finance"] or fleet.devices
    dev = fleet.rng.choice(eng or fleet.devices)
    if name == "rogue_mcp":
        ref = "mcp:unknown-" + uuid.uuid4().hex[:6]
        return [make_event(dev, None, ref, "asset_discovered", "device", now_ms),
                make_event(dev, None, ref, "mcp_tool_call", "device", now_ms + 2000, tool_name="read_file", file_name="customers.csv"),
                make_event(dev, None, ref, "egress", "network", now_ms + 4000, dest_host="203.0.113.42", bytes_out=48_000_000)]
    if name == "finance_upload":
        d = fleet.rng.choice(fin)
        return [make_event(d, None, "service:deepseek-chat", "file_upload", "browser", now_ms, file_name="finance_q3.xlsx", file_size_bytes=1_800_000)]
    if name == "secret_paste":
        return [make_event(dev, None, "agent:claude-code", "paste", "device", now_ms, prompt_text="deploy failing, here is my config " + sensitive_snippet("aws_key"))]
    if name == "poisoned_package":
        return [make_event(d, None, "pypi:chromadb", "asset_discovered", "device", now_ms + i * 500, asset_version="latest")
                for i, d in enumerate(fleet.rng.sample(eng or fleet.devices, 3))]
    if name == "offline_burst":
        return offline_burst(fleet, now_ms)
    raise ValueError(name)


# ───────────────────────────── verdict watcher ─────────────────────────────

def watch_verdicts(stats: Counter, stop: threading.Event) -> None:
    # No offset commits: every run starts at the latest verdict instead of re-counting what earlier runs left behind.
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "simulator-verdicts-live", "auto.offset.reset": "latest",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    c.subscribe(["verdicts"])
    while not stop.is_set():
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        v = json.loads(msg.value())
        stats[v["action"]] += 1
        if v["action"] == "block":
            print(f"  BLOCK {v['device_id']} {v['identity']} -> {v['asset_id']} ({v.get('policy_id')}) "
                  f"band={v.get('band')} sensitive={v.get('sensitive_types')}", flush=True)
    c.close()


# ───────────────────────────── company directory ─────────────────────────────

def export_directory(fleet: Fleet, out_dir: str) -> None:
    """The HR directory and device inventory a real company would export nightly (gold uses it for licence ROI).

    People and device owners depend only on the seed, not on the catalog, so this matches every simulator run.
    """
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "people.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["identity", "department"])
        w.writerows((p["identity"], p["department"]) for p in fleet.people)
    with open(os.path.join(out_dir, "devices.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["device_id", "identity", "department"])
        w.writerows((d["device_id"], d["identity"], d["department"]) for d in fleet.devices)
    print(f"wrote {len(fleet.people)} people and {len(fleet.devices)} devices to {out_dir}", flush=True)


# ───────────────────────────── main loop ─────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=50, help="events per second")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = run until stopped")
    ap.add_argument("--devices", type=int, default=500)
    ap.add_argument("--people", type=int, default=300)
    ap.add_argument("--sensitive-rate", type=float, default=0.04)
    ap.add_argument("--duplicate-rate", type=float, default=0.01)
    ap.add_argument("--offline-rate", type=float, default=0.002, help="chance per tick that a device goes offline")
    ap.add_argument("--unknown-rate", type=float, default=0.02)
    ap.add_argument("--scenarios", default="",
                    help="comma list: rogue_mcp,finance_upload,secret_paste,poisoned_package,offline_burst")
    ap.add_argument("--work-pattern", action=argparse.BooleanOptionalAction, default=True,
                    help="devices alternate working and idle periods (so sessions end); --no-work-pattern = always busy")
    ap.add_argument("--scenario-after", type=float, default=30, help="seconds before scenarios start")
    ap.add_argument("--schema-v2", action="store_true",
                    help="send events with schemas/ai_event_v2.avsc (adds agent_parent_id), for the schema-evolution demo")
    ap.add_argument("--export-directory", metavar="DIR",
                    help="write the company directory (people.csv, devices.csv) to DIR and exit")
    args = ap.parse_args()

    if args.export_directory:
        export_directory(Fleet(args.devices, args.people, {}), args.export_directory)
        return

    ensure_topics(["ai-events-raw", "verdicts"])
    global SCHEMA_V2
    SCHEMA_V2 = args.schema_v2
    schema_id, schema = register_schema("ai-events-raw-value", "ai_event_v2.avsc" if SCHEMA_V2 else "ai_event.avsc")
    print(f"producing with schema id {schema_id} ({'v2' if SCHEMA_V2 else 'v1'})", flush=True)
    fleet = Fleet(args.devices, args.people, pick_assets(load_catalog()))
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "compression.type": "zstd"})
    stats, verdicts, stop = Counter(), Counter(), threading.Event()
    threading.Thread(target=watch_verdicts, args=(verdicts, stop), daemon=True).start()
    scenarios = deque(s for s in args.scenarios.split(",") if s)

    def send(ev: dict) -> None:
        producer.produce("ai-events-raw", key=ev["device_id"], value=encode(schema_id, schema, ev))
        stats["sent"] += 1
        stats[ev["event_type"]] += 1

    start = last_report = time.time()
    tick = 0.1
    print(f"simulating {args.devices} devices / {args.people} people at {args.rate}/s", flush=True)
    while not args.duration or time.time() - start < args.duration:
        t0 = time.time()
        now_ms = int(t0 * 1000)
        n = random.random() < (args.rate * tick) % 1
        for _ in range(int(args.rate * tick) + n):
            dev = fleet.rng.choice(fleet.devices)
            for _ in range(20):                               # prefer someone who is working right now
                if not args.work_pattern or is_working(dev, t0):
                    break
                dev = fleet.rng.choice(fleet.devices)
            if dev["offline_until"] and t0 >= dev["offline_until"]:
                for ev in dev["buffer"]:                      # back online: flush buffered events
                    ev["ts_sent"] = now_ms
                    send(ev)
                stats["flushed_late"] += len(dev["buffer"])
                dev["buffer"], dev["offline_until"] = [], 0.0
            if not dev["installed"]:
                continue
            if fleet.rng.random() < args.unknown_rate:
                ref = "mcp:unknown-" + fleet.rng.choice(["a1", "b2", "c3", "d4", "e5"]) + "-tool"
                evs = [make_event(dev, None, ref, "mcp_tool_call", "device", now_ms, tool_name=fleet.rng.choice(TOOLS))]
            else:
                evs = events_for(fleet, dev, fleet.rng.choice(dev["installed"]), now_ms, args.sensitive_rate)
            if not dev["offline_until"] and fleet.rng.random() < args.offline_rate:
                dev["offline_until"] = t0 + fleet.rng.uniform(60, 600)   # offline 1-10 min (scaled-down hours)
                stats["went_offline"] += 1
            for ev in evs:
                if dev["offline_until"]:
                    dev["buffer"].append(ev)
                    continue
                send(ev)
                if fleet.rng.random() < args.duplicate_rate:      # retry after a lost ack: same event_id
                    send(ev)
                    stats["duplicates"] += 1
        if scenarios and time.time() - start > args.scenario_after:
            name = scenarios.popleft()
            for ev in scenario_events(name, fleet, now_ms):
                send(ev)
            print(f"▶ scenario {name} injected", flush=True)
            args.scenario_after += 15
        producer.poll(0)
        if time.time() - last_report > 10:
            last_report = time.time()
            buffered = sum(len(d["buffer"]) for d in fleet.devices)
            print(f"[{int(time.time() - start):>4}s] sent={stats['sent']} dup={stats['duplicates']} "
                  f"offline_buffered={buffered} flushed_late={stats['flushed_late']} | verdicts: "
                  f"allow={verdicts['allow']} warn={verdicts['warn']} block={verdicts['block']}", flush=True)
        time.sleep(max(0.0, tick - (time.time() - t0)))
    for dev in fleet.devices:                                   # end of run: everyone comes back online
        for ev in dev["buffer"]:
            ev["ts_sent"] = int(time.time() * 1000)
            send(ev)
            stats["flushed_late"] += 1
    producer.flush(30)
    time.sleep(3)
    stop.set()
    print(f"done: {dict(stats)} | verdicts seen: {dict(verdicts)}", flush=True)


if __name__ == "__main__":
    main()
