import json, os, sys, time
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

DUMP = "data/pubchem_dump.jsonl"

def load_base():
    seen = {}
    for line in open(DUMP):
        if line.strip():
            m = json.loads(line)
            seen[m["record_id"]] = m
    return list(seen.values())

def main(topic, rate, total):
    base = load_base()
    os.makedirs("ledger", exist_ok=True)
    led = open(f"ledger/{topic}_sent.csv", "a", buffering=1)
    stats = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is None:
            stats["ok"] += 1
            led.write(msg.key().decode() + "\n")
        else:
            stats["failed"] += 1

    p = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True,
                  "linger.ms": 5, "queue.buffering.max.messages": 500000})
    t0, sent = time.time(), 0
    while sent < total:
        target = min(total, int((time.time() - t0) * rate) + 1)
        while sent < target:
            m = dict(base[sent % len(base)])
            m["record_id"] = f'{m["record_id"]}-r{sent // len(base)}'
            m["ingest_ts"] = datetime.now(timezone.utc).isoformat()
            try:
                p.produce(topic, key=m["record_id"], value=json.dumps(m), on_delivery=on_delivery)
            except BufferError:
                p.poll(0.5)
                continue
            sent += 1
            p.poll(0)
        time.sleep(0.002)
    stats["failed"] += p.flush(120)
    t1 = time.time()
    summary = {"topic": topic, "rate_target": rate, "total": total, "sent_ok": stats["ok"],
               "failed": stats["failed"], "start": t0, "end": t1,
               "achieved_rate": stats["ok"] / (t1 - t0)}
    json.dump(summary, open(f"ledger/{topic}_summary.json", "w"))
    print(summary)

if __name__ == "__main__":
    main(sys.argv[1], float(sys.argv[2]), int(sys.argv[3]))