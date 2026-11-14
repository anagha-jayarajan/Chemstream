import json, sys
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

topic, n_bad, n_dup = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
seen, base = set(), []
for line in open("data/pubchem_dump.jsonl"):
    if line.strip():
        m = json.loads(line)
        if m["record_id"] not in seen:
            seen.add(m["record_id"]); base.append(m)

p = Producer({"bootstrap.servers": KAFKA})
for i in range(n_bad):
    p.produce(topic, value='{"broken json ' + str(i))
for m in base[:n_dup]:
    m = dict(m)
    m["record_id"] = m["record_id"] + "-r0"          # same id as an already-sent message
    m["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    p.produce(topic, key=m["record_id"], value=json.dumps(m))
p.flush(30)
print("injected", n_bad, "corrupt and", n_dup, "duplicate messages")