import json, os, sys, time, requests
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

API = os.getenv("MP_BASE", "https://api.materialsproject.org") + "/materials/summary/"
KEY = os.getenv("MP_API_KEY")
TOPIC, PAGE, PAUSE = "chem.mp.raw", 50, 1.5          # about 40 requests per minute
FIELDS = "material_id,formula_pretty,band_gap,formation_energy_per_atom,nsites,structure"
STATE, LEDGER, DUMP = "ledger/mp_watermark.txt", "ledger/mp_sent.csv", "data/mp_dump.jsonl"

def fetch(skip):
    for attempt in range(5):
        r = requests.get(API, headers={"X-API-KEY": KEY}, timeout=60,
                         params={"_fields": FIELDS, "_limit": PAGE, "_skip": skip})
        if r.status_code == 200:
            return r.json().get("data", [])
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt * 2)
            continue
        r.raise_for_status()
    return []

def main(start=None, count=500):
    if not KEY:
        sys.exit("MP_API_KEY is not set. Add it to .env and restart the container.")
    os.makedirs("ledger", exist_ok=True); os.makedirs("data", exist_ok=True)
    last = int(open(STATE).read()) if os.path.exists(STATE) else 0
    start = last if start is None else start
    p = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True})
    with open(LEDGER, "a") as led, open(DUMP, "a") as dump:
        for skip in range(start, start + count, PAGE):
            for d in fetch(skip):
                ts = datetime.now(timezone.utc).isoformat()
                struct = d.get("structure")
                msg = {"source": "materials_project", "record_type": "material",
                       "record_id": d["material_id"],
                       "payload": {"material_id": d["material_id"], "formula_pretty": d.get("formula_pretty"),
                                   "band_gap": d.get("band_gap"),
                                   "formation_energy_per_atom": d.get("formation_energy_per_atom"),
                                   "nsites": d.get("nsites"),
                                   "structure_json": json.dumps(struct) if struct else None},
                       "event_ts": ts, "ingest_ts": ts, "schema_version": 1}
                line = json.dumps(msg)
                p.produce(TOPIC, key=d["material_id"], value=line)
                led.write(d["material_id"] + "\n"); dump.write(line + "\n")
            p.poll(0)
            open(STATE, "w").write(str(skip + PAGE))
            time.sleep(PAUSE)
    p.flush()
    print("done, next skip =", open(STATE).read())

if __name__ == "__main__":
    a = sys.argv[1:]
    main(int(a[0]) if a else None, int(a[1]) if len(a) > 1 else 500)