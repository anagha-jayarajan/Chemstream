import json, os, sys, time, requests
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

API = ("https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cids}"
       "/property/MolecularFormula,MolecularWeight,CanonicalSMILES,InChIKey/JSON")
TOPIC, BATCH, PAUSE = "chem.pubchem.raw", 20, 0.25   # ~4 requests/s, under PubChem's 5/s limit
STATE, LEDGER, DUMP = "ledger/pubchem_watermark.txt", "ledger/pubchem_sent.csv", "data/pubchem_dump.jsonl"

def fetch(cids):
    url = API.format(cids=",".join(map(str, cids)))
    for attempt in range(5):                       # retry with exponential backoff
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            return r.json()["PropertyTable"]["Properties"]
        if r.status_code == 404:
            return []
        if r.status_code in (429, 503, 504):
            time.sleep(2 ** attempt); continue
        r.raise_for_status()
    return []

def now():
    return datetime.now(timezone.utc).isoformat()

def main(start=None, count=200):
    os.makedirs("ledger", exist_ok=True); os.makedirs("data", exist_ok=True)
    last = int(open(STATE).read()) if os.path.exists(STATE) else 0
    start = start or last + 1                      # watermark: continue after the last CID done
    p = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True})
    with open(LEDGER, "a") as led, open(DUMP, "a") as dump:
        for s in range(start, start + count, BATCH):
            cids = list(range(s, min(s + BATCH, start + count)))
            for rec in fetch(cids):
                ts = now()
                msg = {"source": "pubchem", "record_type": "compound", "record_id": str(rec["CID"]),
                       "payload": rec, "event_ts": ts, "ingest_ts": ts, "schema_version": 1}
                line = json.dumps(msg)
                p.produce(TOPIC, key=str(rec["CID"]), value=line)
                led.write(f"{rec['CID']}\n"); dump.write(line + "\n")
            p.poll(0)
            open(STATE, "w").write(str(cids[-1]))
            time.sleep(PAUSE)
    p.flush()
    print("done, watermark =", open(STATE).read())

if __name__ == "__main__":
    a = sys.argv[1:]
    main(int(a[0]) if a else None, int(a[1]) if len(a) > 1 else 200)