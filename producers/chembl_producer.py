import json, os, sys, time, requests
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

API = "https://www.ebi.ac.uk/chembl/api/data/molecule.json"
TOPIC, PAGE, PAUSE = "chem.chembl.raw", 100, 0.5
STATE, LEDGER, DUMP = "ledger/chembl_watermark.txt", "ledger/chembl_sent.csv", "data/chembl_dump.jsonl"

def fetch(offset):
    for attempt in range(5):
        r = requests.get(API, timeout=60,
                         params={"limit": PAGE, "offset": offset, "order_by": "molecule_chembl_id"})
        if r.status_code == 200:
            return r.json().get("molecules", [])
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt * 2)
            continue
        r.raise_for_status()
    return []

def to_message(m):
    st = m.get("molecule_structures") or {}
    pr = m.get("molecule_properties") or {}
    ts = datetime.now(timezone.utc).isoformat()
    payload = {"ConnectivitySMILES": st.get("canonical_smiles"), "InChIKey": st.get("standard_inchi_key"),
               "MolecularFormula": pr.get("full_molformula"), "MolecularWeight": pr.get("full_mwt"),
               "ALogP": pr.get("alogp"), "PSA": pr.get("psa"),
               "max_phase": m.get("max_phase"), "pref_name": m.get("pref_name")}
    return {"source": "chembl", "record_type": "compound", "record_id": m["molecule_chembl_id"],
            "payload": payload, "event_ts": ts, "ingest_ts": ts, "schema_version": 1}

def main(start=None, count=500):
    os.makedirs("ledger", exist_ok=True); os.makedirs("data", exist_ok=True)
    last = int(open(STATE).read()) if os.path.exists(STATE) else 0
    start = last if start is None else start
    p = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True})
    with open(LEDGER, "a") as led, open(DUMP, "a") as dump:
        for offset in range(start, start + count, PAGE):
            for m in fetch(offset):
                msg = to_message(m)
                line = json.dumps(msg)
                p.produce(TOPIC, key=msg["record_id"], value=line)
                led.write(msg["record_id"] + "\n"); dump.write(line + "\n")
            p.poll(0)
            open(STATE, "w").write(str(offset + PAGE))
            time.sleep(PAUSE)
    p.flush()
    print("done, next offset =", open(STATE).read())

if __name__ == "__main__":
    a = sys.argv[1:]
    main(int(a[0]) if a else None, int(a[1]) if len(a) > 1 else 500)