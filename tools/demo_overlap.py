import json, time, requests
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA
from producers.pubchem_producer import fetch as pubchem_fetch
from producers.chembl_producer import to_message

CIDS = [2244, 2519, 3672, 1983]                          # aspirin, caffeine, ibuprofen, paracetamol
CHEMBL = ["CHEMBL25", "CHEMBL113", "CHEMBL521", "CHEMBL112"]

p = Producer({"bootstrap.servers": KAFKA})
for rec in pubchem_fetch(CIDS):
    ts = datetime.now(timezone.utc).isoformat()
    msg = {"source": "pubchem", "record_type": "compound", "record_id": str(rec["CID"]),
           "payload": rec, "event_ts": ts, "ingest_ts": ts, "schema_version": 1}
    p.produce("chem.pubchem.raw", key=msg["record_id"], value=json.dumps(msg))
for cid in CHEMBL:
    r = requests.get(f"https://www.ebi.ac.uk/chembl/api/data/molecule/{cid}.json", timeout=60)
    r.raise_for_status()
    msg = to_message(r.json())
    p.produce("chem.chembl.raw", key=msg["record_id"], value=json.dumps(msg))
    time.sleep(0.5)
p.flush()
print("sent 4 PubChem + 4 ChEMBL records")