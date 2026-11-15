import json
from datetime import datetime, timezone
from confluent_kafka import Producer
from common.config import KAFKA

NOW = datetime.now(timezone.utc).isoformat()
def msg(rid, smiles):
    payload = {"CID": 0, "MolecularFormula": "X", "MolecularWeight": "1.0", "InChIKey": "X"}
    if smiles:
        payload["ConnectivitySMILES"] = smiles
    return json.dumps({"source": "pubchem", "record_type": "compound", "record_id": rid,
                       "payload": payload, "event_ts": NOW, "ingest_ts": NOW, "schema_version": 1})

p = Producer({"bootstrap.servers": KAFKA})
tests = ["{this is not json",                 # expect reason: bad_json
         msg("t1", "not_smiles"),             # expect reason: bad_smiles
         msg("t2", None),                     # expect reason: missing_smiles
         msg("1", "CC(=O)OC(CC(=O)[O-])C[N+](C)(C)C")]   # duplicate of record 1, silver count must not grow
for t in tests:
    p.produce("chem.pubchem.raw", value=t)
p.flush()
print("sent", len(tests))