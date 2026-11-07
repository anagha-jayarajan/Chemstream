# ChemStream: Member A Test Results (Data Engineering)

Date: 2026-11-07
Environment: Docker Compose on a single laptop. Kafka 3.8.0 (KRaft, 1 broker, 3 partitions),
Spark 3.5.1 (local mode), Delta Lake 3.2.0, RDKit 2024.3.5. Source: PubChem PUG REST.

## 1. Bronze checkpoint test (exactly-once on restart)

Goal: show the streaming job resumes from its checkpoint without losing or repeating records.

Procedure:
1. Bronze job running; 200 records had been produced. Bronze rows = 200 (matched the producer ledger).
2. Stopped the Bronze job (Ctrl+C).
3. While the job was stopped, produced 100 more records (CIDs 201-300).
4. Restarted the Bronze job; it read its checkpoint and continued from the saved Kafka offsets.
   The first batch after restart was slower than the 5 s trigger (catching up on the backlog);
   later batches were normal.
5. Compared Bronze with the producer ledger (ledger/pubchem_sent.csv) and checked for repeated
   Kafka offsets with tools/check_bronze.py.

| Measure | Value |
|---|---|
| Records sent (producer ledger) | 300 |
| Bronze rows after restart | 308 (300 real records + 8 injected test messages, see section 2) |
| Distinct (topic, partition, offset) | 308 |
| Duplicates (rows minus distinct offsets) | 0 |
| Missing (ledger minus Bronze) | 0 |

Result: PASS. Every produced record reached Bronze exactly once across a job restart.
Delivery into Bronze is exactly-once, achieved by the Kafka offsets stored in the Spark
checkpoint together with the transactional Delta write.

## 2. Silver validation and quarantine test

Four test messages were injected with tools/inject_bad.py (run twice, 8 messages in total):

| Test message | Expected outcome | Observed |
|---|---|---|
| Malformed JSON | Quarantine, reason bad_json | Quarantined as bad_json (after the fix in section 5) |
| Invalid SMILES text | Quarantine, reason bad_smiles | Quarantined as bad_smiles |
| Record with no SMILES | Quarantine, reason missing_smiles | Quarantined as missing_smiles |
| Duplicate of record 1 | No new Silver row (MERGE) | Silver count unchanged |

Result: PASS. Invalid records are separated from valid ones, and duplicates are absorbed.

## 3. Data quality report (latest run, after all fixes)

```
run_ts                  : 2026-11-07T07:11:50.893478+00:00
bronze_rows             : 308
silver_rows             : 300
quarantine_rows         : 6
valid_pct               : 98.04
quarantine_reasons      : {"missing_smiles": 2, "bad_smiles": 2, "bad_json": 2}
silver_null_counts      : all 0 (source, record_id, formula, mol_weight, canonical_smiles,
                          inchikey, source_smiles, source_formula, source_mol_weight,
                          ingest_ts, bronze_ts, silver_ts)
silver_duplicates       : 0
silver_freshness_sec    : 87
gold_rows               : 290
gold_distinct_inchikey  : 290
gold_null_inchikey      : 0
gold_inchikey_unique    : True
all_checks_passed       : True
```

Reconciliation: 308 Bronze rows = 300 real PubChem records + 8 injected test messages.
Silver (300) + quarantine (6) + 2 duplicate test messages absorbed by MERGE = 308.
No message is lost.

Definitions: valid_pct = silver / (silver + quarantine) x 100, counted per message
(not per unique molecule). silver_freshness_sec is only meaningful while data is flowing.
Silver keeps one row per source record; Gold keeps one row per unique InChIKey.

## 4. Why Gold (290) has fewer rows than Silver (300)

The gap was investigated with tools/find_dups.py. Exactly 10 InChIKeys appear twice in
Silver, each shared by two PubChem CIDs (10 extra rows = the 10-row gap). In every case one
record is a charged ion and the other is its neutral conjugate acid or base. RDKit's
neutralization step maps both to the same canonical SMILES, so they share an InChIKey and Gold
keeps a single row per molecule.

| CIDs | Forms as delivered by PubChem | Standardized SMILES | Formula after standardization |
|---|---|---|---|
| 283, 284 | formate (CHO2-) and formic acid (CH2O2) | O=CO | CH2O2 |
| 259, 260 | bromide (Br-) and hydrogen bromide (BrH) | Br | HBr |
| 233, 234 | arsenate (AsHO4 2-) and arsenic acid (AsH3O4) | O=[As](O)(O)O | H3AsO4 |
| 276, 277 | CH2NO2- and CH3NO2 (carbamate and carbamic acid) | NC(=O)O | CH3NO2 |
| 112, 113 | C4H6O8P- and C4H7O8P | O=C(O)C(=O)C(O)COP(=O)(O)O | C4H7O8P |
| 285, 286 | C2H4NO3- and C2H5NO3 | NOCC(=O)O | C2H5NO3 |
| 222, 223 | ammonia (H3N) and ammonium (H4N+) | N | H3N |
| 175, 176 | acetate (C2H3O2-) and acetic acid (C2H4O2) | CC(=O)O | C2H4O2 |
| 44, 45 | C3H2O5 2- and C3H4O5 | O=C(O)C(O)C(=O)O | C3H4O5 |
| 242, 243 | benzoate (C7H5O2-) and benzoic acid (C7H6O2) | O=C(O)c1ccccc1 | C7H6O2 |

Design decision: ions are merged with their neutral forms (see docs/data_dictionary.md,
design decision 1). The original charged structure is kept in the Silver columns
source_smiles, source_formula and source_mol_weight. Consequence for modelling: a model
cannot distinguish an ion from its neutral form, although properties such as solubility and
toxicity can differ between them. This was communicated to the ML member.

## 5. Bug found through the quality report: wrong reason for malformed JSON

First quality run (before the fix):
```
quarantine_reasons : {"missing_smiles": 4, "bad_smiles": 2}
```
Expected bad_json: 2, bad_smiles: 2, missing_smiles: 2. The two malformed-JSON messages
were labeled missing_smiles.

Cause: Silver tested whether the parsed record (the from_json result) was null to detect bad
JSON, but a message that fails to parse comes back with all fields null rather than a
null record, so the test never fired and the message fell through to the SMILES check.

Fix (spark_jobs/silver.py): test the record_id field instead of the whole record:
```
before: F.when(F.col("m").isNull(), "bad_json")
after : F.when(F.col("m.record_id").isNull(), "bad_json")
```

Recovery: deleted lake/silver, lake/quarantine and checkpoints/silver, restarted the
Silver job, and rebuilt Silver and Gold from Bronze with no new API calls. This shows
the pipeline is reproducible from the raw layer.

Second quality run (after the fix):
```
quarantine_reasons : {"missing_smiles": 2, "bad_smiles": 2, "bad_json": 2}
```
Reasons now match the injected test messages.

## 6. Operational note

Running Bronze, Silver and an interactive Spark shell at the same time on one laptop made the
interactive query stall, because every Spark session tries to use all CPU cores. Check scripts
now run with a limited core count (get_spark(name, cores="2")), and streaming jobs are stopped
before heavy one-off queries. This matters for the scalability experiments, where core count is
the variable being changed.

## 7. Fix: formula and weight computed from the standardized molecule

Problem: the first Gold version used first() for formula and mol_weight. For the 10 merged
pairs this is not deterministic, so a Gold row could show the formula of the charged record
(for example CHO2-) next to the neutral SMILES (O=CO).

Fix:
- Silver now computes formula (rdMolDescriptors.CalcMolFormula) and mol_weight
  (Descriptors.MolWt) from the standardized molecule, so they always match canonical_smiles.
- The original values are kept in source_smiles, source_formula and source_mol_weight.
- Gold uses min() instead of first(), so its output is deterministic.
- Silver and Gold were rebuilt from Bronze with no new API calls.

Verification:
- Counts unchanged: Silver 300, quarantine 6, Gold 290, all_checks_passed = True.
- tools/find_dups.py: both CIDs in each of the 10 merged pairs now report the same formula
  (see the last column of the table in section 4). Before the fix the two records of each pair
  showed different formulas.
- The three new source_* columns have 0 nulls.

Note: RDKit molecular weights can differ from PubChem's in the last decimal (rounding and
atomic-weight tables), so mol_weight should not be compared exactly with source_mol_weight.