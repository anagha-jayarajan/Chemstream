# ChemStream Data Dictionary (schema_version 1)

Storage: Delta tables under `lake/`. Source: PubChem compound properties (ChEMBL and
Materials Project to be added). All timestamps are UTC.

Data flow: Kafka -> Bronze (raw) -> Silver (validated, standardized) -> Gold (one row per molecule).

| Table | Path | Grain (one row per) |
|---|---|---|
| Bronze | lake/bronze | Kafka message |
| Silver | lake/silver | source record (source, record_id) |
| Quarantine | lake/quarantine | rejected message |
| Gold | lake/gold | unique molecule (inchikey) |
| quality_metrics | lake/quality_metrics | quality-check run |

## Bronze (lake/bronze): raw, never modified

| Column | Type | Meaning |
|---|---|---|
| key | string | Kafka message key (record ID) |
| raw_json | string | Original message exactly as received |
| topic | string | Kafka topic (identifies the source, e.g. chem.pubchem.raw) |
| partition | int | Kafka partition |
| offset | long | Position in the partition; (topic, partition, offset) is unique per message |
| kafka_ts | timestamp | Time Kafka received the message |
| bronze_ts | timestamp | Time Spark wrote the row |

## Silver (lake/silver): validated and standardized, one row per (source, record_id)

Structure columns (formula, mol_weight, canonical_smiles, inchikey) are computed by RDKit
from the standardized molecule, so they are always consistent with each other.

| Column | Type | Meaning |
|---|---|---|
| source | string | pubchem (later chembl, materials_project) |
| record_id | string | ID in the source (PubChem CID) |
| formula | string | Molecular formula of the standardized molecule (RDKit) |
| mol_weight | double | Molecular weight of the standardized molecule in g/mol (RDKit, 3 decimals) |
| canonical_smiles | string | RDKit canonical SMILES after keeping the largest fragment and neutralizing charges |
| inchikey | string | InChIKey computed from the standardized molecule |
| source_smiles | string | SMILES exactly as delivered by the source (before standardization) |
| source_formula | string | Formula as delivered by the source |
| source_mol_weight | double | Molecular weight as delivered by the source |
| ingest_ts | timestamp | Time the producer sent the record |
| bronze_ts | timestamp | Time written to Bronze |
| silver_ts | timestamp | Time written to Silver |

## Quarantine (lake/quarantine): rejected records

Same columns as Silver (structure columns are null when standardization failed), plus:

| Column | Type | Meaning |
|---|---|---|
| reason | string | bad_json, missing_smiles, bad_smiles or standardize_failed |
| raw_json | string | Original message, kept for debugging |

## Gold (lake/gold): one row per unique molecule, ready for ML

| Column | Type | Meaning |
|---|---|---|
| inchikey | string | Unique key of the molecule |
| canonical_smiles | string | Standardized SMILES (model input) |
| formula | string | Molecular formula (from the standardized structure) |
| mol_weight | double | Molecular weight in g/mol (from the standardized structure) |
| sources | array<string> | Sources that reported this molecule |
| updated_ts | timestamp | Latest Silver write for this molecule |

Gold currently has no property labels (no solubility, toxicity, band gap). PubChem supplies
only formula and weight here; benchmark labels come from static MoleculeNet and Materials
Project data until ChEMBL and Materials Project ingestion is added.

## quality_metrics (lake/quality_metrics): one row per run of spark_jobs/quality.py

| Column | Type | Meaning |
|---|---|---|
| run_ts | string | Time of the run (ISO 8601, UTC) |
| bronze_rows, silver_rows, quarantine_rows | long | Row counts per layer |
| valid_pct | double | silver / (silver + quarantine) x 100, counted per message |
| quarantine_reasons | string (JSON text) | Count per rejection reason, e.g. {"bad_smiles": 2} |
| silver_null_counts | string (JSON text) | Null count per Silver column |
| silver_duplicates | long | Silver rows minus distinct (source, record_id); expected 0 |
| silver_freshness_sec | long | Seconds since the newest Silver record (meaningful only while data flows) |
| gold_rows, gold_distinct_inchikey, gold_null_inchikey | long | Gold row count, distinct keys, null keys |
| gold_inchikey_unique | boolean | True if Gold has one row per InChIKey and no nulls |
| all_checks_passed | boolean | True if every evaluable check passed (valid_pct at least 90, no duplicates, Gold unique) |

quarantine_reasons and silver_null_counts are JSON text, so parse them in the dashboard.

## Design decisions

1. **Standardization.** Silver keeps the largest fragment (removes salts and counter-ions) and
   neutralizes charges. An ion and its neutral form (for example formate and formic acid)
   therefore map to the same canonical SMILES and the same InChIKey, and Gold keeps one row
   for them. Reason: benchmark datasets use neutral structures, and keeping both forms could
   leak near-identical molecules between training and test sets. Consequence: a model cannot
   distinguish an ion from its neutral form, and charge or pH information is lost.
   The original structure is kept in source_smiles and source_formula, so the decision can be
   revisited without refetching data.
2. **Computed vs source values.** formula and mol_weight are computed by RDKit from the
   standardized molecule. They can differ from PubChem's values in the last decimal
   (for example 180.159 vs 180.16 for aspirin) because of rounding and atomic-weight tables,
   and they differ more for ions, salts or charged records that were standardized.
3. **Deduplication.** Silver deduplicates with a Delta MERGE on (source, record_id).
   Gold deduplicates on inchikey, using min() for text and numeric columns so results are
   deterministic.
4. **Replay traffic (later).** Records produced by the replay producer in unique mode get a
   synthetic record_id suffix (for example 2244-r3). They exist only for throughput and
   fault-tolerance experiments and must not be used for model training.