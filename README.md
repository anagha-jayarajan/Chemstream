
# ChemStream

**A real-time big-data pipeline for chemical reaction and material property prediction.**

ChemStream ingests data from public chemistry repositories (PubChem, ChEMBL, Materials Project)
as it is published, cleans and stores it in a Delta Lake, trains property-prediction models
(classical baselines and graph neural networks), and serves predictions and pipeline health
through an API and a live dashboard.

Central claim to demonstrate: a streaming architecture can match static-dataset model accuracy
while adding scale, freshness and fault tolerance.

---

## 1. Team and roles

| Role | Member | Owns | Folders |
|---|---|---|---|
| **Member A**: Data Engineer | Anagha Jayarajan | Kafka producers, Spark streaming, Delta Lake (Bronze/Silver/Gold), data quality, fault tolerance, scalability tests | `producers/`, `spark_jobs/`, `tools/`, data docs |
| **Member B**: ML Engineer | Arya Mohan G | Featurization, Spark MLlib baselines, GNN models, experiment tracking, retraining, benchmark comparison | `ml/` (create it) |
| **Member C**: Platform Engineer | Mohammed Yazin | CI, Docker orchestration, monitoring, FastAPI service, dashboard, testing, deployment, coordination | `api/`, `dashboard/`, `infra/` (create them) |

Full methodology, week-by-week plan and evaluation plan: see the project PDF
(`ChemStream_Methodology_and_Work_Division.pdf`).

---

## 2. Current status (Milestone M1 reached for PubChem)

| Component | Status | Owner |
|---|---|---|
| Docker stack (Kafka + Python/Spark container) | Done | A |
| PubChem producer (incremental, rate-limited, retries, ledger) | Done | A |
| Bronze layer (Kafka to Delta, checkpointed) | Done and tested | A |
| Silver layer (validate, standardize, dedupe, quarantine) | Done and tested | A |
| Gold layer (one row per unique molecule) | Done (batch job) | A |
| Data quality metrics table | Done | A |
| Data contract, data dictionary, test results | Done (`docs/`) | A |
| Replay producer (repeatable load for experiments) | Next | A |
| ChEMBL and Materials Project producers | Planned | A |
| Fault-tolerance and scalability experiments (E1-E4) | Planned | A |
| Featurization, baselines, GNNs, MLflow | Not started | B |
| API, dashboard, monitoring, CI, deployment | Not started | C |

Current data volume is small (about 300 PubChem records, 290 unique molecules). It is a pipeline
demonstration, not a training set. Training starts on static benchmark datasets (see section 7).

---

## 3. Architecture

```
PubChem API --> producer --> Kafka topic chem.pubchem.raw
                                  |
                       Spark Structured Streaming
                                  |
   Bronze (raw, as received)  -->  Silver (validated + standardized)  -->  Gold (one row per molecule)
                                  |                                         |
                          Quarantine (rejected records)            ML training, API, dashboard
```

| Layer | Path | One row per | Purpose |
|---|---|---|---|
| Bronze | `lake/bronze` | Kafka message | Untouched raw data; everything else can be rebuilt from it |
| Silver | `lake/silver` | source record | Parsed, validated, standardized with RDKit, deduplicated |
| Quarantine | `lake/quarantine` | rejected message | Bad records with a reason code |
| Gold | `lake/gold` | unique molecule | Clean, unified table for ML and serving |
| quality_metrics | `lake/quality_metrics` | quality run | Counts, valid %, nulls, duplicates, uniqueness checks |

Column-level details: [`docs/data_dictionary.md`](docs/data_dictionary.md).
Message format and source field mapping: [`docs/data_contract.md`](docs/data_contract.md).
Test evidence: [`docs/test_results.md`](docs/test_results.md).

Key design points:
- **Exactly-once into Bronze** via Kafka offsets stored in the Spark checkpoint plus transactional Delta writes (tested, see test results).
- **Idempotent Silver** via Delta `MERGE` on `(source, record_id)`, so reprocessing never creates duplicates.
- **Reproducible**: Silver and Gold can be deleted and rebuilt from Bronze without calling any API.
- **PubChem has no "new records" feed**, so the producer uses the CID number as a watermark and continues after the last CID processed. Replay mode (coming) provides repeatable load for experiments.

---

## 4. Repository structure

```
chemstream/
├── docker-compose.yml        Kafka + app container
├── Dockerfile                Python 3.11, Java 17, PySpark, Delta, RDKit, Kafka client
├── requirements.txt          Pinned versions (PySpark 3.5.1 must match delta-spark 3.2.0)
├── common/                   config.py (paths, Kafka address), spark_session.py
├── producers/                pubchem_producer.py
├── spark_jobs/               bronze.py, silver.py, gold.py, quality.py
├── tools/                    peek.py, check_bronze.py, check_silver.py, inject_bad.py,
│                             show_quality.py, find_dups.py
├── docs/                     data_contract.md, data_dictionary.md, test_results.md
└── (generated, not in Git)   lake/  checkpoints/  ledger/  data/  .venv/
```

---

## 5. Run the pipeline (everyone should do this once)

Requirements: Docker Desktop (running) and Git. Everything else runs inside containers.
Commands work in PowerShell and in bash. Run them from the repository folder.

**1. Start the stack** (first build takes several minutes)
```
docker compose up -d --build
docker compose ps
```
Both `kafka` and `chemstream-app` should be running.

**2. Create the Kafka topics** (once)
```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.pubchem.raw --partitions 3 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.dlq --partitions 1 --bootstrap-server localhost:9092
```

**3. Produce data** (first 200 PubChem compounds; takes about a minute)
```
docker compose exec app python -m producers.pubchem_producer 1 200
```

**4. Start the streaming jobs, each in its own terminal, and leave them running**
```
docker compose exec app python -m spark_jobs.bronze
docker compose exec app python -m spark_jobs.silver
```

**5. Build Gold and the quality report** (new terminal; wait about a minute after step 4)
```
docker compose exec app python -m spark_jobs.gold
docker compose exec app python -m spark_jobs.quality
```
Expected: `all_checks_passed: True`.

**Useful checks**
```
docker compose exec app python -m tools.peek gold          # row counts per layer + sample rows
docker compose exec app python -m tools.show_quality       # history of quality runs
```

**Reset everything** (stop the jobs first)
```
docker compose exec app rm -rf lake checkpoints ledger data
```

Notes:
- The first Spark start downloads packages and needs internet.
- Spark UI: http://localhost:4040 (only while a Spark job holds that port).
- Several Spark sessions compete for CPU. Stop streaming jobs before heavy one-off queries, or use `get_spark(name, cores="2")`.

---

## 6. What Member A has done (summary)

1. Docker stack with Kafka (KRaft) and a Python/Spark container.
2. PubChem producer: incremental watermark, rate limiting, retry with exponential backoff, idempotent Kafka producer, ledger of every ID sent, local dump for replay.
3. Bronze: Kafka to Delta with checkpoints. Checkpoint test passed (300 sent, 0 missing, 0 duplicates after a restart).
4. Silver: JSON parsing, RDKit standardization (largest fragment, neutralize charges), InChIKey, formula and weight computed from the standardized molecule, MERGE deduplication, quarantine with reason codes (`bad_json`, `missing_smiles`, `bad_smiles`, `standardize_failed`).
5. Gold: one row per InChIKey, deterministic aggregation, Z-ordering.
6. Quality job: counts, valid %, null counts, duplicates, Gold uniqueness, one row per run.
7. Found and fixed a labeling bug (`bad_json`) and a formula consistency issue; both documented in `docs/test_results.md`.

**Next for Member A:** replay producer, ChEMBL producer, Materials Project producer, fault-injection tests, throughput/latency/scaling experiments, runbook.

---

## 7. What Member B (ML Engineer) should take

**Your goal:** predict molecular and material properties and show streaming-trained models match static-trained ones.

**Start now (you do not need to wait for the pipeline):**
1. **B1: tasks and metrics.** Fix the tasks: ESOL solubility (RMSE), Tox21 toxicity (ROC-AUC), Materials Project band gap and formation energy (RMSE or MAE). Choose scaffold splits for molecules and a composition-aware split for materials. Write the evaluation protocol.
2. **B2: featurization** on static MoleculeNet datasets first: RDKit descriptors, Morgan fingerprints, molecular graphs; crystal graphs for materials (pymatgen).
3. **B3-B4: baselines and GNNs** on static data (Spark MLlib / XGBoost, then PyTorch Geometric). Use a GPU on Colab or Kaggle if needed.

**What Member A gives you:**
- Table: `lake/gold` (one row per unique molecule). Columns: `inchikey`, `canonical_smiles`, `formula`, `mol_weight`, `sources`, `updated_ts`.
- A sample file `gold_sample.csv` (sent directly, not in Git).
- Gold has **no property labels yet**. PubChem gives only formula and weight here. Labels for training come from static benchmark files until ChEMBL and Materials Project ingestion is added.

**Read the Gold table without Spark** (on your machine, from a copy of the `lake/gold` folder):
```
pip install deltalake pandas
```
```python
from deltalake import DeltaTable
df = DeltaTable("lake/gold").to_pandas()
```

**Things you must know:**
- Ions are merged with their neutral forms (for example formate and formic acid become one molecule). The original structure is kept in Silver columns `source_smiles`. Tell Member A if you need charge-aware modelling. See `docs/data_dictionary.md`, design decision 1.
- Pin the Delta table **version** you train on and log it in MLflow, so every result is reproducible through time travel.
- Replay-generated records (coming) have synthetic IDs and must never be used for training.

**What you hand to Member C:** an MLflow-registered model with a `predict(input)` function, input validation rules, version, metrics and a model card.

**Your experiments:** E5 (accuracy vs published benchmarks, 3 seeds), E6 (streaming-trained vs static-trained), E7 (freshness and drift).

---

## 8. What Member C (Platform Engineer) should take

**Your goal:** make the system runnable, observable, testable and demonstrable.

**Start now:**
1. **C1: repo hygiene.** Branch rules (no direct commits to `main`, one reviewer per pull request), GitHub Actions for linting, unit tests and Docker build. Create the `api/`, `dashboard/`, `infra/` folders.
2. **C2: orchestration.** Take ownership of `docker-compose.yml` (Member A wrote the Kafka and app services). Add health checks, environment variables, and later services (MLflow, Prometheus, Grafana, API, dashboard). Coordinate changes through pull requests.
3. **C4: API skeleton** (FastAPI) with a dummy model: predict endpoints for molecules and materials, health, models list, pipeline status. Validate SMILES with RDKit.
4. **C5: dashboard skeleton** (Streamlit or Dash) with four pages: live ingestion, data quality, model performance, prediction playground.

**What Member A gives you:**
- `lake/quality_metrics`: one row per quality run. `quarantine_reasons` and `silver_null_counts` are **JSON text**, so parse them. This feeds the data-quality page.
- `lake/gold`, `lake/silver`, `lake/quarantine`: read with `deltalake` or DuckDB, so the dashboard does not need to start Spark.
- Pipeline health numbers (input rate, batch duration, Kafka lag) will be added for Grafana later.
- Ports: Kafka from your host machine is `localhost:29092`; inside containers it is `kafka:9092`. Spark UI is `localhost:4040`.

**What you need from Member B:** a registered model and a `predict` contract (see section 7). Until then use the dummy model.

**Your experiments and tests:** E8 (API load test with Locust), E9 (full end-to-end demo with failure injection), integration tests, deployment guide, demo script, and final report assembly.

---

## 9. Interface contracts (do not change silently)

| Interface | From to | Contract |
|---|---|---|
| Kafka message | producers to Spark | Key = record ID; value = JSON with `source`, `record_type`, `record_id`, `payload`, `event_ts`, `ingest_ts`, `schema_version` |
| Silver and Gold schemas | A to B and C | `docs/data_dictionary.md`; any change gets a new `schema_version` and an announcement |
| Training snapshot | A to B | Delta table version number pinned in each MLflow run |
| Model package | B to C | MLflow registered model + `predict()` + validation rules + model card |
| Pipeline metrics | A to C | `lake/quality_metrics` table (now), metrics endpoint (later) |

Message example:
```json
{"source": "pubchem", "record_type": "compound", "record_id": "2244",
 "payload": {"CID": 2244, "MolecularFormula": "C9H8O4", "MolecularWeight": "180.16",
             "ConnectivitySMILES": "CC(=O)OC1=CC=CC=C1C(=O)O", "InChIKey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"},
 "event_ts": "2026-11-07T06:14:25+00:00", "ingest_ts": "2026-11-07T06:14:25+00:00", "schema_version": 1}
```

---

## 10. Known limitations and design decisions

- **"Real-time" is incremental.** PubChem, ChEMBL and Materials Project update in releases, not as event streams. The producers poll incrementally; replay mode will provide repeatable load.
- **Ion merging.** Silver neutralizes charges and keeps the largest fragment, so an ion and its neutral form share one Gold row. Original structures are kept in `source_*` columns.
- **Gold is a batch job** (re-run it periodically). Delta time travel keeps every version.
- **Spark runs in local mode** inside one container. Scaling experiments vary core count (`local[N]`), which is not the same as multiple machines; the report must say so.
- **RDKit vs PubChem weights** can differ in the last decimal.
- **Small data.** About 300 records so far; scale tests use replay traffic.

---

## 11. Working agreement

- Work on feature branches (`feature/data-pipeline`, `feature/ml`, `feature/platform`); open pull requests to `main`; one reviewer from a different track.
- Weekly 30-minute integration meeting: demo, contract check, blockers.
- Never commit `lake/`, `checkpoints/`, `ledger/`, `data/`, `.venv/` or `.env` (API keys).
- Pin library versions; log the git commit and the Delta table version in every experiment.
- Milestones: **M1** (Week 6) thin slice end to end; **M2** (Week 9) full system and frozen experiments; **M3** (Week 12) final delivery.

---

## 12. Troubleshooting

| Problem | Fix |
|---|---|
| `failed to connect to the docker API` | Start Docker Desktop and wait for "Engine running" |
| `Connection refused` to Kafka | Inside containers use `kafka:9092`, not `localhost` |
| First Spark run seems stuck | It is downloading packages; wait a few minutes (needs internet) |
| Schema or checkpoint errors after code changes | Stop jobs, delete `checkpoints/` and the affected `lake/` folder, restart from Bronze |
| Containers killed or very slow | Docker Desktop, Settings, Resources: at least 4 GB memory (6 GB better) |
| `No module named 'tools'` (or `common`, `producers`) | Add an empty `__init__.py` to that folder |
| Interactive Spark query hangs | Stop streaming jobs, or use `get_spark(name, cores="2")` |
