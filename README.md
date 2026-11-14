# ChemStream

**A real-time big-data pipeline for chemical reaction and material property prediction.**

ChemStream ingests data from public chemistry and materials repositories (PubChem, ChEMBL,
Materials Project) as it is published, cleans and stores it in a Delta Lake, trains
property-prediction models (classical baselines and graph neural networks), and serves
predictions and pipeline health through an API and a live dashboard.

Central claim to demonstrate: a streaming architecture can match static-dataset model accuracy
while adding scale, freshness and fault tolerance.

---

## 1. Team and roles

| Role | Member | Owns | Folders |
|---|---|---|---|
| **Member A**: Data Engineer | ___Anagha Jayarajan_________ | Kafka producers, Spark streaming, Delta Lake layers, data quality, fault-tolerance and scalability experiments | `producers/`, `spark_jobs/`, `tools/`, data docs |
| **Member B**: ML Engineer | _Arya Mohan G___________ | Featurization, baselines, GNN models, experiment tracking, retraining, benchmark comparison | `ml/` (create it) |
| **Member C**: Platform Engineer | ___Mohammed Yazin_________ | CI, Docker orchestration, monitoring, FastAPI service, dashboard, testing, deployment, coordination | `api/`, `dashboard/`, `infra/` (create them) |

Full methodology, week-by-week plan and evaluation plan:
`docs/ChemStream_Methodology_and_Work_Division.pdf`.

---

## 2. Current status

*Member A: update this table as items are verified.*

| Component | Status | Owner |
|---|---|---|
| Docker stack (Kafka + Python/Spark container) | Done and verified | A |
| PubChem producer (incremental, rate-limited, retries, ledger) | Done and verified | A |
| Bronze layer (Kafka to Delta, checkpointed) | Done and verified (checkpoint test passed) | A |
| Silver layer for molecules (validate, standardize, dedupe, quarantine) | Done and verified | A |
| Gold layer for molecules (one row per unique molecule) | Done and verified | A |
| Data quality metrics table | Done and verified | A |
| Data contract, data dictionary, test results | Done (`docs/`) | A |
| Replay producer and experiment runner (`tools/run_experiment.py`) | Code in repo, results being collected | A |
| Fault-tolerance experiments (E3) | Code in repo, results being collected | A |
| Throughput, latency, scaling experiments (E1, E2, E4) | Code in repo, results being collected | A |
| Materials Project producer, Silver and Gold for materials | Code in repo, being verified | A |
| ChEMBL producer | Code in repo, being verified | A |
| Runbook and final report sections | Planned | A |
| Featurization, baselines, GNNs, MLflow | Not started | B |
| API, dashboard, monitoring, CI, deployment | Not started | C |

Headline results (filled in from `results/results.csv` when the experiments are final):

| Measure | Result |
|---|---|
| Bronze restart test | 300 sent, 0 missing, 0 duplicates (verified) |
| Max sustained throughput (records/s) | TBD |
| p95 end-to-end latency | TBD |
| Recovery after a crashed job | TBD |
| Loss or duplicates under all fault tests | TBD |

Data volume is small so far (hundreds of records per source). Pipeline experiments use replay
traffic; model training starts on static benchmark datasets (see section 9).

---

## 3. Architecture

```
PubChem API ----> producer ----> Kafka chem.pubchem.raw --+
ChEMBL API -----> producer ----> Kafka chem.chembl.raw ---+--> Bronze (raw)
Materials Project -> producer --> Kafka chem.mp.raw -------+        |
                                                                    |
              +-----------------------------------------------------+
              |                                                     |
      Silver molecules  --> Quarantine                Silver materials --> Quarantine materials
              |                                                     |
        Gold molecules                                      Gold materials
              |                                                     |
              +---------------> ML training, API, dashboard <-------+
```

| Layer | Path | One row per | Purpose |
|---|---|---|---|
| Bronze | `lake/bronze` | Kafka message | Untouched raw data; everything else can be rebuilt from it |
| Silver (molecules) | `lake/silver` | source record | PubChem and ChEMBL records, validated and standardized with RDKit |
| Quarantine | `lake/quarantine` | rejected message | Bad molecule records with a reason code |
| Gold (molecules) | `lake/gold` | unique molecule | Clean table for ML and serving |
| Silver (materials) | `lake/silver_materials` | material | Materials Project records validated with pymatgen |
| Quarantine (materials) | `lake/quarantine_materials` | rejected message | Bad material records with a reason code |
| Gold (materials) | `lake/gold_materials` | material | Crystal structure plus band gap and formation energy |
| quality_metrics | `lake/quality_metrics` | quality run | Counts, valid %, nulls, duplicates, uniqueness checks |

Column details: [`docs/data_dictionary.md`](docs/data_dictionary.md).
Message format and field mapping: [`docs/data_contract.md`](docs/data_contract.md).
Test evidence: [`docs/test_results.md`](docs/test_results.md).
Operations: [`docs/runbook.md`](docs/runbook.md).

Key design points:
- **Exactly-once into Bronze** through Kafka offsets in the Spark checkpoint plus transactional Delta writes (tested: restart with zero loss and zero duplicates).
- **Idempotent Silver** through Delta `MERGE` on `(source, record_id)`; reprocessing never creates duplicates.
- **Reproducible**: Silver and Gold can be deleted and rebuilt from Bronze without calling any API.
- **No true event streams**: PubChem, ChEMBL and Materials Project publish in releases. Producers poll incrementally with a saved watermark; a replay producer provides repeatable load for experiments.

---

## 4. Repository structure

```
chemstream/
├── docker-compose.yml        Kafka + app container (reads MP_API_KEY from .env)
├── Dockerfile                Python 3.11, Java 17, PySpark, Delta, RDKit, pymatgen, Kafka client
├── requirements.txt          Pinned versions (PySpark 3.5.1 must match delta-spark 3.2.0)
├── common/                   config.py (paths, topics, sandbox switch), spark_session.py
├── producers/                pubchem_producer.py, chembl_producer.py, mp_producer.py, replay_producer.py
├── spark_jobs/               bronze.py, silver.py, silver_materials.py, gold.py, gold_materials.py, quality.py
├── tools/                    peek.py, check_bronze.py, check_silver.py, show_quality.py, find_dups.py,
│                             inject_bad.py, inject_faults.py, run_experiment.py, measure.py
├── docs/                     data_contract.md, data_dictionary.md, test_results.md, runbook.md, project PDF
├── results/                  results.csv (experiment results, one row per run)
└── (generated, not in Git)   lake/  checkpoints/  ledger/  data/  exp/  .venv/  .env
```

---

## 5. Run the pipeline (everyone should do this once)

Requirements: Docker Desktop (running, at least 6 GB memory) and Git. Everything else runs in
containers. Commands work in PowerShell and bash, from the repository folder.

**1. Secrets (only needed for Materials Project).** Get a free API key from materialsproject.org.
Create a file named `.env` in the repository root with one line, no quotes:
```
MP_API_KEY=your_key_here
```
Never commit `.env` or paste the key anywhere public.

**2. Start the stack** (the first build takes several minutes)
```
docker compose up -d --build
docker compose ps
```
Both `kafka` and `chemstream-app` should be running.

**3. Create the Kafka topics** (once)
```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.pubchem.raw --partitions 3 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.chembl.raw --partitions 3 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.mp.raw --partitions 3 --bootstrap-server localhost:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic chem.dlq --partitions 1 --bootstrap-server localhost:9092
```

**4. Produce data** (each takes a minute or more because of API rate limits)
```
docker compose exec app python -m producers.pubchem_producer 1 300
docker compose exec app python -m producers.chembl_producer 0 500
docker compose exec app python -m producers.mp_producer 0 100
```

**5. Start the streaming jobs, one terminal each, and leave them running**
```
docker compose exec app python -m spark_jobs.bronze
docker compose exec app python -m spark_jobs.silver
docker compose exec app python -m spark_jobs.silver_materials
```

**6. Build Gold and the quality report** (new terminal, about a minute after step 5)
```
docker compose exec app python -m spark_jobs.gold
docker compose exec app python -m spark_jobs.gold_materials
docker compose exec app python -m spark_jobs.quality
```
Expected: `all_checks_passed: True`.

**Useful checks**
```
docker compose exec app python -m tools.peek gold            # counts per layer + sample rows
docker compose exec app python -m tools.show_quality         # history of quality runs
```

**Reset everything** (stop all jobs first)
```
docker compose exec app rm -rf lake checkpoints ledger data exp results
```

Notes:
- The first Spark start downloads packages and needs internet.
- Spark UI: http://localhost:4040 (only while a Spark job holds that port).
- Spark sessions compete for CPU; stop streaming jobs before heavy one-off queries.
- After editing `requirements.txt` or `docker-compose.yml`, run `docker compose up -d --build`
  (installed packages and environment variables only change when the container is rebuilt or recreated).

---

## 6. Experiments (Member A)

Each experiment runs in an isolated sandbox (its own Kafka topic, data folder and checkpoints),
so it never touches the real tables. Stop the main streaming jobs first.

```
docker compose exec app python -m tools.run_experiment --exp <new-name> --cores 2 --rate 500 --seconds 30
```

| Option | Meaning |
|---|---|
| `--cores N` | Spark threads per job (scaling experiment) |
| `--rate R`, `--seconds S` | Replay load: R messages per second for S seconds |
| `--trigger N` | Micro-batch interval in seconds for both jobs (default 5 and 10) |
| `--fault` | `kill_silver`, `kill_bronze`, `kill_both`, or `bad_and_dup` |
| `--fault_at`, `--downtime` | When to inject the fault and how long jobs stay down |

Results are appended to `results/results.csv`: counts per layer, `missing`, `duplicates`,
`lat_p50_s`, `lat_p95_s`, `pipeline_rps`, `recovery_s`. A Kafka outage is tested by hand
(stop and start the Kafka container while an experiment runs).

Experiments: E1 throughput, E2 latency, E3 fault tolerance, E4 scaling.
Caveats to state in the report: cores are threads inside one container (not a cluster), replay
traffic is synthetic, and results depend on the laptop CPU and Docker's CPU and memory limits.

---

## 7. What Member A has done (summary)

1. Docker stack with Kafka (KRaft) and a Python/Spark container; sandboxed experiment configuration.
2. PubChem producer: incremental watermark, rate limiting, retry with backoff, idempotent producer,
   ledger of every ID sent, local dump for replay.
3. Bronze: Kafka to Delta with checkpoints. Restart test passed (300 sent, 0 missing, 0 duplicates).
4. Silver: RDKit standardization (largest fragment, neutralize charges), InChIKey, formula and weight
   computed from the standardized molecule, MERGE deduplication, quarantine with reason codes.
5. Gold: one row per InChIKey, deterministic aggregation, Z-ordering.
6. Quality job: counts, valid %, null counts, duplicates, Gold uniqueness, one row per run.
7. Found and fixed two issues through the quality report and tests (wrong `bad_json` label;
   formula inconsistent with standardized SMILES). Both documented in `docs/test_results.md`.
8. Replay producer, fault injection and an automated experiment runner with results table.
9. Materials Project and ChEMBL ingestion, Silver and Gold for materials (verification in progress).

**Next for Member A:** finish and record experiments E1 to E4 on frozen code, runbook, report sections.

---

## 8. Interface contracts (do not change silently)

| Interface | From to | Contract |
|---|---|---|
| Kafka message | producers to Spark | Key = record ID; value = JSON with `source`, `record_type`, `record_id`, `payload`, `event_ts`, `ingest_ts`, `schema_version` |
| Silver and Gold schemas | A to B and C | `docs/data_dictionary.md`; any change gets a new `schema_version` and an announcement |
| Training snapshot | A to B | Delta table version number pinned in each MLflow run |
| Model package | B to C | MLflow registered model + `predict()` + validation rules + model card |
| Pipeline metrics | A to C | `lake/quality_metrics` table (now), metrics endpoint (later) |

Molecule message (PubChem; ChEMBL uses the same payload keys):
```json
{"source": "pubchem", "record_type": "compound", "record_id": "2244",
 "payload": {"CID": 2244, "MolecularFormula": "C9H8O4", "MolecularWeight": "180.16",
             "ConnectivitySMILES": "CC(=O)OC1=CC=CC=C1C(=O)O", "InChIKey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"},
 "event_ts": "2026-11-07T06:14:25+00:00", "ingest_ts": "2026-11-07T06:14:25+00:00", "schema_version": 1}
```
Material message (Materials Project):
```json
{"source": "materials_project", "record_type": "material", "record_id": "mp-149",
 "payload": {"material_id": "mp-149", "formula_pretty": "Si", "band_gap": 0.61,
             "formation_energy_per_atom": 0.0, "nsites": 2, "structure_json": "{...pymatgen dict as text...}"},
 "event_ts": "...", "ingest_ts": "...", "schema_version": 1}
```

---

## 9. What Member B (ML Engineer) should take

**Your goal:** predict molecular and material properties and show streaming-trained models match
static-trained ones.

**What you receive (a handoff zip from Member A, not stored in Git):**
- `gold_materials`: one row per material with the crystal structure and the labels you need.
  Columns: `material_id`, `formula`, `reduced_formula`, `n_sites`, `volume`, `density`, `band_gap`,
  `formation_energy_per_atom`, `structure_json`, `has_band_gap`, `has_formation_energy`, `updated_ts`.
- `gold`: one row per unique molecule: `inchikey`, `canonical_smiles`, `formula`, `mol_weight`,
  `sources`, `updated_ts`.
- `gold_sample.csv` and `gold_materials_sample.csv` for a quick look.

**Which task gets its labels where:**

| Task | Labels from |
|---|---|
| Band gap, formation energy (materials) | `gold_materials` (from the pipeline). Filter with `has_band_gap` and `has_formation_energy`, because some materials have no value. |
| Solubility (ESOL), toxicity (Tox21) | Static MoleculeNet files. The pipeline's molecule Gold has structures but **no property labels yet**. |

**Read the tables** (copy the folders from the zip; no Spark needed):
```
pip install deltalake pandas pymatgen
```
```python
from deltalake import DeltaTable
import json
from pymatgen.core import Structure

mats = DeltaTable("gold_materials").to_pandas()
mats = mats[mats.has_band_gap]                       # rows that have a band gap label
s = Structure.from_dict(json.loads(mats.iloc[0].structure_json))   # crystal for your graph builder
mols = DeltaTable("gold").to_pandas()
```
If reading fails on your machine, tell Member A; the same code runs inside the project container.

**Things you must know:**
- **Ions are merged with their neutral forms** (for example formate and formic acid become one
  molecule). The original charged structure is kept in Silver (`source_smiles`). If you need
  charge-aware modelling, tell Member A. See `docs/data_dictionary.md`, design decision 1.
- **Split by composition for materials.** Several materials share a formula (polymorphs). Use
  `reduced_formula` as the group key so related crystals do not land in both train and test.
- **ChEMBL's calculated properties** (ALogP, PSA, max phase) are stored in Bronze `raw_json` but are
  not yet columns in Silver or Gold. Ask Member A if you want them promoted.
- **Pin the Delta table version** you train on and log it in MLflow so results are reproducible.
- **Replay data is synthetic** (record IDs like `2244-r3`). It never appears in the real tables, but
  never train on anything from the `exp/` sandboxes.

**Start now, in this order:**
1. B1: fix tasks, metrics and splits; write the evaluation protocol.
2. B2: featurization on static MoleculeNet sets (RDKit descriptors, Morgan fingerprints, graphs) and
   on `gold_materials` (crystal graphs).
3. B3 and B4: baselines (Spark MLlib or XGBoost), then PyTorch Geometric GNNs. Use a Colab or Kaggle GPU.

**You hand to Member C:** an MLflow-registered model with `predict(input)`, input validation rules,
version, metrics and a model card.

**Your experiments:** E5 (accuracy vs published benchmarks, 3 seeds), E6 (streaming-trained vs
static-trained), E7 (freshness and drift).

---

## 10. What Member C (Platform Engineer) should take

**Your goal:** make the system runnable, observable, testable and demonstrable.

**Start now:**
1. C1: repository hygiene. Branch rules (no direct commits to `main`, one reviewer per pull request),
   GitHub Actions for linting, unit tests and Docker build. Create `api/`, `dashboard/`, `infra/`.
2. C2: take ownership of `docker-compose.yml` (Member A wrote the Kafka and app services). Add health
   checks and later MLflow, Prometheus, Grafana, API and dashboard services; coordinate through
   pull requests. Keep secrets out of the file: `MP_API_KEY` is read from `.env`.
3. C4: API skeleton (FastAPI) with a dummy model: predict endpoints for molecules and materials,
   health, models list, pipeline status. Validate SMILES with RDKit.
4. C5: dashboard skeleton (Streamlit or Dash): live ingestion, data quality, model performance,
   prediction playground.

**Data you can use now (from the handoff zip):**
- `quality_metrics`: one row per quality run. `quarantine_reasons` and `silver_null_counts` are
  **JSON text**, so parse them. New fields: `mat_silver_rows`, `mat_quarantine_rows`, `mat_gold_rows`,
  `mat_no_band_gap`, `mat_no_formation_energy`.
- `results/results.csv` (in Git): experiment results for a "System performance" page
  (throughput, latency, recovery, scaling).
- `gold` and `gold_materials`: read with `deltalake` so the dashboard does not need to start Spark.

```python
from deltalake import DeltaTable
q = DeltaTable("quality_metrics").to_pandas().sort_values("run_ts")
```

**Facts you need:**
- Kafka from your host machine: `localhost:29092`; inside containers: `kafka:9092`.
- Spark UI: `http://localhost:4040` while a Spark job is running.
- Pipeline health numbers (input rate, batch duration, Kafka lag) are not exposed yet. Agree with
  Member A what to expose for Grafana.

**You need from Member B:** a registered model and the `predict` contract (section 9). Until then use
a dummy model.

**Your experiments and tests:** E8 (API load test with Locust), E9 (full end-to-end demo with failure
injection, using `tools.run_experiment --fault ...` or stopping containers), integration tests,
deployment guide, demo script and final report assembly.

---

## 11. Known limitations and design decisions

- **"Real-time" is incremental.** The three sources update in releases. Producers poll with a saved
  watermark; replay mode provides repeatable load for experiments.
- **Ion merging.** Silver neutralizes charges and keeps the largest fragment, so an ion and its
  neutral form share one Gold row. Originals are kept in `source_*` columns.
- **Missing structures and labels.** Some ChEMBL entries (for example antibodies) have no SMILES and
  go to quarantine as `missing_smiles`; some Materials Project entries lack a band gap or formation
  energy (`has_band_gap`, `has_formation_energy` flags).
- **Materials Project paging** has no explicit sort, so a page can occasionally repeat or skip a
  record; Silver deduplicates on `material_id`.
- **Gold is a batch job** (re-run periodically). Delta time travel keeps every version.
- **Spark runs in local mode** in one container. Scaling experiments vary core count, not machines.
- **Replay traffic is synthetic**; it is used only for system experiments, never for training.
- **Small data.** Hundreds of records per source so far.

---

## 12. Working agreement

- Feature branches (`feature/data-pipeline`, `feature/ml`, `feature/platform`); pull requests to
  `main`; one reviewer from a different track.
- Weekly 30-minute integration meeting: demo, contract check, blockers.
- **Never commit** `.env`, `lake/`, `checkpoints/`, `ledger/`, `data/`, `exp/` or `.venv/`.
  A leaked API key must be regenerated, because deleting it from Git does not remove it from history.
- Pin library versions; log the git commit and Delta table version in every experiment.
- Milestones: **M1** (Week 6) thin slice end to end; **M2** (Week 9) full system and frozen
  experiments; **M3** (Week 12) final delivery.

---

## 13. Troubleshooting

| Problem | Fix |
|---|---|
| `failed to connect to the docker API` | Start Docker Desktop and wait for "Engine running" |
| `service "app" is not running` | `docker compose up -d` |
| `Connection refused` to Kafka | Inside containers use `kafka:9092`, not `localhost` |
| `ModuleNotFoundError` (for example `pymatgen`, `deltalake`) | Package is missing from `requirements.txt`, or the image was not rebuilt: save the file, then `docker compose up -d --build` |
| `KeyError: 'MP_API_KEY'` | Put the key in `.env`, make sure `docker-compose.yml` has `MP_API_KEY: ${MP_API_KEY:-}` under `environment:`, then `docker compose up -d` |
| Docker build fails with `Invalid requirement` | `requirements.txt` has a stray terminal command in it; keep only package names |
| First Spark run seems stuck | It is downloading packages; wait a few minutes (needs internet) |
| Schema or checkpoint errors after code changes | Stop jobs, delete `checkpoints/` and the affected `lake/` folder, restart from Bronze |
| Containers killed or very slow | Docker Desktop, Settings, Resources: at least 6 GB memory |
| `No module named 'tools'` (or `common`, `producers`) | Add an empty `__init__.py` to that folder |
| Interactive Spark query hangs | Stop streaming jobs, or use `get_spark(name, cores="2")` |
