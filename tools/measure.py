import csv, json, os
from pyspark.sql import functions as F
from common.config import EXP, BRONZE, SILVER, QUARANTINE
from common.spark_session import get_spark

ROOT = "/workspace"
topic = f"chem.{EXP}.raw"
meta = json.load(open(f"{ROOT}/exp/{EXP}/meta.json"))
summ = json.load(open(f"{ROOT}/ledger/{topic}_summary.json"))

spark = get_spark("measure")
spark.sparkContext.setLogLevel("ERROR")
rd = lambda p: spark.read.format("delta").load(p)

bronze, silver = rd(BRONZE), rd(SILVER)
try:
    quar_n = rd(QUARANTINE).count()
except Exception:
    quar_n = 0
ledger = spark.read.csv(f"{ROOT}/ledger/{topic}_sent.csv").toDF("record_id")

n_ledger, n_bronze, n_silver = ledger.count(), bronze.count(), silver.count()
n_distinct = silver.select("record_id").distinct().count()
missing = ledger.join(silver.select("record_id"), "record_id", "left_anti").count()

lat = silver.select((F.col("silver_ts").cast("double") - F.col("ingest_ts").cast("double")).alias("lat"))
p50, p95, pmax = lat.selectExpr("percentile_approx(lat, 0.5)", "percentile_approx(lat, 0.95)", "max(lat)").first()
t_first, t_last = silver.agg(F.min(F.col("ingest_ts").cast("double")),
                             F.max(F.col("silver_ts").cast("double"))).first()
span = t_last - t_first

row = {"exp": EXP, "cores": meta["cores"], "rate_target": meta["rate"], "seconds": meta["seconds"],
       "trigger": meta["trigger"], "fault": meta["fault"],
       "sent_ok": summ["sent_ok"], "send_failed": summ["failed"],
       "producer_rate": round(summ["achieved_rate"], 1),
       "bronze_rows": n_bronze, "silver_rows": n_silver, "quarantine_rows": quar_n,
       "missing": missing, "duplicates": n_silver - n_distinct,
       "caught_up": meta["drained"], "catchup_s": round(meta["drain_seconds"], 1),
       "recovery_s": meta["recovery_s"],
       "lat_p50_s": round(p50, 1), "lat_p95_s": round(p95, 1), "lat_max_s": round(pmax, 1),
       "pipeline_rps": round(n_silver / span, 1) if span > 0 else None}

os.makedirs(f"{ROOT}/results", exist_ok=True)
path = f"{ROOT}/results/results.csv"
new = not os.path.exists(path)
with open(path, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(row))
    if new:
        w.writeheader()
    w.writerow(row)

print("\n===== RESULT =====")
for k, v in row.items():
    print(f"{k:16}: {v}")