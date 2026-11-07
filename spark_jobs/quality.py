import json
from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
from common.config import BRONZE, SILVER, QUARANTINE, GOLD, QUALITY
from common.spark_session import get_spark

MIN_VALID_PCT = 90.0      # a run "fails" the validity check below this value; change if you like

spark = get_spark("quality")
spark.sparkContext.setLogLevel("ERROR")


def read(path):
    """Return the Delta table as a DataFrame, or None if it doesn't exist yet."""
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None


def count(df):
    return df.count() if df is not None else 0


bronze, silver, quar, gold = read(BRONZE), read(SILVER), read(QUARANTINE), read(GOLD)

# ---------- row counts and valid percentage ----------
bronze_rows, silver_rows, quarantine_rows = count(bronze), count(silver), count(quar)
processed = silver_rows + quarantine_rows
valid_pct = round(100.0 * silver_rows / processed, 2) if processed else None

# ---------- quarantine reasons ----------
reasons = {}
if quar is not None:
    reasons = {r["reason"]: int(r["count"]) for r in quar.groupBy("reason").count().collect()}

# ---------- Silver: null counts, duplicates, freshness ----------
null_counts, silver_dups, freshness_sec = {}, None, None
if silver is not None and silver_rows > 0:
    row = silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in silver.columns]).first()
    null_counts = {c: int(row[c] or 0) for c in silver.columns}
    distinct_keys = silver.select("source", "record_id").distinct().count()
    silver_dups = silver_rows - distinct_keys
    freshness_sec = silver.agg(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("silver_ts"))).alias("f")
    ).first()["f"]

# ---------- Gold: row count and InChIKey uniqueness ----------
gold_rows = gold_distinct = gold_null_ik = None
gold_unique = None
if gold is not None:
    gold_rows = gold.count()
    gold_distinct = gold.select("inchikey").distinct().count()
    gold_null_ik = gold.filter(F.col("inchikey").isNull()).count()
    gold_unique = (gold_rows == gold_distinct) and gold_null_ik == 0

# ---------- pass/fail summary (only checks that could be evaluated) ----------
checks = []
if valid_pct is not None:
    checks.append(valid_pct >= MIN_VALID_PCT)
if silver_dups is not None:
    checks.append(silver_dups == 0)
if gold_unique is not None:
    checks.append(gold_unique)
all_passed = all(checks) if checks else None

# ---------- build ONE row and append it to the quality_metrics table ----------
schema = T.StructType([
    T.StructField("run_ts", T.StringType()),
    T.StructField("bronze_rows", T.LongType()),
    T.StructField("silver_rows", T.LongType()),
    T.StructField("quarantine_rows", T.LongType()),
    T.StructField("valid_pct", T.DoubleType()),
    T.StructField("quarantine_reasons", T.StringType()),      # JSON text, e.g. {"bad_smiles": 3}
    T.StructField("silver_null_counts", T.StringType()),      # JSON text, one count per column
    T.StructField("silver_duplicates", T.LongType()),
    T.StructField("silver_freshness_sec", T.LongType()),
    T.StructField("gold_rows", T.LongType()),
    T.StructField("gold_distinct_inchikey", T.LongType()),
    T.StructField("gold_null_inchikey", T.LongType()),
    T.StructField("gold_inchikey_unique", T.BooleanType()),
    T.StructField("all_checks_passed", T.BooleanType()),
])
m = {
    "run_ts": datetime.now(timezone.utc).isoformat(),
    "bronze_rows": bronze_rows, "silver_rows": silver_rows, "quarantine_rows": quarantine_rows,
    "valid_pct": valid_pct,
    "quarantine_reasons": json.dumps(reasons), "silver_null_counts": json.dumps(null_counts),
    "silver_duplicates": silver_dups, "silver_freshness_sec": freshness_sec,
    "gold_rows": gold_rows, "gold_distinct_inchikey": gold_distinct,
    "gold_null_inchikey": gold_null_ik, "gold_inchikey_unique": gold_unique,
    "all_checks_passed": all_passed,
}
df = spark.createDataFrame([tuple(m[f.name] for f in schema.fields)], schema)
df.write.format("delta").mode("append").option("mergeSchema", "true").save(QUALITY)

# ---------- print a readable report ----------
print("\n=========== DATA QUALITY REPORT ===========")
for k, v in m.items():
    print(f"{k:24}: {v}")
print("===========================================")