import json
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
from pymatgen.core import Structure
from common.config import BRONZE, SILVER_MAT, QUAR_MAT, CKPT, SILVER_TRIGGER
from common.spark_session import get_spark

def check_structure(sj):
    if not sj or sj == "null":
        return (None, None, None, None, "missing_structure")
    try:
        s = Structure.from_dict(json.loads(sj))
        if len(s) == 0 or not s.is_valid(tol=0.5):      # atoms closer than 0.5 A are invalid
            return (None, None, None, None, "bad_structure")
        return (s.composition.reduced_formula, len(s), float(s.volume), float(s.density), None)
    except Exception:
        return (None, None, None, None, "bad_structure")

structure_udf = F.udf(check_structure, T.StructType([
    T.StructField("reduced_formula", T.StringType()), T.StructField("n_sites", T.IntegerType()),
    T.StructField("volume", T.DoubleType()), T.StructField("density", T.DoubleType()),
    T.StructField("reason", T.StringType())]))

PAYLOAD = T.StructType([
    T.StructField("material_id", T.StringType()), T.StructField("formula_pretty", T.StringType()),
    T.StructField("band_gap", T.DoubleType()), T.StructField("formation_energy_per_atom", T.DoubleType()),
    T.StructField("nsites", T.IntegerType()), T.StructField("structure_json", T.StringType())])
MSG = T.StructType([
    T.StructField("source", T.StringType()), T.StructField("record_type", T.StringType()),
    T.StructField("record_id", T.StringType()), T.StructField("payload", PAYLOAD),
    T.StructField("event_ts", T.StringType()), T.StructField("ingest_ts", T.StringType()),
    T.StructField("schema_version", T.IntegerType())])

spark = get_spark("silver_materials")
spark.sparkContext.setLogLevel("WARN")

def process(batch_df, batch_id):
    df = (batch_df.withColumn("m", F.from_json("raw_json", MSG))
          .withColumn("c", structure_udf("m.payload.structure_json")))
    df = df.select(
        F.col("m.source").alias("source"), F.col("m.record_id").alias("record_id"),
        F.col("m.payload.formula_pretty").alias("formula"),
        F.col("c.reduced_formula").alias("reduced_formula"),
        F.col("c.n_sites").alias("n_sites"), F.col("c.volume").alias("volume"),
        F.col("c.density").alias("density"),
        F.col("m.payload.band_gap").alias("band_gap"),
        F.col("m.payload.formation_energy_per_atom").alias("formation_energy_per_atom"),
        F.col("m.payload.structure_json").alias("structure_json"),
        F.when(F.col("m.record_id").isNull(), "bad_json").otherwise(F.col("c.reason")).alias("reason"),
        F.col("m.ingest_ts").cast("timestamp").alias("ingest_ts"),
        "bronze_ts", F.current_timestamp().alias("silver_ts"), "raw_json")

    df.filter("reason IS NOT NULL").drop("structure_json") \
      .write.format("delta").mode("append").save(QUAR_MAT)

    valid = df.filter("reason IS NULL").drop("reason", "raw_json").dropDuplicates(["source", "record_id"])
    if DeltaTable.isDeltaTable(spark, SILVER_MAT):
        (DeltaTable.forPath(spark, SILVER_MAT).alias("t")
         .merge(valid.alias("s"), "t.source = s.source AND t.record_id = s.record_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        valid.write.format("delta").save(SILVER_MAT)

(spark.readStream.format("delta").load(BRONZE)
 .filter(F.col("topic") == "chem.mp.raw")
 .writeStream.foreachBatch(process)
 .option("checkpointLocation", f"{CKPT}/silver_materials")
 .trigger(processingTime=SILVER_TRIGGER).start().awaitTermination())