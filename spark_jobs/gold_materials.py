from pyspark.sql import functions as F
from common.config import SILVER_MAT, GOLD_MAT
from common.spark_session import get_spark

spark = get_spark("gold_materials")
spark.sparkContext.setLogLevel("WARN")
s = spark.read.format("delta").load(SILVER_MAT)
g = s.select(
    F.col("record_id").alias("material_id"), "formula", "reduced_formula", "n_sites", "volume", "density",
    "band_gap", "formation_energy_per_atom", "structure_json",
    F.col("band_gap").isNotNull().alias("has_band_gap"),
    F.col("formation_energy_per_atom").isNotNull().alias("has_formation_energy"),
    F.col("silver_ts").alias("updated_ts"))
g.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_MAT)
print("gold materials rows:", g.count())