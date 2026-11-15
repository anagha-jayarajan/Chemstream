from pyspark.sql import functions as F
from delta.tables import DeltaTable
from common.config import SILVER, GOLD
from common.spark_session import get_spark

spark = get_spark("gold"); spark.sparkContext.setLogLevel("WARN")
silver = spark.read.format("delta").load(SILVER)
gold = (silver.groupBy("inchikey").agg(
    F.min("canonical_smiles").alias("canonical_smiles"),
    F.min("formula").alias("formula"),
    F.min("mol_weight").alias("mol_weight"),
    F.collect_set("source").alias("sources"),
    F.max("silver_ts").alias("updated_ts")))
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD)
DeltaTable.forPath(spark, GOLD).optimize().executeZOrderBy("inchikey")
print("gold rows:", spark.read.format("delta").load(GOLD).count())