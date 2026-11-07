from common.spark_session import get_spark
from common.config import SILVER

spark = get_spark("dups", cores="2")      # use only 2 cores so it doesn't hang
spark.sparkContext.setLogLevel("ERROR")
s = spark.read.format("delta").load(SILVER)

# InChIKeys that appear more than once in Silver
d = s.groupBy("inchikey").count().filter("count > 1")
d.show(20, truncate=False)

# the records behind those duplicates
(s.join(d.select("inchikey"), "inchikey")
  .select("inchikey", "record_id", "formula", "canonical_smiles")
  .orderBy("inchikey")
  .show(30, truncate=False))