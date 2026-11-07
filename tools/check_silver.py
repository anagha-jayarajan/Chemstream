from common.spark_session import get_spark
from common.config import BRONZE, SILVER, QUARANTINE

spark = get_spark("check_silver")
spark.sparkContext.setLogLevel("ERROR")
b = spark.read.format("delta").load(BRONZE).count()
s = spark.read.format("delta").load(SILVER).count()
try:
    qdf = spark.read.format("delta").load(QUARANTINE)
    q = qdf.count()
except Exception:
    qdf, q = None, 0
print("bronze:", b, "| silver:", s, "| quarantine:", q)
if qdf is not None:
    qdf.groupBy("reason").count().show()
spark.read.format("delta").load(SILVER).select(
    "record_id", "canonical_smiles", "inchikey", "mol_weight").show(5, truncate=False)