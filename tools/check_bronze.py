from common.spark_session import get_spark
from common.config import BRONZE

spark = get_spark("check")
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("delta").load(BRONZE)
total = df.count()
distinct = df.select("topic", "partition", "offset").distinct().count()
print("rows:", total, "| distinct kafka offsets:", distinct, "| duplicates:", total - distinct)