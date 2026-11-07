from pyspark.sql import functions as F
from common.config import KAFKA, BRONZE, CKPT
from common.spark_session import get_spark

spark = get_spark("bronze")
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribePattern", r"chem\..*\.raw")     # all source topics land in one table
       .option("startingOffsets", "earliest").load())

bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("raw_json"),
    "topic", "partition", "offset",
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("bronze_ts"))

q = (bronze.writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", f"{CKPT}/bronze")
     .trigger(processingTime="5 seconds").start(BRONZE))
q.awaitTermination()