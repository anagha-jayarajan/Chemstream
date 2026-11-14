from pyspark.sql import functions as F
from common.config import KAFKA, BRONZE, CKPT, TOPIC_PATTERN, BRONZE_TRIGGER
from common.spark_session import get_spark

spark = get_spark("bronze")
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribePattern", TOPIC_PATTERN)
       .option("startingOffsets", "earliest").load())

bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("raw_json"),
    "topic", "partition", "offset",
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("bronze_ts"))

q = (bronze.writeStream.format("delta").outputMode("append")
     .option("checkpointLocation", f"{CKPT}/bronze")
     .trigger(processingTime=BRONZE_TRIGGER).start(BRONZE))
q.awaitTermination()