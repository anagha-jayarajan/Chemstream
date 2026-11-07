from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark(name, cores="*"):
    builder = (SparkSession.builder.appName(name).master(f"local[{cores}]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4"))
    return configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]).getOrCreate()