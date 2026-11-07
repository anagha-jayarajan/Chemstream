import sys
from common.spark_session import get_spark
from common.config import LAKE
spark = get_spark("peek"); spark.sparkContext.setLogLevel("ERROR")
for name in ["bronze", "silver", "quarantine", "gold"]:
    try: print(name, spark.read.format("delta").load(f"{LAKE}/{name}").count())
    except Exception: print(name, "not created yet")
if len(sys.argv) > 1:
    spark.read.format("delta").load(f"{LAKE}/{sys.argv[1]}").show(5, truncate=False)