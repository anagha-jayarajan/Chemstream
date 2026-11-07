from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors
from rdkit.Chem.MolStandardize import rdMolStandardize
from common.config import BRONZE, SILVER, QUARANTINE, CKPT
from common.spark_session import get_spark

_tools = {}
def clean(smiles):
    if not smiles:
        return (None, None, None, None, "missing_smiles")
    if not _tools:
        _tools["frag"] = rdMolStandardize.LargestFragmentChooser()
        _tools["unch"] = rdMolStandardize.Uncharger()
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return (None, None, None, None, "bad_smiles")
    try:
        mol = _tools["unch"].uncharge(_tools["frag"].choose(mol))   # strip salts, neutralize
        return (Chem.MolToSmiles(mol),
                Chem.MolToInchiKey(mol),
                rdMolDescriptors.CalcMolFormula(mol),               # formula of the CLEANED molecule
                round(Descriptors.MolWt(mol), 3),                   # weight of the CLEANED molecule
                None)
    except Exception:
        return (None, None, None, None, "standardize_failed")

clean_udf = F.udf(clean, T.StructType([
    T.StructField("canonical_smiles", T.StringType()),
    T.StructField("inchikey", T.StringType()),
    T.StructField("formula", T.StringType()),
    T.StructField("mol_weight", T.DoubleType()),
    T.StructField("reason", T.StringType())]))

PAYLOAD = T.StructType([T.StructField(n, T.StringType()) for n in
    ["CID", "MolecularFormula", "MolecularWeight", "CanonicalSMILES", "ConnectivitySMILES", "SMILES", "InChIKey"]])
MSG = T.StructType([T.StructField("source", T.StringType()), T.StructField("record_type", T.StringType()),
    T.StructField("record_id", T.StringType()), T.StructField("payload", PAYLOAD),
    T.StructField("event_ts", T.StringType()), T.StructField("ingest_ts", T.StringType()),
    T.StructField("schema_version", T.IntegerType())])

spark = get_spark("silver"); spark.sparkContext.setLogLevel("WARN")

def process(batch_df, batch_id):
    df = batch_df.withColumn("m", F.from_json("raw_json", MSG))
    df = df.withColumn("smiles_in", F.coalesce("m.payload.ConnectivitySMILES",
                       "m.payload.CanonicalSMILES", "m.payload.SMILES"))
    df = df.withColumn("c", clean_udf("smiles_in"))
    df = df.select(
        F.col("m.source").alias("source"),
        F.col("m.record_id").alias("record_id"),
        F.col("c.formula").alias("formula"),
        F.col("c.mol_weight").alias("mol_weight"),
        F.col("c.canonical_smiles").alias("canonical_smiles"),
        F.col("c.inchikey").alias("inchikey"),
        F.col("smiles_in").alias("source_smiles"),
        F.col("m.payload.MolecularFormula").alias("source_formula"),
        F.col("m.payload.MolecularWeight").cast("double").alias("source_mol_weight"),
        F.when(F.col("m.record_id").isNull(), "bad_json").otherwise(F.col("c.reason")).alias("reason"),
        F.col("m.ingest_ts").cast("timestamp").alias("ingest_ts"),
        "bronze_ts", F.current_timestamp().alias("silver_ts"), "raw_json")

    bad = df.filter("reason IS NOT NULL")
    bad.write.format("delta").mode("append").save(QUARANTINE)

    valid = df.filter("reason IS NULL").drop("reason", "raw_json").dropDuplicates(["source", "record_id"])
    if DeltaTable.isDeltaTable(spark, SILVER):
        (DeltaTable.forPath(spark, SILVER).alias("t")
         .merge(valid.alias("s"), "t.source = s.source AND t.record_id = s.record_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        valid.write.format("delta").save(SILVER)

(spark.readStream.format("delta").load(BRONZE)
 .writeStream.foreachBatch(process)
 .option("checkpointLocation", f"{CKPT}/silver")
 .trigger(processingTime="10 seconds").start().awaitTermination())