import os

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
EXP = os.getenv("EXP", "")          # EXP=name runs everything in an isolated sandbox

if EXP:
    LAKE = f"/workspace/exp/{EXP}/lake"
    CKPT = f"/workspace/exp/{EXP}/ckpt"
    TOPIC_PATTERN = f"chem.{EXP}.raw"
else:
    LAKE = "/workspace/lake"
    CKPT = "/workspace/checkpoints"
    TOPIC_PATTERN = r"chem\.(pubchem|chembl|mp)\.raw"

BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
QUARANTINE = f"{LAKE}/quarantine"
GOLD = f"{LAKE}/gold"
QUALITY = f"{LAKE}/quality_metrics"
SILVER_MAT = f"{LAKE}/silver_materials"
QUAR_MAT = f"{LAKE}/quarantine_materials"
GOLD_MAT = f"{LAKE}/gold_materials"

BRONZE_TRIGGER = os.getenv("BRONZE_TRIGGER", "5 seconds")
SILVER_TRIGGER = os.getenv("SILVER_TRIGGER", "10 seconds")