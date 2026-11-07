import os
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
LAKE = "/workspace/lake"
CKPT = "/workspace/checkpoints"
BRONZE = f"{LAKE}/bronze"
SILVER = f"{LAKE}/silver"
QUARANTINE = f"{LAKE}/quarantine"
GOLD = f"{LAKE}/gold"
QUALITY = f"{LAKE}/quality_metrics"