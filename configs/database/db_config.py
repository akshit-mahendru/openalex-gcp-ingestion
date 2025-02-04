# configs/database/db_config.py
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
TEMP_DIR = "data/temp"
LOG_DIR = "logs/etl"