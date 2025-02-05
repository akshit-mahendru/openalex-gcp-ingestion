# configs/database/db_config.py
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_config():
    config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5435)),
        "database": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD"),
    }
    
    # Debug log (without password)
    debug_config = config.copy()
    debug_config['password'] = 'REDACTED' if config['password'] else 'NOT SET'
    logging.info(f"Database configuration: {debug_config}")
    
    # Validate configuration
    if not config['password']:
        raise ValueError("Database password not set in environment variables")
    
    return config

# Export the config
DB_CONFIG = get_db_config()

# Constants
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
TEMP_DIR = os.path.join("data", "temp")
LOG_DIR = os.path.join("logs", "etl")
