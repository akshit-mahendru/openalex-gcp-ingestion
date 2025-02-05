# scripts/etl/load/db_loader.py
import os
import logging
import psycopg2
from datetime import datetime
from typing import List, Optional

class OpenAlexLoader:
    def __init__(self, base_dir, db_config):
        self.base_dir = base_dir
        self.processed_dir = os.path.join(base_dir, 'data', 'processed')
        self.db_config = db_config
        self.setup_logging()

    def setup_logging(self):
        log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            filename=os.path.join(log_dir, f'load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)

    def load_entity(self, entity_type: str, batch_size: int = 10000):
        """Load CSV files for a specific entity type into the database."""
        input_dir = os.path.join(self.processed_dir, entity_type)
        
        logging.info(f"Starting load of {entity_type}")

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Process each CSV file in the input directory
                    for filename in os.listdir(input_dir):
                        if not filename.endswith('.csv'):
                            continue

                        input_path = os.path.join(input_dir, filename)
                        table_name = f"openalex.{entity_type}"

                        # Use COPY command for efficient bulk loading
                        with open(input_path, 'r') as f:
                            cur.copy_expert(
                                f"COPY {table_name} FROM STDIN WITH CSV HEADER",
                                f
                            )
                        
                        conn.commit()
                        logging.info(f"Loaded {filename} into {table_name}")

            return True

        except Exception as e:
            logging.error(f"Error loading {entity_type}: {str(e)}")
            return False

    def load_all_entities(self):
        """Load all entity types."""
        entities = ['concepts', 'works', 'authors', 'institutions', 'venues']
        
        for entity in entities:
            success = self.load_entity(entity)
            if not success:
                logging.error(f"Failed to load {entity}")
                return False
        return True

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    
    db_config = {
        "host": "localhost",
        "port": 5435,
        "database": "postgres",
        "user": "postgres",
        "password": os.environ.get("DB_PASSWORD")
    }
    
    loader = OpenAlexLoader(base_dir, db_config)
    loader.load_all_entities()
