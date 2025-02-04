# scripts/etl/transform/streaming_processor.py
import gzip
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from ..utils.streaming_base import StreamingBase
from ..utils.entity_processors import EntityProcessors

class StreamingProcessor(StreamingBase):
    def __init__(self, base_dir, db_config):
        super().__init__(base_dir, "processor")
        self.db_config = db_config
        self.conn = None
        self.batch_size = 1000
        self.connect()

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database connection error: {str(e)}")
            raise

    def process_file(self, file_path, entity_type):
        """Process a gzipped JSON Lines file and load to database."""
        processor = EntityProcessors.get_processor(entity_type)
        if not processor:
            logging.error(f"No processor found for entity type: {entity_type}")
            return False

        current_batch = []
        records_processed = 0

        try:
            with gzip.open(file_path, 'rt') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        processed_data = processor(data)
                        current_batch.append(processed_data)
                        
                        if len(current_batch) >= self.batch_size:
                            self.load_batch(current_batch, entity_type)
                            records_processed += len(current_batch)
                            current_batch = []
                            logging.info(f"Processed {records_processed} records from {file_path}")

                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error in {file_path}: {str(e)}")
                        continue

                # Load any remaining records
                if current_batch:
                    self.load_batch(current_batch, entity_type)
                    records_processed += len(current_batch)

            logging.info(f"Completed processing {file_path}. Total records: {records_processed}")
            return True

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {str(e)}")
            return False

    def load_batch(self, batch, entity_type):
        """Load a batch of records to the database."""
        if not batch:
            return

        table = f"openalex.{entity_type}"
        columns = list(batch[0].keys())
        values = [[record.get(col) for col in columns] for record in batch]

        query = f"""
            INSERT INTO {table} ({','.join(columns)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col != 'id')}
        """

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Database error loading batch: {str(e)}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")