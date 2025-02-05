# scripts/etl/transform/streaming_processor.py
import gzip
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import traceback
from ..utils.streaming_base import StreamingBase
from ..utils.entity_processors import EntityProcessors
from ..utils.state_manager import StateManager

class StreamingProcessor(StreamingBase):
    def __init__(self, base_dir, db_config, batch_size=1000, max_errors=100):
        """
        Initialize the StreamingProcessor.

        :param base_dir: Base directory for the ETL process
        :param db_config: Database configuration dictionary
        :param batch_size: Number of records to process in a batch
        :param max_errors: Maximum number of errors before stopping processing
        """
        super().__init__(base_dir, "processor")
        self.db_config = db_config
        self.conn = None
        self.batch_size = batch_size
        self.max_errors = max_errors
        self.state_manager = StateManager(base_dir)
        self.connect()

    def connect(self):
        """
        Establish a database connection with retry mechanism.
        
        :return: True if connection is successful, False otherwise
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if self.conn is None or self.conn.closed:
                    self.conn = psycopg2.connect(**self.db_config)
                    self.conn.autocommit = False
                    logging.info("Database connection established")
                return True
            except Exception as e:
                retry_count += 1
                logging.error(f"Database connection error (attempt {retry_count}): {str(e)}")
                if retry_count == max_retries:
                    raise
                import time
                time.sleep(5)  # Wait before retrying

    def process_file(self, file_path, entity_type):
        """
        Process a gzipped JSON Lines file and load to database.

        :param file_path: Path to the gzipped JSON Lines file
        :param entity_type: Type of entity being processed
        :return: True if processing is successful, False otherwise
        """
        processor = EntityProcessors.get_processor(entity_type)
        if not processor:
            logging.error(f"No processor found for entity type: {entity_type}")
            return False

        # Expanded batch dictionary to support all potential entities
        current_batches = {
            'main': [],
            'ids': [],
            'counts_by_year': [],
            'authorships': [],
            'related_works': [],
            'referenced_works': [],
            'concepts': [],
            'open_access': [],
            'geo': [],
            'associated_institutions': [],
            # New taxonomy entities
            'domains': [],
            'fields': [],
            'subfields': [],
            'topics': [],
            'publishers': []
        }
        
        records_processed = 0
        errors = 0

        try:
            with gzip.open(file_path, 'rt') as f:
                for line_number, line in enumerate(f, 1):
                    try:
                        data = json.loads(line)
                        processed_data = processor(data)
                        
                        if processed_data:
                            self._collect_batches(entity_type, processed_data, current_batches)
                        
                        # Load batches when they reach the batch size
                        if len(current_batches['main']) >= self.batch_size:
                            if not self.load_batches(entity_type, current_batches):
                                errors += 1
                                if errors >= self.max_errors:
                                    logging.error(f"Too many errors ({errors}). Stopping processing.")
                                    return False
                            
                            records_processed += len(current_batches['main'])
                            
                            # Reset batches
                            for key in current_batches:
                                current_batches[key] = []
                            
                            logging.info(f"Processed {records_processed} records from {file_path}")

                    except json.JSONDecodeError as e:
                        errors += 1
                        logging.error(f"JSON decode error in {file_path} at line {line_number}: {str(e)}")
                        if errors >= self.max_errors:
                            return False
                        continue
                    except Exception as e:
                        errors += 1
                        logging.error(f"Error processing line {line_number} in {file_path}: {str(e)}")
                        logging.error(traceback.format_exc())
                        if errors >= self.max_errors:
                            return False
                        continue

                # Load any remaining records
                if any(current_batches.values()):
                    if not self.load_batches(entity_type, current_batches):
                        logging.error("Error loading final batch")
                        return False
                    records_processed += len(current_batches['main'])

            logging.info(f"Completed processing {file_path}. Total records: {records_processed}, Errors: {errors}")
            return errors < self.max_errors

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def _collect_batches(self, entity_type: str, processed_data: dict, current_batches: dict):
        """
        Collect batches for different entity types.

        :param entity_type: Type of entity being processed
        :param processed_data: Processed data from entity processor
        :param current_batches: Dictionary to collect batches
        """
        try:
            # Mapping for different entity types
            entity_batch_mapping = {
                'works': [
                    ('main', 'works'),
                    ('ids', 'works_ids'),
                    ('open_access', 'works_open_access'),
                    ('authorships', 'works_authorships'),
                    ('related_works', 'works_related_works'),
                    ('referenced_works', 'works_referenced_works'),
                    ('concepts', 'works_concepts'),
                    ('counts_by_year', 'works_counts_by_year')
                ],
                'authors': [
                    ('main', 'authors'),
                    ('ids', 'authors_ids'),
                    ('counts_by_year', 'authors_counts_by_year'),
                    ('concepts', 'authors_concepts')
                ],
                'sources': [
                    ('main', 'sources'),
                    ('ids', 'sources_ids'),
                    ('counts_by_year', 'sources_counts_by_year')
                ],
                'institutions': [
                    ('main', 'institutions'),
                    ('ids', 'institutions_ids'),
                    ('geo', 'institutions_geo'),
                    ('counts_by_year', 'institutions_counts_by_year'),
                    ('associated_institutions', 'institutions_associated_institutions')
                ],
                'domains': [
                    ('main', 'domains'),
                    ('counts_by_year', 'domains_counts_by_year')
                ],
                'fields': [
                    ('main', 'fields'),
                    ('counts_by_year', 'fields_counts_by_year')
                ],
                'subfields': [
                    ('main', 'subfields'),
                    ('counts_by_year', 'subfields_counts_by_year')
                ],
                'topics': [
                    ('main', 'topics'),
                    ('counts_by_year', 'topics_counts_by_year')
                ],
                'publishers': [
                    ('main', 'publishers'),
                    ('ids', 'publishers_ids'),
                    ('counts_by_year', 'publishers_counts_by_year')
                ]
            }

            # Collect batches for the specific entity type
            for batch_key, data_key in entity_batch_mapping.get(entity_type, []):
                if data_key in processed_data:
                    if isinstance(processed_data[data_key], list):
                        current_batches[batch_key].extend(processed_data[data_key])
                    elif processed_data[data_key]:
                        current_batches[batch_key].append(processed_data[data_key])
        except Exception as e:
            logging.error(f"Error collecting batches for {entity_type}: {str(e)}")
            logging.error(traceback.format_exc())

    def load_batches(self, entity_type: str, batches: dict):
        """
        Load batches of records to the database with error handling.

        :param entity_type: Type of entity being processed
        :param batches: Dictionary of batches to load
        :return: True if loading is successful, False otherwise
        """
        # Comprehensive table mapping for all entity types
        table_mapping = {
            'works': {
                'main': 'works',
                'ids': 'works_ids',
                'open_access': 'works_open_access',
                'authorships': 'works_authorships',
                'related_works': 'works_related_works',
                'referenced_works': 'works_referenced_works',
                'concepts': 'works_concepts',
                'counts_by_year': 'works_counts_by_year'
            },
            'authors': {
                'main': 'authors',
                'ids': 'authors_ids',
                'counts_by_year': 'authors_counts_by_year',
                'concepts': 'authors_concepts'
            },
            'sources': {
                'main': 'sources',
                'ids': 'sources_ids',
                'counts_by_year': 'sources_counts_by_year'
            },
            'institutions': {
                'main': 'institutions',
                'ids': 'institutions_ids',
                'geo': 'institutions_geo',
                'counts_by_year': 'institutions_counts_by_year',
                'associated_institutions': 'institutions_associated_institutions'
            },
            'domains': {
                'main': 'domains',
                'counts_by_year': 'domains_counts_by_year'
            },
            'fields': {
                'main': 'fields',
                'counts_by_year': 'fields_counts_by_year'
            },
            'subfields': {
                'main': 'subfields',
                'counts_by_year': 'subfields_counts_by_year'
            },
            'topics': {
                'main': 'topics',
                'counts_by_year': 'topics_counts_by_year'
            },
            'publishers': {
                'main': 'publishers',
                'ids': 'publishers_ids',
                'counts_by_year': 'publishers_counts_by_year'
            }
        }

        # Ensure the entity type is supported
        if entity_type not in table_mapping:
            logging.error(f"Unsupported entity type: {entity_type}")
            return False

        try:
            # Ensure connection is alive
            self.connect()
            
            with self.conn.cursor() as cur:
                # Process each batch type for the specific entity
                for batch_key, table_suffix in table_mapping[entity_type].items():
                    if batches.get(batch_key):
                        # Prepare columns and values
                        columns = list(batches[batch_key][0].keys())
                        table = f"openalex.{table_suffix}"
                        values = [[record.get(col) for col in columns] for record in batches[batch_key]]
                        
                        # Construct upsert query based on batch type
                        if batch_key == 'main':
                            # Primary entity table uses id as conflict target
                            query = f"""
                                INSERT INTO {table} ({','.join(columns)})
                                VALUES %s
                                ON CONFLICT (id) DO UPDATE
                                SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col != 'id')}
                            """
                        elif batch_key in ['ids', 'geo']:
                            # ID and geo tables use specific ID column
                            id_col = f"{entity_type[:-1]}_id"
                            query = f"""
                                INSERT INTO {table} ({','.join(columns)})
                                VALUES %s
                                ON CONFLICT ({id_col}) DO UPDATE
                                SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col != id_col)}
                            """
                        elif batch_key in ['counts_by_year', 'authorships', 'related_works', 'referenced_works', 'concepts', 'associated_institutions']:
                            # These tables use composite primary keys
                            pk_columns = [col for col in columns if col not in ['works_count', 'cited_by_count', 'score', 'author_position', 'primary_author', 'relationship']]
                            query = f"""
                                INSERT INTO {table} ({','.join(columns)})
                                VALUES %s
                                ON CONFLICT ({','.join(pk_columns)}) DO UPDATE
                                SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col not in pk_columns)}
                            """
                        else:
                            # Default - insert with no conflict handling
                            query = f"""
                                INSERT INTO {table} ({','.join(columns)})
                                VALUES %s
                                ON CONFLICT DO NOTHING
                            """
                        
                        # Execute batch insert
                        execute_values(cur, query, values)

            # Commit the transaction
            self.conn.commit()
            return True
        
        except Exception as e:
            # Rollback in case of error
            self.conn.rollback()
            logging.error(f"Database error loading batch for {entity_type}: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def close(self):
        """Close the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error closing database connection: {str(e)}")

    def __enter__(self):
        """Context manager entry method."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method to ensure connection closure."""
        self.close()
