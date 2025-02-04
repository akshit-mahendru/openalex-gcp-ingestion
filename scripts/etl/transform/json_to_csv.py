# scripts/etl/transform/json_to_csv.py
import os
import json
import gzip
import csv
import logging
from datetime import datetime
from typing import Dict, List, Any

class OpenAlexTransformer:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.raw_dir = os.path.join(base_dir, 'data', 'raw')
        self.processed_dir = os.path.join(base_dir, 'data', 'processed')
        self.setup_logging()

    def setup_logging(self):
        log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            filename=os.path.join(log_dir, f'transform_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def flatten_json(self, json_obj: Dict, prefix: str = '') -> Dict:
        """Flatten nested JSON object."""
        flattened = {}
        for key, value in json_obj.items():
            if isinstance(value, dict):
                flattened.update(self.flatten_json(value, f"{prefix}{key}_"))
            elif isinstance(value, list):
                # Handle lists appropriately based on your needs
                flattened[f"{prefix}{key}"] = json.dumps(value)
            else:
                flattened[f"{prefix}{key}"] = value
        return flattened

    def transform_entity(self, entity_type: str):
        """Transform JSON Lines files to CSV for a specific entity type."""
        input_dir = os.path.join(self.raw_dir, entity_type)
        output_dir = os.path.join(self.processed_dir, entity_type)
        os.makedirs(output_dir, exist_ok=True)

        logging.info(f"Starting transformation of {entity_type}")

        try:
            # Process each gzipped file in the input directory
            for filename in os.listdir(input_dir):
                if not filename.endswith('.gz'):
                    continue

                input_path = os.path.join(input_dir, filename)
                output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.csv")

                with gzip.open(input_path, 'rt') as f_in, \
                     open(output_path, 'w', newline='') as f_out:
                    
                    # Read first line to get fields
                    first_line = f_in.readline()
                    first_obj = json.loads(first_line)
                    flattened_obj = self.flatten_json(first_obj)
                    fieldnames = list(flattened_obj.keys())

                    # Setup CSV writer
                    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(flattened_obj)

                    # Process remaining lines
                    for line in f_in:
                        obj = json.loads(line)
                        flattened = self.flatten_json(obj)
                        writer.writerow(flattened)

                logging.info(f"Transformed {filename} to CSV")

            return True

        except Exception as e:
            logging.error(f"Error transforming {entity_type}: {str(e)}")
            return False

    def transform_all_entities(self):
        """Transform all entity types."""
        entities = ['works', 'authors', 'concepts', 'institutions', 'venues']
        
        for entity in entities:
            success = self.transform_entity(entity)
            if not success:
                logging.error(f"Failed to transform {entity}")
                return False
        return True

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    transformer = OpenAlexTransformer(base_dir)
    transformer.transform_all_entities()