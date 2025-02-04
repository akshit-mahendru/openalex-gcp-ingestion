# scripts/etl/download/s3_downloader.py
import os
import subprocess
import logging
from datetime import datetime

class OpenAlexDownloader:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.raw_dir = os.path.join(base_dir, 'data', 'raw')
        self.setup_logging()

    def setup_logging(self):
        log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            filename=os.path.join(log_dir, f'download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def download_entity(self, entity_type):
        """Download a specific entity type from OpenAlex S3."""
        target_dir = os.path.join(self.raw_dir, entity_type)
        os.makedirs(target_dir, exist_ok=True)

        s3_path = f"s3://openalex/data/{entity_type}/updated_date=2024-01-*"
        
        logging.info(f"Starting download of {entity_type}")
        try:
            cmd = [
                "aws", "s3", "sync",
                "--no-sign-request",
                s3_path,
                target_dir
            ]
            subprocess.run(cmd, check=True)
            logging.info(f"Successfully downloaded {entity_type}")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Error downloading {entity_type}: {str(e)}")
            return False

    def download_all_entities(self):
        """Download all entity types."""
        entities = ['works', 'authors', 'concepts', 'institutions', 'venues']
        
        for entity in entities:
            success = self.download_entity(entity)
            if not success:
                logging.error(f"Failed to download {entity}")
                return False
        return True

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    downloader = OpenAlexDownloader(base_dir)
    downloader.download_all_entities()