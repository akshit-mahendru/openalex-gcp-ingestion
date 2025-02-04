# scripts/etl/run_etl.py
import os
import logging
from datetime import datetime
from download.s3_downloader import OpenAlexDownloader
from transform.json_to_csv import OpenAlexTransformer
from load.db_loader import OpenAlexLoader

def setup_logging(base_dir):
    log_dir = os.path.join(base_dir, 'logs', 'etl')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        filename=os.path.join(log_dir, f'etl_main_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def run_etl(base_dir):
    """Run the complete ETL process."""
    setup_logging(base_dir)
    
    logging.info("Starting ETL process")
    
    try:
        # Download data
        logging.info("Starting download phase")
        downloader = OpenAlexDownloader(base_dir)
        if not downloader.download_all_entities():
            raise Exception("Download phase failed")
        
        # Transform data
        logging.info("Starting transform phase")
        transformer = OpenAlexTransformer(base_dir)
        if not transformer.transform_all_entities():
            raise Exception("Transform phase failed")
        
        # Load data
        logging.info("Starting load phase")
        db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "postgres",
            "user": "postgres",
            "password": os.environ.get("DB_PASSWORD")
        }
        loader = OpenAlexLoader(base_dir, db_config)
        if not loader.load_all_entities():
            raise Exception("Load phase failed")
        
        logging.info("ETL process completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return False

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    run_etl(base_dir)