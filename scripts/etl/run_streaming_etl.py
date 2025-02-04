# scripts/etl/run_streaming_etl.py
import os
import sys
import logging
from datetime import datetime
from configs.database.db_config import DB_CONFIG
from download.streaming_downloader import StreamingDownloader
from transform.streaming_processor import StreamingProcessor

def run_streaming_etl(base_dir):
    """Run the streaming ETL process."""
    # Setup logging
    log_dir = os.path.join(base_dir, 'logs', 'etl')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        filename=os.path.join(log_dir, f'main_etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize components
    try:
        downloader = StreamingDownloader(base_dir)
        processor = StreamingProcessor(base_dir, DB_CONFIG)
        
        # Process each entity type
        entity_types = ['concepts', 'authors', 'institutions', 'venues', 'works']
        
        for entity_type in entity_types:
            logging.info(f"Starting processing of {entity_type}")
            
            def process_callback(file_path):
                return processor.process_file(file_path, entity_type)
            
            success = downloader.process_entity(entity_type, process_callback)
            
            if not success:
                logging.error(f"Failed to process {entity_type}")
                return False
            
            logging.info(f"Completed processing of {entity_type}")
        
        logging.info("ETL process completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        return False
        
    finally:
        if 'processor' in locals():
            processor.close()

if __name__ == "__main__":
    # Get base directory
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    # Run ETL
    if run_streaming_etl(base_dir):
        sys.exit(0)
    else:
        sys.exit(1)