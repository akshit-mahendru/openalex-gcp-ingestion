# scripts/etl/run_streaming_etl.py
import os
import sys
import logging
import signal
import threading
from datetime import datetime
from configs.database.db_config import DB_CONFIG
from scripts.etl.download.streaming_downloader import StreamingDownloader
from scripts.etl.transform.streaming_processor import StreamingProcessor
from scripts.etl.utils.state_manager import StateManager

class ETLManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.setup_logging()
        self.state_manager = StateManager(base_dir)
        self.running = True
        self.downloader = None
        self.processor = None
        self.setup_signal_handlers()

    def setup_logging(self):
        """Set up logging configuration."""
        log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'etl_main_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def setup_signal_handlers(self):
        """Set up handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logging.info("Received shutdown signal. Cleaning up...")
            self.running = False
            if self.processor:
                self.processor.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start_monitoring(self):
        """Start monitoring thread."""
        def monitor():
            while self.running:
                try:
                    # Use get_state_summary instead of load_state
                    state = self.state_manager.get_state_summary()
                    logging.info(f"Current progress: {state}")
                    import time
                    time.sleep(300)  # Monitor every 5 minutes
                except Exception as e:
                    logging.error(f"Monitoring error: {str(e)}")
                    import time
                    time.sleep(300)  # Prevent rapid error logging

        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()

    def run(self):
        """Run the ETL process."""
        try:
            self.downloader = StreamingDownloader(self.base_dir)
            self.processor = StreamingProcessor(self.base_dir, DB_CONFIG)
            self.start_monitoring()

            # Define entity types in processing order
            entity_types = ['works', 'authors', 'sources', 'institutions', 
                            'domains', 'fields', 'subfields', 'topics', 
                            'publishers']
            
            # Determine starting point based on current state
            state_summary = self.state_manager.get_state_summary()
            
            # Find the last incomplete or not started entity
            start_idx = 0
            for idx, entity_type in enumerate(entity_types):
                entity_state = state_summary['entities'].get(entity_type, {})
                if entity_state.get('status') != 'completed':
                    start_idx = idx
                    break
            
            # Slice entity types to start from the right point
            entity_types_to_process = entity_types[start_idx:]
            
            logging.info(f"Starting ETL process. Will process: {entity_types_to_process}")

            for entity_type in entity_types_to_process:
                if not self.running:
                    break

                logging.info(f"Starting processing of {entity_type}")
                
                def process_callback(file_path):
                    return self.processor.process_file(file_path, entity_type)
                
                success = self.downloader.process_entity(entity_type, process_callback)
                
                if not success:
                    logging.error(f"Failed to process {entity_type}")
                    return False
                
                logging.info(f"Completed processing {entity_type}")

            logging.info("ETL process completed successfully")
            return True
            
        except Exception as e:
            logging.error(f"ETL process failed: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return False
            
        finally:
            if self.processor:
                self.processor.close()

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    etl_manager = ETLManager(base_dir)
    success = etl_manager.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
