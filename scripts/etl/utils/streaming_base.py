# scripts/etl/utils/streaming_base.py
import os
import logging
from datetime import datetime

class StreamingBase:
    def __init__(self, base_dir, component_name):
        self.base_dir = base_dir
        self.temp_dir = os.path.join(base_dir, 'data', 'temp')
        self.setup_logging(component_name)

    def setup_logging(self, component_name):
        log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

        logging.basicConfig(
            filename=os.path.join(log_dir, f'{component_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def cleanup_temp_file(self, file_path):
        """Safely remove temporary file."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            logging.error(f"Error cleaning up {file_path}: {str(e)}")