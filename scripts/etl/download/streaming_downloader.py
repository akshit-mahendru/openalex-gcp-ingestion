# scripts/etl/download/streaming_downloader.py
import os
import subprocess
import logging
from datetime import datetime
import argparse
import json
from ..utils.streaming_base import StreamingBase
from ..utils.state_manager import StateManager

class StreamingDownloader(StreamingBase):
    def __init__(self, base_dir, entity_types=None):
        """
        Initialize the StreamingDownloader.

        :param base_dir: Base directory for ETL process
        :param entity_types: List of entity types to download (optional)
        """
        super().__init__(base_dir, "downloader")
        self.state_manager = StateManager(base_dir)
        self.entity_types = entity_types or [
            'works', 'authors', 'sources', 'institutions', 
            'domains', 'fields', 'subfields', 'topics', 
            'publishers'
        ]
        
        # Logging setup
        log_dir = os.path.join(base_dir, 'logs', 'download')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f'download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def get_latest_date_folder(self, entity_type):
        """
        Get the latest updated_date folder for an entity type.

        :param entity_type: Type of entity to download
        :return: Latest date folder or None
        """
        try:
            cmd = [
                "aws", "s3", "ls", "--no-sign-request",
                f"s3://openalex/data/{entity_type}/"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Filter and sort updated_date folders
            date_folders = [
                line.split()[-1] for line in result.stdout.splitlines()
                if line.strip().endswith('/') and 'updated_date=' in line
            ]
            
            if not date_folders:
                logging.warning(f"No updated_date folders found for {entity_type}")
                return None
                
            latest_folder = sorted(date_folders)[-1]
            logging.info(f"Latest folder for {entity_type}: {latest_folder}")
            return latest_folder
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error getting latest date folder for {entity_type}: {str(e)}")
            return None

    def list_s3_files(self, entity_type):
        """
        List files for an entity type in the latest updated_date folder.

        :param entity_type: Type of entity to list files for
        :return: List of files to download
        """
        latest_folder = self.get_latest_date_folder(entity_type)
        if not latest_folder:
            return []

        try:
            cmd = [
                "aws", "s3", "ls", "--no-sign-request",
                f"s3://openalex/data/{entity_type}/{latest_folder}"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            files = []
            for line in result.stdout.splitlines():
                if line.strip().endswith('.gz'):
                    file_name = line.split()[-1]
                    file_info = {
                        'folder': latest_folder,
                        'name': file_name,
                        'full_path': f"{latest_folder}{file_name}"
                    }
                    # Skip if already processed
                    if not self.state_manager.is_file_processed(entity_type, file_info['full_path']):
                        files.append(file_info)
            
            logging.info(f"Found {len(files)} new files to download for {entity_type}")
            return files
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error listing files for {entity_type}: {str(e)}")
            return []

    def download_file(self, entity_type, file_info, max_retries=3):
        """
        Download a single file from S3.

        :param entity_type: Type of entity being downloaded
        :param file_info: Dictionary with file information
        :param max_retries: Maximum number of download retries
        :return: Path to downloaded file or None
        """
        temp_file = os.path.join(self.temp_dir, file_info['name'])
        s3_path = f"s3://openalex/data/{entity_type}/{file_info['folder']}{file_info['name']}"
        
        for attempt in range(max_retries):
            try:
                cmd = ["aws", "s3", "cp", "--no-sign-request", s3_path, temp_file]
                logging.info(f"Downloading (attempt {attempt+1}): {s3_path}")
                
                subprocess.run(cmd, check=True)
                logging.info(f"Successfully downloaded to {temp_file}")
                return temp_file
                
            except subprocess.CalledProcessError as e:
                logging.warning(f"Download error attempt {attempt+1}: {str(e)}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logging.error(f"Failed to download {s3_path} after {max_retries} attempts")
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    return None

    def process_entity(self, entity_type, processor_callback=None):
        """
        Download and process files for a specific entity type.

        :param entity_type: Type of entity to process
        :param processor_callback: Optional callback for processing downloaded files
        :return: True if successful, False otherwise
        """
        # Check if entity is already completed
        if self.state_manager.is_entity_completed(entity_type):
            logging.info(f"Entity {entity_type} already completed. Skipping.")
            return True

        # List files to download
        files = self.list_s3_files(entity_type)
        if not files:
            logging.error(f"No files found for {entity_type}")
            return False

        # Process each file
        for i, file_info in enumerate(files, 1):
            logging.info(f"Processing file {i}/{len(files)}: {file_info['name']}")
            
            # Save current state
            self.state_manager.save_state(entity_type, file_info['full_path'])
            
            # Download file
            temp_file = self.download_file(entity_type, file_info)
            if temp_file:
                try:
                    # Process file if callback is provided
                    if processor_callback:
                        if processor_callback(temp_file):
                            self.state_manager.mark_file_complete(entity_type, file_info['full_path'])
                        else:
                            logging.error(f"Processing failed for {file_info['name']}")
                            return False
                    
                    # Clean up temporary file
                    self.cleanup_temp_file(temp_file)
                except Exception as e:
                    logging.error(f"Error processing {file_info['name']}: {str(e)}")
                    return False
            else:
                logging.error(f"Failed to download {file_info['name']}")
                return False

        # Mark entity as completed
        self.state_manager.mark_entity_complete(entity_type)
        return True

def main():
    """
    Command-line interface for the streaming downloader.
    """
    parser = argparse.ArgumentParser(description='Download OpenAlex data files')
    parser.add_argument(
        '--base-dir', 
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help='Base directory for ETL process'
    )
    parser.add_argument(
        '--entity-type', 
        choices=['works', 'authors', 'sources', 'institutions', 
                 'domains', 'fields', 'subfields', 'topics', 
                 'publishers'],
        help='Specific entity type to download'
    )
    parser.add_argument(
        '--all', 
        action='store_true',
        help='Download all entity types'
    )

    args = parser.parse_args()

    # Determine entity types to download
    entity_types = []
    if args.all:
        entity_types = [
            'works', 'authors', 'sources', 'institutions', 
            'domains', 'fields', 'subfields', 'topics', 
            'publishers'
        ]
    elif args.entity_type:
        entity_types = [args.entity_type]
    else:
        parser.error("Please specify either --entity-type or --all")

    # Initialize and run downloader
    downloader = StreamingDownloader(args.base_dir, entity_types)
    
    # Process each entity type
    for entity_type in entity_types:
        success = downloader.process_entity(entity_type)
        if not success:
            logging.error(f"Failed to download {entity_type}")

if __name__ == "__main__":
    main()
