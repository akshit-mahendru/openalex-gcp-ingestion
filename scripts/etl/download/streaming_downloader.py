# scripts/etl/download/streaming_downloader.py
import os
import subprocess
import logging
from ..utils.streaming_base import StreamingBase

class StreamingDownloader(StreamingBase):
    def __init__(self, base_dir):
        super().__init__(base_dir, "downloader")

    def get_latest_date_folder(self, entity_type):
        """Get the latest updated_date folder for an entity type."""
        try:
            cmd = ["aws", "s3", "ls", "--no-sign-request",
                  f"s3://openalex/data/{entity_type}/"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Filter and sort updated_date folders
            date_folders = [
                line.split()[-1] for line in result.stdout.splitlines()
                if line.strip().endswith('/') and 'updated_date=' in line
            ]
            
            if not date_folders:
                logging.error(f"No updated_date folders found for {entity_type}")
                return None
                
            latest_folder = sorted(date_folders)[-1]
            logging.info(f"Latest folder for {entity_type}: {latest_folder}")
            return latest_folder
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error getting latest date folder: {str(e)}")
            return None

    def list_s3_files(self, entity_type):
        """List all files for an entity type in OpenAlex S3."""
        latest_folder = self.get_latest_date_folder(entity_type)
        if not latest_folder:
            return []

        try:
            cmd = ["aws", "s3", "ls", "--no-sign-request",
                  f"s3://openalex/data/{entity_type}/{latest_folder}"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            files = []
            for line in result.stdout.splitlines():
                if line.strip().endswith('.gz'):
                    file_name = line.split()[-1]
                    files.append({
                        'folder': latest_folder,
                        'name': file_name
                    })
            
            logging.info(f"Found {len(files)} files in {latest_folder}")
            return files
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error listing files in folder: {str(e)}")
            return []

    def download_file(self, entity_type, file_info):
        """Download a single file from S3."""
        temp_file = os.path.join(self.temp_dir, file_info['name'])
        s3_path = f"s3://openalex/data/{entity_type}/{file_info['folder']}{file_info['name']}"
        
        try:
            cmd = ["aws", "s3", "cp", "--no-sign-request", s3_path, temp_file]
            logging.info(f"Downloading: {s3_path}")
            
            subprocess.run(cmd, check=True)
            logging.info(f"Successfully downloaded to {temp_file}")
            return temp_file
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error downloading {s3_path}: {str(e)}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return None

    def process_entity(self, entity_type, processor_callback):
        """Process all files for an entity type."""
        files = self.list_s3_files(entity_type)
        if not files:
            logging.error(f"No files found for {entity_type}")
            return False

        for i, file_info in enumerate(files, 1):
            logging.info(f"Processing file {i}/{len(files)}: {file_info['name']}")
            temp_file = self.download_file(entity_type, file_info)
            
            if temp_file:
                try:
                    processor_callback(temp_file)
                    self.cleanup_temp_file(temp_file)
                except Exception as e:
                    logging.error(f"Error processing {file_info['name']}: {str(e)}")
                    self.cleanup_temp_file(temp_file)
                    return False
                    
            else:
                logging.error(f"Failed to download {file_info['name']}")
                return False

        return True
