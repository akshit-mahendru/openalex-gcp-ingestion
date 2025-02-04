# scripts/etl/download/streaming_downloader.py
import os
import subprocess
import logging
from ..utils.streaming_base import StreamingBase

class StreamingDownloader(StreamingBase):
    def __init__(self, base_dir):
        super().__init__(base_dir, "downloader")
        self.manifest_file = os.path.join(self.temp_dir, 'manifest.txt')

    def list_s3_files(self, entity_type):
        """List all files for an entity type in OpenAlex S3."""
        cmd = ["aws", "s3", "ls", "--no-sign-request",
               f"s3://openalex/data/{entity_type}/updated_date=2024-01-*"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            files = [line.split()[-1] for line in result.stdout.splitlines()]
            logging.info(f"Found {len(files)} files for {entity_type}")
            return files
        except subprocess.CalledProcessError as e:
            logging.error(f"Error listing S3 files: {str(e)}")
            return []

    def download_file(self, entity_type, file_name):
        """Download a single file from S3."""
        temp_file = os.path.join(self.temp_dir, file_name)
        s3_path = f"s3://openalex/data/{entity_type}/updated_date=2024-01-*/{file_name}"
        
        try:
            cmd = ["aws", "s3", "cp", "--no-sign-request", s3_path, temp_file]
            subprocess.run(cmd, check=True)
            logging.info(f"Downloaded {file_name}")
            return temp_file
        except subprocess.CalledProcessError as e:
            logging.error(f"Error downloading {file_name}: {str(e)}")
            return None

    def process_entity(self, entity_type, processor_callback):
        """Process all files for an entity type."""
        files = self.list_s3_files(entity_type)
        if not files:
            logging.error(f"No files found for {entity_type}")
            return False

        for i, file_name in enumerate(files, 1):
            logging.info(f"Processing file {i}/{len(files)}: {file_name}")
            temp_file = self.download_file(entity_type, file_name)
            
            if temp_file:
                try:
                    processor_callback(temp_file)
                    self.cleanup_temp_file(temp_file)
                except Exception as e:
                    logging.error(f"Error processing {file_name}: {str(e)}")
                    self.cleanup_temp_file(temp_file)
                    return False

        return True