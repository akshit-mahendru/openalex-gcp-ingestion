# scripts/etl/utils/state_manager.py
import os
import json
import logging
import shutil
from datetime import datetime
from typing import Dict, Any, Optional

class StateManager:
    def __init__(self, base_dir: str):
        """
        Initialize StateManager with base directory.

        :param base_dir: Base directory for the ETL process """
        self.base_dir = base_dir
        self.state_dir = os.path.join(base_dir, 'data', 'state')
        self.state_file = os.path.join(self.state_dir, 'ingestion_state.json')
        self.backup_dir = os.path.join(self.state_dir, 'backups')
        self.setup_directories()

    def setup_directories(self):
        """Create necessary directories if they don't exist."""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)

    def _load_state(self) -> Dict[str, Any]:
        """
        Load current state from file.

        :return: State dictionary
        """
        if not os.path.exists(self.state_file):
            return self._create_initial_state()
        
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Error loading state file: {e}")
            # Try to restore from backup
            return self._restore_from_backup()

    def _save_state(self, state: Dict[str, Any]):
        """
        Save state to file with backup.

        :param state: State dictionary to save
        """
        # Create backup before saving
        self._backup_state(state)
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except IOError as e:
            logging.error(f"Error saving state file: {e}")

    def _create_initial_state(self) -> Dict[str, Any]:
        """
        Create initial state structure.

        :return: Initial state dictionary
        """
        initial_state = {
            'version': '1.0',
            'last_updated': datetime.now().isoformat(),
            'entities': {},
            'total_processed': {
                'works_count': 0,
                'authors_count': 0,
                'sources_count': 0,
                'institutions_count': 0,
                'domains_count': 0,
                'fields_count': 0,
                'subfields_count': 0,
                'topics_count': 0,
                'publishers_count': 0
            },
            'error_log': []
        }
        
        # Save initial state
        self._save_state(initial_state)
        return initial_state

    def _backup_state(self, state: Dict[str, Any]):
        """
        Create a backup of the current state.

        :param state: State dictionary to backup
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(self.backup_dir, f'state_backup_{timestamp}.json')
        
        try:
            with open(backup_file, 'w') as f:
                json.dump(state, f, indent=2)
        except IOError as e:
            logging.error(f"Error creating backup: {e}")
        
        # Keep only last 10 backups
        backups = sorted(os.listdir(self.backup_dir))
        if len(backups) > 10:
            for old_backup in backups[:-10]:
                os.remove(os.path.join(self.backup_dir, old_backup))

    def _restore_from_backup(self) -> Dict[str, Any]:
        """
        Restore state from the most recent backup.

        :return: Restored state or initial state
        """
        backups = sorted(os.listdir(self.backup_dir))
        if not backups:
            return self._create_initial_state()
        
        try:
            latest_backup = os.path.join(self.backup_dir, backups[-1])
            with open(latest_backup, 'r') as f:
                state = json.load(f)
            
            # Copy backup to main state file
            shutil.copy2(latest_backup, self.state_file)
            return state
        except Exception as e:
            logging.error(f"Error restoring from backup: {e}")
            return self._create_initial_state()

    def save_state(self, entity_type: str, current_file: str, status: str = 'in_progress'):
        """
        Save current processing state for a specific entity type.

        :param entity_type: Type of entity being processed
        :param current_file: Current file being processed
        :param status: Current status of processing
        """
        state = self._load_state()
        
        # Update entity-specific state
        if entity_type not in state['entities']:
            state['entities'][entity_type] = {}
        
        state['entities'][entity_type].update({
            'current_file': current_file,
            'last_processed': datetime.now().isoformat(),
            'status': status
        })
        
        state['last_updated'] = datetime.now().isoformat()
        
        self._save_state(state)
        logging.info(f"Updated state for {entity_type}: {current_file}")

    def mark_file_complete(self, entity_type: str, file_path: str):
        """
        Mark a file as completely processed.

        :param entity_type: Type of entity
        :param file_path: Path of the processed file
        """
        state = self._load_state()
        
        # Ensure entity exists in state
        if entity_type not in state['entities']:
            state['entities'][entity_type] = {}
        
        # Initialize completed files list if not exists
        if 'completed_files' not in state['entities'][entity_type]:
            state['entities'][entity_type]['completed_files'] = []
        
        # Add file to completed list if not already present
        if file_path not in state['entities'][entity_type]['completed_files']:
            state['entities'][entity_type]['completed_files'].append(file_path)
        
        # Update total processed count
        count_key = f'{entity_type}_count'
        if count_key in state['total_processed']:
            state['total_processed'][count_key] += 1
        
        state['last_updated'] = datetime.now().isoformat()
        
        self._save_state(state)
        logging.info(f"Marked file complete: {file_path}")

    def mark_entity_complete(self, entity_type: str):
        """
        Mark an entire entity type as completely processed.

        :param entity_type: Type of entity to mark complete
        """
        state = self._load_state()
        
        # Ensure entity exists in state
        if entity_type not in state['entities']:
            state['entities'][entity_type] = {}
        
        # Mark entity as complete
        state['entities'][entity_type].update({
            'status': 'completed',
            'completed_at': datetime.now().isoformat()
        })
        
        state['last_updated'] = datetime.now().isoformat()
        
        self._save_state(state)
        logging.info(f"Marked entity type {entity_type} as complete")

    def is_file_processed(self, entity_type: str, file_path: str) -> bool:
        """
        Check if a file has been processed.

        :param entity_type: Type of entity
        :param file_path: Path of the file to check
        :return: True if file is processed, False otherwise
        """
        state = self._load_state()
        
        # Check if entity exists in state
        if entity_type not in state['entities']:
            return False
        
        # Check completed files
        completed_files = state['entities'][entity_type].get('completed_files', [])
        return file_path in completed_files

    def is_entity_completed(self, entity_type: str) -> bool:
        """
        Check if an entire entity type has been completed.

        :param entity_type: Type of entity to check
        :return: True if entity is completed, False otherwise
        """
        state = self._load_state()
        
        # Check if entity exists and is marked as completed
        if entity_type not in state['entities']:
            return False
        
        return state['entities'][entity_type].get('status') == 'completed'

    def log_error(self, entity_type: str, error_message: str):
        """
        Log an error in the state file.

        :param entity_type: Type of entity where error occurred
        :param error_message: Error message to log
        """
        state = self._load_state()
        
        # Initialize error log if not exists
        if 'error_log' not in state:
            state['error_log'] = []
        
        # Add error to log
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'entity_type': entity_type,
            'error_message': error_message
        }
        
        state['error_log'].append(error_entry)
        
        # Limit error log to last 100 entries
        state['error_log'] = state['error_log'][-100:]
        
        self._save_state(state)
        logging.error(f"Logged error for {entity_type}: {error_message}")

    def get_state_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current state.

        :return: Dictionary with state summary
        """
        state = self._load_state()
        
        summary = {
            'last_updated': state.get('last_updated'),
            'total_processed': state.get('total_processed', {}),
            'entities': {}
        }
        
        # Summarize state for each entity
        for entity_type, entity_state in state.get('entities', {}).items():
            summary['entities'][entity_type] = {
                'status': entity_state.get('status', 'not_started'),
                'last_processed_file': entity_state.get('current_file'),
                'completed_files_count': len(entity_state.get('completed_files', []))
            }
        
        return summary

    def reset_entity(self, entity_type: str):
        """
        Reset the state for a specific entity type.

        :param entity_type: Type of entity to reset
        """
        state = self._load_state()
        
        # Remove entity from state
        if entity_type in state['entities']:
            del state['entities'][entity_type]
        
        # Reset processed count
        count_key = f'{entity_type}_count'
        if count_key in state['total_processed']:
            state['total_processed'][count_key] = 0
        
        state['last_updated'] = datetime.now().isoformat()
        
        self._save_state(state)
        logging.info(f"Reset state for entity type {entity_type}")

    def cleanup_old_states(self, days_to_keep: int = 30):
        """
        Clean up old backup states.

        :param days_to_keep: Number of days to keep backup files
        """
        import datetime as dt
        
        backups = os.listdir(self.backup_dir)
        current_time = dt.datetime.now()
        
        for backup in backups:
            try:
                # Extract timestamp from backup filename
                backup_time_str = backup.replace('state_backup_', '').replace('.json', '')
                backup_time = dt.datetime.strptime(backup_time_str, '%Y%m%d_%H%M%S')
                
                # Calculate days since backup
                days_since_backup = (current_time - backup_time).days
                
                # Remove backup if older than specified days
                if days_since_backup > days_to_keep:
                    os.remove(os.path.join(self.backup_dir, backup))
            except Exception as e:
                logging.error(f"Error cleaning up backup {backup}: {e}")
