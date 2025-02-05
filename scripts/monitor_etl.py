# scripts/monitor_etl.py
import os
import sys
import json
import psutil
import logging
import argparse
from datetime import datetime
import psycopg2
import traceback
from configs.database.db_config import DB_CONFIG

class ETLMonitor:
    def __init__(self, base_dir, db_config=None):
        """
        Initialize ETL Monitor.

        :param base_dir: Base directory of the ETL project
        :param db_config: Database configuration (optional)
        """
        self.base_dir = base_dir
        self.db_config = db_config or DB_CONFIG
        self.setup_logging()

    def setup_logging(self):
        """
        Set up logging configuration.
        """
        log_dir = os.path.join(self.base_dir, 'logs', 'monitoring')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'monitor_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def get_process_info(self):
        """
        Get ETL process information.

        :return: Dictionary with process details or None
        """
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if ('python' in proc.info['name'] and 
                        'run_streaming_etl.py' in ' '.join(proc.info.get('cmdline', []))):
                        return {
                            'pid': proc.info['pid'],
                            'memory_percent': proc.memory_percent(),
                            'cpu_percent': proc.cpu_percent(),
                            'status': proc.status()
                        }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return None
        except Exception as e:
            logging.error(f"Error getting process info: {e}")
            logging.error(traceback.format_exc())
            return None

    def get_disk_usage(self):
        """
        Get disk usage information for the base directory.

        :return: Dictionary with disk usage details
        """
        try:
            disk_usage = psutil.disk_usage(self.base_dir)
            return {
                'total': disk_usage.total / (1024**3),  # GB
                'used': disk_usage.used / (1024**3),
                'free': disk_usage.free / (1024**3),
                'percent': disk_usage.percent
            }
        except Exception as e:
            logging.error(f"Error getting disk usage: {e}")
            logging.error(traceback.format_exc())
            return {}

    def get_database_stats(self):
        """
        Get PostgreSQL database statistics for OpenAlex schema.

        :return: List of database statistics
        """
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            schemaname, 
                            relname, 
                            n_live_tup AS row_count,
                            pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS size,
                            schemaname = 'openalex' AS is_openalex_schema
                        FROM pg_stat_user_tables 
                        ORDER BY n_live_tup DESC
                    """)
                    return cur.fetchall()
        except Exception as e:
            logging.error(f"Error getting database stats: {str(e)}")
            logging.error(traceback.format_exc())
            return []

    def get_etl_state(self):
        """
        Read current ETL state from state file.

        :return: Dictionary with ETL state
        """
        try:
            state_file = os.path.join(self.base_dir, 'data', 'state', 'ingestion_state.json')
            with open(state_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading state file: {str(e)}")
            logging.error(traceback.format_exc())
            return {}

    def check_logs_for_errors(self, log_dir=None):
        """
        Check recent ETL logs for errors.

        :param log_dir: Optional custom log directory
        :return: List of recent errors
        """
        if not log_dir:
            log_dir = os.path.join(self.base_dir, 'logs', 'etl')
        
        errors = []
        try:
            # Find the most recent log file
            log_files = [
                os.path.join(log_dir, f) for f in os.listdir(log_dir)
                if f.endswith('.log')
            ]
            
            if not log_files:
                return errors

            latest_log = max(log_files, key=os.path.getctime)
            
            with open(latest_log, 'r') as f:
                # Capture last 50 errors
                for line in f.readlines()[-50:]:
                    if 'ERROR' in line:
                        errors.append(line.strip())
        except Exception as e:
            logging.error(f"Error checking logs: {str(e)}")
            logging.error(traceback.format_exc())
        
        return errors

    def print_report(self):
        """
        Generate and print a comprehensive ETL monitoring report.
        """
        print("\n=== OpenAlex ETL Monitoring Report ===")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process Information
        print("\n--- Process Information ---")
        proc_info = self.get_process_info()
        if proc_info:
            print(f"PID: {proc_info['pid']}")
            print(f"Memory Usage: {proc_info['memory_percent']:.2f}%")
            print(f"CPU Usage: {proc_info['cpu_percent']:.2f}%")
            print(f"Status: {proc_info['status']}")
        else:
            print("ETL Process not found!")

        # Disk Usage
        print("\n--- Disk Usage ---")
        disk_info = self.get_disk_usage()
        print(f"Total: {disk_info.get('total', 0):.2f} GB")
        print(f"Used: {disk_info.get('used', 0):.2f} GB ({disk_info.get('percent', 0)}%)")
        print(f"Free: {disk_info.get('free', 0):.2f} GB")

        # Database Statistics
        print("\n--- Database Statistics ---")
        db_stats = self.get_database_stats()
        for stats in db_stats:
            print(f"{stats[1]}: {stats[2]} rows ({stats[3]})")

        # ETL State
        print("\n--- ETL State ---")
        state = self.get_etl_state()
        for entity_type, entity_state in state.get('entities', {}).items():
            print(f"{entity_type.capitalize()}:")
            print(f"  Status: {entity_state.get('status', 'N/A')}")
            print(f"  Last Processed File: {entity_state.get('current_file', 'N/A')}")
            print(f"  Completed Files: {len(entity_state.get('completed_files', []))}")

        # Recent Errors
        errors = self.check_logs_for_errors()
        if errors:
            print("\n--- Recent Errors ---")
            for error in errors:
                print(error)

def main():
    """
    Command-line interface for ETL monitoring.
    """
    parser = argparse.ArgumentParser(description='Monitor OpenAlex ETL process')
    parser.add_argument(
        '--base-dir', 
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help='Base directory of the ETL project'
    )
    parser.add_argument(
        '--continuous', 
        action='store_true',
        help='Run monitoring continuously'
    )
    parser.add_argument(
        '--interval', 
        type=int, 
        default=300,
        help='Monitoring interval in seconds (default: 300)'
    )
    parser.add_argument(
        '--email-alerts', 
        action='store_true',
        help='Enable email alerts for critical issues'
    )
    parser.add_argument(
        '--critical-memory', 
        type=float, 
        default=90.0,
        help='Memory usage threshold for alerts (default: 90.0%%)'
    )
    parser.add_argument(
        '--critical-disk', 
        type=float, 
        default=90.0,
        help='Disk usage threshold for alerts (default: 90.0%%)'
    )

    args = parser.parse_args()

    # Initialize monitor
    monitor = ETLMonitor(args.base_dir)

    # Email alert function (placeholder - can be expanded)
    def send_email_alert(subject, message):
        """
        Send email alert (to be implemented with actual email service).
        
        :param subject: Email subject
        :param message: Email body
        """
        try:
            # Placeholder for email sending logic
            # In a real implementation, use libraries like smtplib or third-party email services
            logging.warning(f"ALERT - {subject}: {message}")
        except Exception as e:
            logging.error(f"Failed to send email alert: {e}")

    # Continuous monitoring loop
    if args.continuous:
        import time
        
        print(f"Starting continuous monitoring. Interval: {args.interval} seconds")
        print("Press Ctrl+C to stop.")
        
        try:
            while True:
                # Generate and print report
                monitor.print_report()
                
                # Check for critical conditions and send alerts
                try:
                    # Memory usage alert
                    proc_info = monitor.get_process_info()
                    if proc_info and proc_info['memory_percent'] > args.critical_memory:
                        send_email_alert(
                            "High Memory Usage",
                            f"ETL Process memory usage is {proc_info['memory_percent']:.2f}% "
                            f"(Threshold: {args.critical_memory}%)"
                        )
                    
                    # Disk usage alert
                    disk_info = monitor.get_disk_usage()
                    if disk_info.get('percent', 0) > args.critical_disk:
                        send_email_alert(
                            "High Disk Usage",
                            f"Disk usage is {disk_info.get('percent', 0)}% "
                            f"(Threshold: {args.critical_disk}%)"
                        )
                    
                    # Check for recent errors
                    errors = monitor.check_logs_for_errors()
                    if errors:
                        send_email_alert(
                            "ETL Errors Detected",
                            f"Found {len(errors)} recent errors. Check logs for details."
                        )
                
                except Exception as e:
                    logging.error(f"Error in continuous monitoring checks: {e}")
                
                # Wait for the specified interval
                time.sleep(args.interval)
        
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user.")
    else:
        # Single run mode
        monitor.print_report()

if __name__ == "__main__":
    main()
