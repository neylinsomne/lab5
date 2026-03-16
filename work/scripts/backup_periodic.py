"""
Periodic backup script for SQL Server.
Creates FULL backups at regular intervals while SUMO data is being ingested.
This demonstrates the B&R configuration requirement of the lab.

Usage: python backup_periodic.py [interval_seconds]
  Default interval: 120 seconds (2 minutes)
"""
import pyodbc
import time
import os
import sys
from datetime import datetime

SQL_HOST = os.environ.get("SQL_HOST", "localhost")
SQL_PORT = os.environ.get("SQL_PORT", "1433")
SQL_USER = os.environ.get("SQL_USER", "sa")
SQL_PASS = os.environ.get("SQL_PASS", "Lab05Pass1")
SQL_DB = os.environ.get("SQL_DB", "sumo_traffic")
BACKUP_DIR = "/var/opt/backups"
INTERVAL = int(sys.argv[1]) if len(sys.argv) > 1 else 120

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"DATABASE=master;"
        f"UID={SQL_USER};PWD={SQL_PASS};"
        f"TrustServerCertificate=yes;",
        autocommit=True
    )

def do_full_backup(conn, label):
    cursor = conn.cursor()
    filename = f"{BACKUP_DIR}/sumo_traffic_full_{label}.bak"
    print(f"[{datetime.now()}] Starting FULL backup -> {filename}")
    cursor.execute(f"BACKUP DATABASE [{SQL_DB}] TO DISK = '{filename}' WITH INIT, FORMAT")
    cursor.close()
    print(f"[{datetime.now()}] FULL backup completed: {filename}")
    return filename

def do_log_backup(conn, label):
    cursor = conn.cursor()
    filename = f"{BACKUP_DIR}/sumo_traffic_log_{label}.trn"
    print(f"[{datetime.now()}] Starting LOG backup -> {filename}")
    cursor.execute(f"BACKUP LOG [{SQL_DB}] TO DISK = '{filename}' WITH INIT")
    cursor.close()
    print(f"[{datetime.now()}] LOG backup completed: {filename}")
    return filename

def get_row_count(conn):
    c = conn.cursor()
    c.execute(f"SELECT COUNT(*) FROM [{SQL_DB}].dbo.vehicle_positions")
    count = c.fetchone()[0]
    c.close()
    return count

def main():
    print(f"=== Periodic Backup Service ===")
    print(f"Database: {SQL_DB}")
    print(f"Interval: {INTERVAL} seconds")
    print(f"Backup dir: {BACKUP_DIR}")
    print()

    conn = get_conn()
    backup_num = 0

    try:
        while True:
            backup_num += 1
            label = datetime.now().strftime("%Y%m%d_%H%M%S")
            rows = get_row_count(conn)
            print(f"\n--- Backup #{backup_num} | Rows in table: {rows} ---")

            # Alternate between full and log backups
            # Full every 3rd backup, log otherwise
            if backup_num % 3 == 1:
                do_full_backup(conn, label)
            else:
                do_log_backup(conn, label)

            print(f"Sleeping {INTERVAL}s until next backup...")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\nBackup service stopped.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
