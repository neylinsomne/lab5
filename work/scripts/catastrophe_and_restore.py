"""
Catastrophic Event Simulation & Recovery Script.

This script demonstrates:
1. Shows current data status
2. Creates a final backup BEFORE the catastrophe
3. Simulates a catastrophic DELETE (removes data between timestep 4000-6000)
4. Shows the damage (how many rows were lost)
5. Restores from the latest backup
6. Compares recovered vs lost data

Usage: python catastrophe_and_restore.py
"""
import pyodbc
import os
import sys
from datetime import datetime

SQL_HOST = os.environ.get("SQL_HOST", "localhost")
SQL_PORT = os.environ.get("SQL_PORT", "1433")
SQL_USER = os.environ.get("SQL_USER", "sa")
SQL_PASS = os.environ.get("SQL_PASS", "Lab05Pass1")
SQL_DB = os.environ.get("SQL_DB", "sumo_traffic")
BACKUP_DIR = "/var/opt/backups"

def get_conn(db="master"):
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"DATABASE={db};"
        f"UID={SQL_USER};PWD={SQL_PASS};"
        f"TrustServerCertificate=yes;",
        autocommit=True
    )

def show_status(conn):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            MIN(timestep) as min_ts,
            MAX(timestep) as max_ts,
            COUNT(DISTINCT vehicle_id) as unique_vehicles
        FROM [{SQL_DB}].dbo.vehicle_positions
    """)
    row = cur.fetchone()
    print(f"  Total rows:       {row[0]:,}")
    print(f"  Timestep range:   {row[1]:.0f}s - {row[2]:.0f}s")
    print(f"  Unique vehicles:  {row[3]:,}")
    cur.close()
    return row[0]

def count_in_range(conn, ts_start, ts_end):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM [{SQL_DB}].dbo.vehicle_positions
        WHERE timestep >= {ts_start} AND timestep <= {ts_end}
    """)
    count = cur.fetchone()[0]
    cur.close()
    return count

def main():
    print("=" * 60)
    print("  CATASTROPHIC EVENT SIMULATION & RECOVERY")
    print("=" * 60)

    conn = get_conn("master")

    # Step 1: Show current status
    print("\n[STEP 1] Current database status:")
    total_before = show_status(conn)

    ts_start, ts_end = 4000, 6000
    rows_in_range = count_in_range(conn, ts_start, ts_end)
    print(f"\n  Rows in danger zone (timestep {ts_start}-{ts_end}): {rows_in_range:,}")

    # Step 2: Create a pre-catastrophe backup
    print(f"\n[STEP 2] Creating pre-catastrophe FULL backup...")
    label = datetime.now().strftime("%Y%m%d_%H%M%S")
    pre_backup = f"{BACKUP_DIR}/sumo_traffic_pre_catastrophe_{label}.bak"
    cur = conn.cursor()
    cur.execute(f"BACKUP DATABASE [{SQL_DB}] TO DISK = '{pre_backup}' WITH INIT, FORMAT")
    cur.close()
    print(f"  Backup saved to: {pre_backup}")

    # Step 3: CATASTROPHE - Delete data
    print(f"\n[STEP 3] !!! CATASTROPHIC EVENT !!!")
    print(f"  Executing: DELETE FROM vehicle_positions WHERE timestep BETWEEN {ts_start} AND {ts_end}")

    db_conn = get_conn(SQL_DB)
    cur = db_conn.cursor()
    cur.execute(f"DELETE FROM vehicle_positions WHERE timestep >= {ts_start} AND timestep <= {ts_end}")
    deleted = cur.rowcount
    cur.close()
    db_conn.close()

    print(f"  ROWS DELETED: {deleted:,}")

    # Step 4: Show damage
    print(f"\n[STEP 4] Post-catastrophe status:")
    total_after = show_status(conn)
    print(f"\n  Data loss: {total_before - total_after:,} rows ({(total_before - total_after) / total_before * 100:.1f}%)")
    remaining_in_range = count_in_range(conn, ts_start, ts_end)
    print(f"  Rows remaining in range {ts_start}-{ts_end}: {remaining_in_range:,}")

    # Step 5: Restore from backup
    print(f"\n[STEP 5] RESTORING from pre-catastrophe backup...")
    print(f"  Setting database to SINGLE_USER mode...")

    cur = conn.cursor()
    cur.execute(f"ALTER DATABASE [{SQL_DB}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
    print(f"  Restoring from: {pre_backup}")
    cur.execute(f"RESTORE DATABASE [{SQL_DB}] FROM DISK = '{pre_backup}' WITH REPLACE")
    cur.execute(f"ALTER DATABASE [{SQL_DB}] SET MULTI_USER")
    cur.close()
    print(f"  Restore completed!")

    # Step 6: Verify recovery
    print(f"\n[STEP 6] Post-recovery status:")
    total_recovered = show_status(conn)
    recovered_in_range = count_in_range(conn, ts_start, ts_end)

    print(f"\n{'=' * 60}")
    print(f"  RECOVERY ANALYSIS")
    print(f"{'=' * 60}")
    print(f"  Rows before catastrophe:  {total_before:,}")
    print(f"  Rows after catastrophe:   {total_after:,}")
    print(f"  Rows after recovery:      {total_recovered:,}")
    print(f"  Rows recovered:           {total_recovered - total_after:,}")
    print(f"  Recovery rate:            {(total_recovered / total_before) * 100:.1f}%")
    print(f"  Range {ts_start}-{ts_end} recovered: {recovered_in_range:,} / {rows_in_range:,} rows")
    print(f"{'=' * 60}")

    conn.close()

if __name__ == "__main__":
    main()
