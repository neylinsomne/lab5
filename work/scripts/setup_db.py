"""
Setup script: Creates the sumo_traffic database and the vehicle_positions table.
Run this ONCE before starting the ingestion.
"""
import pyodbc
import time
import os

SQL_HOST = os.environ.get("SQL_HOST", "localhost")
SQL_PORT = os.environ.get("SQL_PORT", "1433")
SQL_USER = os.environ.get("SQL_USER", "sa")
SQL_PASS = os.environ.get("SQL_PASS", "Lab05Pass1")
SQL_DB = os.environ.get("SQL_DB", "sumo_traffic")

def get_conn(database="master"):
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"DATABASE={database};"
        f"UID={SQL_USER};PWD={SQL_PASS};"
        f"TrustServerCertificate=yes;",
        autocommit=True
    )

def wait_for_server():
    print("Waiting for SQL Server to be ready...")
    for i in range(30):
        try:
            conn = get_conn()
            conn.close()
            print("SQL Server is ready!")
            return
        except Exception:
            print(f"  Attempt {i+1}/30 - not ready yet...")
            time.sleep(2)
    raise Exception("SQL Server did not become ready in time")

def setup():
    wait_for_server()

    # Create database
    conn = get_conn("master")
    cur = conn.cursor()
    cur.execute(f"""
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{SQL_DB}')
        BEGIN
            CREATE DATABASE [{SQL_DB}]
        END
    """)
    cur.close()
    conn.close()
    print(f"Database '{SQL_DB}' ready.")

    # Create table
    conn = get_conn(SQL_DB)
    cur = conn.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='vehicle_positions' AND xtype='U')
        BEGIN
            CREATE TABLE vehicle_positions (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                timestep FLOAT NOT NULL,
                vehicle_id NVARCHAR(50) NOT NULL,
                x FLOAT NOT NULL,
                y FLOAT NOT NULL,
                speed FLOAT NOT NULL,
                angle FLOAT NOT NULL,
                lane NVARCHAR(100),
                pos FLOAT,
                inserted_at DATETIME2 DEFAULT GETDATE()
            )
        END
    """)

    # Create index on timestep for faster queries and recovery analysis
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_timestep' AND object_id = OBJECT_ID('vehicle_positions'))
        BEGIN
            CREATE INDEX idx_timestep ON vehicle_positions(timestep)
        END
    """)
    cur.close()
    conn.close()
    print("Table 'vehicle_positions' ready with index on timestep.")

    # Set recovery model to FULL (required for point-in-time restore)
    conn = get_conn("master")
    cur = conn.cursor()
    cur.execute(f"ALTER DATABASE [{SQL_DB}] SET RECOVERY FULL")
    cur.close()
    conn.close()
    print(f"Recovery model set to FULL for '{SQL_DB}'.")

    # Create initial full backup (required before log backups work)
    conn = get_conn("master")
    cur = conn.cursor()
    cur.execute(f"BACKUP DATABASE [{SQL_DB}] TO DISK = '/var/opt/backups/sumo_traffic_initial.bak' WITH INIT, FORMAT")
    cur.close()
    conn.close()
    print("Initial FULL backup created at /var/opt/backups/sumo_traffic_initial.bak")

if __name__ == "__main__":
    setup()
