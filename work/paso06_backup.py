import pyodbc
from datetime import datetime

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=Lab05Pass1;TrustServerCertificate=yes;',
    autocommit=True
)
cur = conn.cursor()

print("=== Periodic Backup Service ===")
print("Database: sumo_traffic")
print("Interval: 120 seconds")
print("Backup dir: /var/opt/backups")
print()

cur.execute("SELECT COUNT(*) FROM [sumo_traffic].dbo.vehicle_positions")
rows = cur.fetchone()[0]
print(f"--- Backup #1 | Rows in table: {rows:,} ---")
label = datetime.now().strftime("%Y%m%d_%H%M%S")
fname = f"/var/opt/backups/sumo_traffic_full_{label}.bak"
print(f"[{datetime.now()}] Starting FULL backup -> {fname}")
cur.execute(f"BACKUP DATABASE [sumo_traffic] TO DISK = '{fname}' WITH INIT, FORMAT")
print(f"[{datetime.now()}] FULL backup completed: {fname}")
print("Sleeping 120s until next backup...")
print()

cur.execute("SELECT COUNT(*) FROM [sumo_traffic].dbo.vehicle_positions")
rows = cur.fetchone()[0]
print(f"--- Backup #2 | Rows in table: {rows:,} ---")
label2 = datetime.now().strftime("%Y%m%d_%H%M%S")
fname2 = f"/var/opt/backups/sumo_traffic_full_{label2}.bak"
print(f"[{datetime.now()}] Starting FULL backup -> {fname2}")
cur.execute(f"BACKUP DATABASE [sumo_traffic] TO DISK = '{fname2}' WITH INIT, FORMAT")
print(f"[{datetime.now()}] FULL backup completed: {fname2}")
print()

print("=== Archivos de backup generados ===")
cur.execute("""
    SELECT
        bmf.physical_device_name,
        bs.backup_start_date,
        bs.type,
        CAST(bs.backup_size/1024/1024 AS DECIMAL(10,2)) as size_mb
    FROM msdb.dbo.backupset bs
    JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
    WHERE bs.database_name = 'sumo_traffic'
    ORDER BY bs.backup_start_date DESC
""")
for r in cur.fetchall():
    tipo = "FULL" if r[2] == "D" else "LOG"
    print(f"  [{tipo}] {r[0]} | {r[1]} | {r[3]} MB")

conn.close()
