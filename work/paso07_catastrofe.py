import pyodbc
import time
from datetime import datetime

def get_conn(db="master"):
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;DATABASE={db};UID=sa;PWD=Lab05Pass1;TrustServerCertificate=yes;",
        autocommit=True
    )

conn = get_conn("master")
cur = conn.cursor()

print("=" * 60)
print("  CATASTROPHIC EVENT SIMULATION & RECOVERY")
print("=" * 60)

# STEP 1: Status ANTES
print("\n[STEP 1] Current database status:")
cur.execute("""
    SELECT COUNT(*), MIN(timestep), MAX(timestep), COUNT(DISTINCT vehicle_id)
    FROM [sumo_traffic].dbo.vehicle_positions
""")
row = cur.fetchone()
total_before = row[0]
print(f"  Total rows:       {total_before:,}")
print(f"  Timestep range:   {row[1]:.0f}s - {row[2]:.0f}s")
print(f"  Unique vehicles:  {row[3]:,}")

ts_start, ts_end = 4000, 6000
cur.execute(f"SELECT COUNT(*) FROM [sumo_traffic].dbo.vehicle_positions WHERE timestep >= {ts_start} AND timestep <= {ts_end}")
rows_in_range = cur.fetchone()[0]
print(f"\n  Rows in danger zone (timestep {ts_start}-{ts_end}): {rows_in_range:,}")

print("\n" + "=" * 60)
print("  >>> SCREENSHOT #7: ANTES DE CATASTROFE <<<")
print("=" * 60)
input("\nPresiona ENTER para continuar con el backup pre-catastrofe...")

# STEP 2: Backup pre-catastrofe
print(f"\n[STEP 2] Creating pre-catastrophe FULL backup...")
label = datetime.now().strftime("%Y%m%d_%H%M%S")
pre_backup = f"/var/opt/backups/sumo_traffic_pre_catastrophe_{label}.bak"
cur.execute(f"BACKUP DATABASE [sumo_traffic] TO DISK = '{pre_backup}' WITH INIT, FORMAT")
print(f"  Backup saved to: {pre_backup}")

# STEP 3: CATASTROFE
print(f"\n[STEP 3] !!! CATASTROPHIC EVENT !!!")
print(f"  Executing: DELETE FROM vehicle_positions WHERE timestep BETWEEN {ts_start} AND {ts_end}")

db_conn = get_conn("sumo_traffic")
db_cur = db_conn.cursor()
db_cur.execute(f"DELETE FROM vehicle_positions WHERE timestep >= {ts_start} AND timestep <= {ts_end}")
deleted = db_cur.rowcount
db_cur.close()
db_conn.close()

print(f"  ROWS DELETED: {deleted:,}")

# STEP 4: Damage
print(f"\n[STEP 4] Post-catastrophe status:")
cur.execute("""
    SELECT COUNT(*), MIN(timestep), MAX(timestep), COUNT(DISTINCT vehicle_id)
    FROM [sumo_traffic].dbo.vehicle_positions
""")
row = cur.fetchone()
total_after = row[0]
print(f"  Total rows:       {total_after:,}")
print(f"  Timestep range:   {row[1]:.0f}s - {row[2]:.0f}s")
print(f"  Unique vehicles:  {row[3]:,}")
print(f"\n  Data loss: {total_before - total_after:,} rows ({(total_before - total_after) / total_before * 100:.1f}%)")

cur.execute(f"SELECT COUNT(*) FROM [sumo_traffic].dbo.vehicle_positions WHERE timestep >= {ts_start} AND timestep <= {ts_end}")
remaining = cur.fetchone()[0]
print(f"  Rows remaining in range {ts_start}-{ts_end}: {remaining:,}")

print("\n" + "=" * 60)
print("  >>> SCREENSHOT #8: DESPUES DE CATASTROFE (PERDIDA) <<<")
print("=" * 60)
input("\nPresiona ENTER para continuar con la restauracion...")

# STEP 5: RESTORE
print(f"\n[STEP 5] RESTORING from pre-catastrophe backup...")
print(f"  Setting database to SINGLE_USER mode...")
cur.execute("ALTER DATABASE [sumo_traffic] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
print(f"  Restoring from: {pre_backup}")
cur.execute(f"""
    RESTORE DATABASE [sumo_traffic] FROM DISK = '{pre_backup}'
    WITH REPLACE,
    MOVE 'sumo_traffic' TO '/var/opt/mssql/data/sumo_traffic.mdf',
    MOVE 'sumo_traffic_log' TO '/var/opt/mssql/data/sumo_traffic_log.ldf'
""")
# Consume all result sets from RESTORE
while cur.nextset():
    pass
time.sleep(2)
cur.execute("ALTER DATABASE [sumo_traffic] SET MULTI_USER")
print(f"  Restore completed!")

print("\n" + "=" * 60)
print("  >>> SCREENSHOT #9: PROCESO DE RESTORE <<<")
print("=" * 60)
input("\nPresiona ENTER para ver el analisis final...")

# STEP 6: Recovery analysis
print(f"\n[STEP 6] Post-recovery status:")
cur.execute("""
    SELECT COUNT(*), MIN(timestep), MAX(timestep), COUNT(DISTINCT vehicle_id)
    FROM [sumo_traffic].dbo.vehicle_positions
""")
row = cur.fetchone()
total_recovered = row[0]
print(f"  Total rows:       {total_recovered:,}")
print(f"  Timestep range:   {row[1]:.0f}s - {row[2]:.0f}s")
print(f"  Unique vehicles:  {row[3]:,}")

cur.execute(f"SELECT COUNT(*) FROM [sumo_traffic].dbo.vehicle_positions WHERE timestep >= {ts_start} AND timestep <= {ts_end}")
recovered_in_range = cur.fetchone()[0]

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

print("\n  >>> SCREENSHOT #10: ANALISIS DE RECUPERACION <<<")

conn.close()
