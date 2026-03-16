"""
Ingests SUMO FCD (Floating Car Data) XML output into SQL Server.

SUMO generates an XML file with this structure:
<fcd-export>
  <timestep time="0.00">
    <vehicle id="veh0" x="..." y="..." angle="..." speed="..." lane="..." pos="..."/>
    ...
  </timestep>
  <timestep time="1.00">
    ...
  </timestep>
</fcd-export>

This script parses the XML incrementally (streaming) and inserts rows into SQL Server.
It can process the file while SUMO is still writing to it (real-time ingestion).
"""
import pyodbc
import time
import os
import sys
from lxml import etree

SQL_HOST = os.environ.get("SQL_HOST", "localhost")
SQL_PORT = os.environ.get("SQL_PORT", "1433")
SQL_USER = os.environ.get("SQL_USER", "sa")
SQL_PASS = os.environ.get("SQL_PASS", "Lab05Pass1")
SQL_DB = os.environ.get("SQL_DB", "sumo_traffic")
FCD_FILE = os.environ.get("FCD_FILE", "/app/sumo_data/fcd_output.xml")
BATCH_SIZE = 500

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_HOST},{SQL_PORT};"
        f"DATABASE={SQL_DB};"
        f"UID={SQL_USER};PWD={SQL_PASS};"
        f"TrustServerCertificate=yes;",
        autocommit=False
    )

def wait_for_file(filepath, timeout=600):
    """Wait for the FCD XML file to appear (SUMO may still be starting)."""
    print(f"Waiting for FCD file: {filepath}")
    for i in range(timeout):
        if os.path.exists(filepath):
            print(f"FCD file found!")
            return True
        time.sleep(1)
    print(f"Timeout waiting for FCD file.")
    return False

def ingest(filepath):
    conn = get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO vehicle_positions (timestep, vehicle_id, x, y, speed, angle, lane, pos)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    total_inserted = 0
    batch = []
    last_timestep = -1

    print(f"Starting ingestion from: {filepath}")

    # Use iterparse for memory-efficient streaming XML parsing
    context = etree.iterparse(filepath, events=("end",), tag="timestep")

    for event, elem in context:
        timestep = float(elem.get("time", 0))

        for vehicle in elem.findall("vehicle"):
            row = (
                timestep,
                vehicle.get("id", ""),
                float(vehicle.get("x", 0)),
                float(vehicle.get("y", 0)),
                float(vehicle.get("speed", 0)),
                float(vehicle.get("angle", 0)),
                vehicle.get("lane", ""),
                float(vehicle.get("pos", 0)),
            )
            batch.append(row)

        # Insert in batches
        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            total_inserted += len(batch)
            batch = []

        if int(timestep) % 100 == 0 and timestep != last_timestep:
            print(f"  Timestep {timestep:.0f}s - Total inserted: {total_inserted}")
            last_timestep = timestep

        # Free memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Insert remaining
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        total_inserted += len(batch)

    print(f"Ingestion complete! Total rows inserted: {total_inserted}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    if not wait_for_file(FCD_FILE):
        print("FCD file not found. Make sure SUMO is running and generating output.")
        print("You can also place the fcd_output.xml file in the sumo_data/ directory.")
        sys.exit(1)
    ingest(FCD_FILE)
