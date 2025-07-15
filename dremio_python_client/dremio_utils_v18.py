# dremio_jdbc_v18.py
import os
import jaydebeapi
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime
import shutil
import argparse


def human_readable_size(size, decimal_places=2):
    """
    Convert a size in bytes to a human-readable string with the nearest unit.
    Returns a string like '1.23 MB'.
    """
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024.0


# Configuration (update as needed)
HOST = "localhost"
PORT = 31010  # Default JDBC port for Dremio
USER = "admin"
PASSWORD = "admin1234"
OUTPUT_PATH = "./data/local/export.parquet"
CHUNK_SIZE = 10000  # Tune based on memory and performance

# JDBC driver details
JDBC_DRIVER_CLASS = "com.dremio.jdbc.Driver"
JDBC_JAR = "./jdbc-driver/dremio-jdbc-driver-18.0.0-202109101536100970-a32fc9f4.jar"  # Update with actual path
JDBC_URL = f"jdbc:dremio:direct={HOST}:{PORT}"

parser = argparse.ArgumentParser(description="Export Dremio SQL results to Parquet.")
parser.add_argument("--sql-file", type=str, default="all", help="Specific SQL file to run (from src/), or 'all' to run all .sql files in src/")
args = parser.parse_args()

if args.sql_file == "all":
    sql_files_to_run = [f for f in os.listdir('./src') if f.endswith('.sql')]
else:
    sql_files_to_run = [args.sql_file] if args.sql_file.endswith('.sql') else [args.sql_file + '.sql']


start_time = time.time()
start_dt = datetime.now()


# Read all SQL files from src folder
SRC_SQL_DIR = './src'
TGT_SQL_DIR = './tgt'

for sql_file in sql_files_to_run: #os.listdir(SRC_SQL_DIR):
    if sql_file.endswith('.sql'):
        OUTPUT_PATH = "./data/local/{SQL_FILE_NAME}.parquet".format(SQL_FILE_NAME=sql_file[:-4])  # Use the SQL file name for the output Parquet file
        sql_path = os.path.join(SRC_SQL_DIR, sql_file)
        with open(sql_path, 'r') as f:
            SQL = f.read()
        print(f"Running query from: {sql_file}")
        # Connect to Dremio via JDBC
        conn = jaydebeapi.connect(
            JDBC_DRIVER_CLASS,
            JDBC_URL,
            [USER, PASSWORD],
            JDBC_JAR
        )
        # Stream query results in chunks and write to Parquet incrementally
        first_chunk = True
        writer = None
        total_rows = 0
        sample_rows = []
        job_status = 'FAILED'
        try:
            for chunk in pd.read_sql(SQL, conn, chunksize=CHUNK_SIZE):
                table = pa.Table.from_pandas(chunk)
                if first_chunk:
                    writer = pq.ParquetWriter(OUTPUT_PATH, table.schema, compression="snappy")
                    first_chunk = False
                writer.write_table(table)
                total_rows += len(chunk)
                if len(sample_rows) < 10:
                    sample_rows.extend(chunk.head(10 - len(sample_rows)).to_dict(orient="records"))
            if writer:
                writer.close()
            job_status = 'SUCCEEDED'
        except Exception as e:
            print(f"Error running query from {sql_file}: {e}")
        finally:
            conn.close()
        print(f"Job status for {sql_file}: {job_status}")
        if job_status == 'SUCCEEDED':
            tgt_path = os.path.join(TGT_SQL_DIR, sql_file)
            shutil.move(sql_path, tgt_path)
            print(f"Moved {sql_file} to {TGT_SQL_DIR}")
        # Output file info
        if job_status == 'SUCCEEDED':
            file_size = os.path.getsize(OUTPUT_PATH)
            print(f"Export complete: {OUTPUT_PATH}")
            print(f"File size: {file_size} bytes ({human_readable_size(file_size)})")
            print(f"Dremio URL: {JDBC_URL}")
            print(f"User ID: {USER}")
            print(f"Start time: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
            print(f"Row count: {total_rows} rows Fetched")
            print("Sample 10 rows:", pd.DataFrame(sample_rows))
