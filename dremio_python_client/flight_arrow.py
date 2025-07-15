import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.flight as flight
import pandas as pd
import time
from datetime import datetime
import shutil
import argparse

def human_readable_size(size, decimal_places=2):
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024.0

# Configuration (update as needed)
HOST = "localhost"
PORT = 32010  # Default Flight port for Dremio
USER = "admin"
PASSWORD = "admin1234"
OUTPUT_PATH = "./data/local/export.parquet"
CHUNK_SIZE = 10000

parser = argparse.ArgumentParser(description="Export Dremio SQL results to Parquet using Flight Arrow.")
parser.add_argument("--sql-file", type=str, default="all", help="Specific SQL file to run (from src/), or 'all' to run all .sql files in src/")
args = parser.parse_args()

if args.sql_file == "all":
    sql_files_to_run = [f for f in os.listdir('./src') if f.endswith('.sql')]
else:
    sql_files_to_run = [args.sql_file] if args.sql_file.endswith('.sql') else [args.sql_file + '.sql']

start_time = time.time()
start_dt = datetime.now()

SRC_SQL_DIR = './src'
TGT_SQL_DIR = './tgt'

for sql_file in sql_files_to_run:
    if sql_file.endswith('.sql'):
        output_path = f"./data/local/{sql_file[:-4]}.parquet"
        sql_path = os.path.join(SRC_SQL_DIR, sql_file)
        with open(sql_path, 'r') as f:
            SQL = f.read()
        print(f"Running query from: {sql_file}")
        client = flight.FlightClient(f"grpc+tcp://{HOST}:{PORT}")
        headers = [(b'authorization', f'Basic {flight.basic_auth_header(USER, PASSWORD)}'.encode())]
        job_status = 'FAILED'
        total_rows = 0
        sample_rows = []
        try:
            # Get FlightInfo for the SQL query
            flight_desc = flight.FlightDescriptor.for_command(SQL)
            info = client.get_flight_info(flight_desc, headers=headers)
            # Retrieve results
            reader = client.do_get(info.endpoints[0].ticket, headers=headers)
            writer = None
            first_chunk = True
            for batch in reader:
                table = pa.Table.from_batches([batch])
                df = table.to_pandas()
                if first_chunk:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
                    first_chunk = False
                writer.write_table(table)
                total_rows += len(df)
                if len(sample_rows) < 10:
                    sample_rows.extend(df.head(10 - len(sample_rows)).to_dict(orient="records"))
            if writer:
                writer.close()
            job_status = 'SUCCEEDED'
        except Exception as e:
            print(f"Error running query from {sql_file}: {e}")
        print(f"Job status for {sql_file}: {job_status}")
        if job_status == 'SUCCEEDED':
            tgt_path = os.path.join(TGT_SQL_DIR, sql_file)
            shutil.move(sql_path, tgt_path)
            print(f"Moved {sql_file} to {TGT_SQL_DIR}")
            file_size = os.path.getsize(output_path)
            print(f"Export complete: {output_path}")
            print(f"File size: {file_size} bytes ({human_readable_size(file_size)})")
            print(f"Dremio Flight URL: grpc+tcp://{HOST}:{PORT}")
            print(f"User ID: {USER}")
            print(f"Start time: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
            print(f"Row count: {total_rows} rows Fetched")
            print("Sample 10 rows:", pd.DataFrame(sample_rows))
