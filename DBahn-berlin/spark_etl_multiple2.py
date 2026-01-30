import json
from pathlib import Path
from tqdm import tqdm
from datetime import date, datetime, time
from lxml import etree
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import sys
import pandas as pd

# Set environment variables for Java and PySpark
os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROW_COMMIT_THRESHOLD = 100_000

SOURCES = [
    Path("C:/Users/frane/Desktop/Minor/DIA/Queries/timetables"),
    Path("C:/Users/frane/Desktop/Minor/DIA/Queries/timetable_changes")
]

OUTPUT_MOVEMENTS = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/staging_movements2.parquet"
XSLT_FILE = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/flatten_movements.xslt"

#parse date/time helpers
def parse_date(ts):
    if not ts or len(ts) < 6:
        return None
    try:
        return date(
            2000 + int(ts[0:2]),
            int(ts[2:4]),
            int(ts[4:6]),
        )
    except Exception:
        return None

def parse_time(ts):
    if not ts or len(ts) < 10:
        return None
    try:
        return time(
            int(ts[6:8]),
            int(ts[8:10]),
        )
    except Exception:
        return None

#combine date and time to timestamp
def parse_timestamp(ts):
    d = parse_date(ts)
    if d is None:
        return None
    t = parse_time(ts)
    if t is None:
        return None
    return datetime.combine(d, t)

#safe get helper
def safe_get(value):
    if value is None:
        return None
    value = value.strip()
    return value if value != "" else None

# Create Spark session
spark = SparkSession.builder \
    .appName("DBahnIngestion") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for movements
movements_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("trip_date", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("station_xml_name", StringType(), True),
    StructField("arrival_planned", TimestampType(), True),
    StructField("arrival_actual", TimestampType(), True),
    StructField("arrival_status", StringType(), True),
    StructField("departure_planned", TimestampType(), True),
    StructField("departure_actual", TimestampType(), True),
    StructField("departure_status", StringType(), True),
])

# Load XSLT once
xslt = etree.XSLT(etree.parse(XSLT_FILE))

first_write = True
movements_buffer = []
row_counter = 0

# Function to flush movements buffer to Parquet
def flush_movements():
    global movements_buffer, first_write
    
    if not movements_buffer:
        return
     
    try:
        # Create Spark DataFrame from buffer and convert to Pandas DataFrame
        df = spark.createDataFrame(movements_buffer, schema=movements_schema)
        pandas_df = df.toPandas()
        
        # Convert timestamp columns to datetime64[us] for Parquet compatibility
        timestamp_cols = ['arrival_planned', 'arrival_actual', 'departure_planned', 'departure_actual']
        for col in timestamp_cols:
            if col in pandas_df.columns:
                pandas_df[col] = pd.to_datetime(pandas_df[col], errors='coerce').astype('datetime64[us]')
        
        if first_write:
            pandas_df.to_parquet(OUTPUT_MOVEMENTS, engine='pyarrow', index=False)
            first_write = False
        else:
            # Append to existing Parquet file, 
            existing_df = pd.read_parquet(OUTPUT_MOVEMENTS)
            combined_df = pd.concat([existing_df, pandas_df], ignore_index=True)
            combined_df.to_parquet(OUTPUT_MOVEMENTS, engine='pyarrow', index=False)
        
        movements_buffer = []
        
    except Exception as e:
        import traceback
        traceback.print_exc()

total_files_processed = 0
total_movements = 0

for source in SOURCES:
    if not source.exists():
        continue

    for range_dir in source.iterdir():
        if not range_dir.is_dir():
            continue

        ts_dirs = sorted(d for d in range_dir.iterdir() if d.is_dir())

        desc = f"Ingesting {range_dir.name} changes" if "changes" in source.name else f"Ingesting {range_dir.name}"
        
        for ts_dir in tqdm(ts_dirs, desc=desc):
            #trip date, later needed for daily average
            trip_date = ts_dir.name[:8]

            for xml_file in ts_dir.glob("*.xml"):
                try:
                    total_files_processed += 1
                    
                    tree = etree.parse(xml_file)
                    result = xslt(tree)
                    root = result.getroot()
                    
                    station_xml_name = root.attrib.get("station")
                    #for each row in xml, parse and add to buffer
                    for row in root.iter("row"):
                        stop_id = safe_get(row.get("stop_id"))
                        if not stop_id:
                            continue
                        
                        parts = stop_id.split("-")
                        if stop_id.startswith("-"):
                            trip_id = f"-{parts[1]}"
                        else:
                            trip_id = parts[0]
                        
                        # Parse timestamps and statuses, for timetables it will first fill planned times, and make the actual times same as planned
                        # For timetable changes, it will fill actual times if available, and then append the record, making duplicate records in the parquet
                        ar_pt_ts = parse_timestamp(safe_get(row.get("ar_pt")))
                        ar_ct_ts = parse_timestamp(safe_get(row.get("ar_ct"))) or ar_pt_ts
                        dp_pt_ts = parse_timestamp(safe_get(row.get("dp_pt")))
                        dp_ct_ts = parse_timestamp(safe_get(row.get("dp_ct"))) or dp_pt_ts
                        
                        ar_status = safe_get(row.get("ar_cs"))
                        if ar_status == "c":
                            ar_ct_ts = None
                        
                        dp_status = safe_get(row.get("dp_cs"))
                        if dp_status == "c":
                            dp_ct_ts = None
                        
                        #create movement record
                        movement_record = (
                            stop_id,
                            trip_date,
                            trip_id,
                            station_xml_name,
                            ar_pt_ts,
                            ar_ct_ts,
                            ar_status,
                            dp_pt_ts,
                            dp_ct_ts,
                            dp_status,
                        )
                        movements_buffer.append(movement_record)
                        total_movements += 1
                        
                        row_counter += 1
                        #Append to existing parquet but in batches
                        if row_counter >= ROW_COMMIT_THRESHOLD:
                            flush_movements()
                            row_counter = 0
                
                except Exception as e:
                    continue

    print(f"Completed ingestion from source: {source.name}")

if movements_buffer:
    flush_movements()

if os.path.exists(OUTPUT_MOVEMENTS):
    df_movements = spark.read.parquet(OUTPUT_MOVEMENTS)
    #output example data
    df_movements.show(5, truncate=False)
    df_movements.select("station_xml_name").distinct().show(truncate=False)

spark.stop()

## change names output files and paths
## verify spark works on other machines
## load entire data again and run queries on that