import json
from pathlib import Path
from tqdm import tqdm
from datetime import date, datetime, time
from xml.etree import ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import sys
import pandas as pd

os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["HADOOP_HOME"] = r"C:/hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROW_COMMIT_THRESHOLD = 50_000

SOURCES = [Path("C:/Users/frane/Desktop/Minor/DIA/Queries/timetables")]

OUTPUT_PARQUET = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/output.parquet"

def parse_date(ts):
    if not ts:
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
    if not ts:
        return None
    try:
        return time(
            int(ts[6:8]),
            int(ts[8:10]),
        )
    except Exception:
        return None

def parse_db_dt(ts):
    d = parse_date(ts)
    if d is None:
        return None

    t = parse_time(ts)
    if t is None:
        return None

    return datetime.combine(d, t)

spark = SparkSession.builder \
    .appName("DBahnIngestion") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("observation_ts", TimestampType(), True),
    StructField("station_xml_name", StringType(), True),
    StructField("arrival_planned", TimestampType(), True),
    StructField("arrival_actual", TimestampType(), True),
    StructField("arrival_status", StringType(), True),
    StructField("departure_planned", TimestampType(), True),
    StructField("departure_actual", TimestampType(), True),
    StructField("departure_status", StringType(), True),
])

# Define globals BEFORE function
first_write = True
row_counter = 0
data_buffer = []

def flush_spark_parquet():
    global data_buffer, first_write
    
    if not data_buffer:
        print("⚠ No data to flush, buffer is empty!")
        return
     
    try:
        # Create DataFrame from buffer
        df = spark.createDataFrame(data_buffer, schema=schema)
        
        # Convert to Pandas
        pandas_df = df.toPandas()
        
        # Convert all timestamp columns to microsecond precision (Spark-compatible)
        # This fixes the "TIMESTAMP(NANOS,false)" error
        timestamp_cols = ['observation_ts', 'arrival_planned', 'arrival_actual', 
                         'departure_planned', 'departure_actual']
        for col in timestamp_cols:
            if col in pandas_df.columns:
                pandas_df[col] = pd.to_datetime(pandas_df[col], errors='coerce').astype('datetime64[us]')
        
        if first_write:
            # First write: create new file
            print(f"📝 Creating new file: {OUTPUT_PARQUET}")
            pandas_df.to_parquet(OUTPUT_PARQUET, engine='pyarrow', index=False)
            first_write = False
        else:
            # Subsequent writes: read existing, append, write back
            print(f"➕ Appending to existing file: {OUTPUT_PARQUET}")
            existing_df = pd.read_parquet(OUTPUT_PARQUET)
            print(f"   Existing rows: {len(existing_df)}")
            combined_df = pd.concat([existing_df, pandas_df], ignore_index=True)
            combined_df.to_parquet(OUTPUT_PARQUET, engine='pyarrow', index=False)
        
        data_buffer = []
        file_size = os.path.getsize(OUTPUT_PARQUET) if os.path.exists(OUTPUT_PARQUET) else 0
        print(f"✓ Flush successful! File size: {file_size:,} bytes")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ ERROR during flush: {e}")
        import traceback
        traceback.print_exc()

total_files_processed = 0
total_rows_found = 0

for source in SOURCES:
    if not source.exists():
        print(f"❌ Source not found: {source}")
        continue

    for range_dir in source.iterdir():
        if not range_dir.is_dir():
            continue

        ts_dirs = sorted(d for d in range_dir.iterdir() if d.is_dir())

        for ts_dir in tqdm(ts_dirs, desc=f"Ingesting {range_dir.name}"):
            obs_ts = parse_db_dt(ts_dir.name)

            for xml_file in ts_dir.glob("*.xml"):
                try:
                    total_files_processed += 1
                    
                    with open(xml_file, 'r', encoding='utf-8') as f:
                        tree = ET.parse(f).getroot()
                    
                    station = tree.attrib.get("station")
                    
                    # Count rows in this file
                    rows_in_file = 0
                    
                    for row in tree.iter("s"):  # Adjust based on your XML structure
                        sid = row.get("id")
                        if not sid:
                            continue
                        
                        rows_in_file += 1
                        
                        ar_pt = row.get("ar_pt")
                        ar_ct = row.get("ar_ct") or ar_pt
                        
                        dp_pt = row.get("dp_pt")
                        dp_ct = row.get("dp_ct") or dp_pt
                        
                        record = (
                            sid,
                            obs_ts,
                            station,
                            parse_db_dt(ar_pt),
                            parse_db_dt(ar_ct),
                            row.get("ar_cs"),
                            parse_db_dt(dp_pt),
                            parse_db_dt(dp_ct),
                            row.get("dp_cs"),
                        )
                        
                        data_buffer.append(record)
                        row_counter += 1
                        total_rows_found += 1
                        
                        if row_counter >= ROW_COMMIT_THRESHOLD:
                            flush_spark_parquet()
                            row_counter = 0
                    
                    # Debug: Print if we found rows
                    if rows_in_file > 0 and total_files_processed % 100 == 0:
                        print(f"\n📊 Progress: {total_files_processed} files, {total_rows_found} total rows, {len(data_buffer)} in buffer")
                
                except Exception as e:
                    print(f"\n❌ Error processing {xml_file}: {e}")
                    continue

# Final flush for remaining data
print(f"\n{'='*60}")
print(f"📊 Processing Complete!")
print(f"   Files processed: {total_files_processed}")
print(f"   Total rows found: {total_rows_found}")
print(f"   Rows in buffer: {len(data_buffer)}")
print(f"{'='*60}\n")

if data_buffer:
    print(f"🔄 Final flush with {len(data_buffer)} remaining rows...")
    flush_spark_parquet()
else:
    print(f"⚠ No data in buffer to flush!")

# Verify the data
print(f"\n{'='*60}")
print(f"Verification")
print(f"{'='*60}")

if os.path.exists(OUTPUT_PARQUET):
    print(f"✓ Output file exists: {OUTPUT_PARQUET}")
    print(f"✓ File size: {os.path.getsize(OUTPUT_PARQUET):,} bytes")
    
    df_verify = spark.read.parquet(OUTPUT_PARQUET)
    total_rows = df_verify.count()
    print(f"✓ Total rows in Parquet: {total_rows:,}")
    
    print("\nFirst 5 rows:")
    df_verify.show(5, truncate=False)
else:
    print(f"❌ ERROR: Output file not created at {OUTPUT_PARQUET}!")
    print(f"   This means NO data was written at all.")
    print(f"   Check the XML structure - maybe 'tree.iter(\"s\")' is wrong?")

spark.stop()

print(f"\n✓ Ingestion Finished!")