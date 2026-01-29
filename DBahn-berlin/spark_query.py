import os
import sys
from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["HADOOP_HOME"] = r"C:/hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

OUTPUT_PARQUET = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/output.parquet"

# Create Spark session
spark = SparkSession.builder \
    .appName("GetStationNames") \
    .master("local[*]") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet(OUTPUT_PARQUET)

# Get unique station names
station_names = df.select("station_xml_name").distinct().orderBy("station_xml_name")

# Show the results
print("\n" + "="*60)
print("Station Names in the Dataset")
print("="*60)
station_names.show(truncate=False)

# Get count
count = station_names.count()
print(f"\nTotal unique stations: {count}")

# Stop Spark
spark.stop()