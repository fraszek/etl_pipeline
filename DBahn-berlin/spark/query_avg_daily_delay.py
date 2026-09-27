"""
Task 3.2: Average daily delay for one station.

Reads the Parquet dataset produced by etl_movements_to_parquet.py and, for the
station in STATION_NAME, prints one row per day with:
    date | avg_delay_minutes | num_departures

Delay = departure_actual - departure_planned, in minutes. Only departures with
both a planned and an actual time are used, so cancelled departures (which
have no actual time) are left out.

Before running, update JAVA_HOME and OUTPUT_PARQUET below.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, avg, count

# Set environment variables for Java and PySpark
os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Path to the Parquet file written by the ETL job
OUTPUT_PARQUET = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/staging_movements.parquet"
#Change the parameter for station name as needed (must match the station name in the XML files)
STATION_NAME = "Berlin Zoologischer Garten"

# Create Spark session
spark = SparkSession.builder \
    .appName("GetStationNames") \
    .master("local[*]") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet(OUTPUT_PARQUET)

# Filter for the specified station and valid planned and actual departure times
station_df = df.filter(
    (col("station_xml_name") == STATION_NAME) &
    (col("departure_planned").isNotNull()) &
    (col("departure_actual").isNotNull())
)

# Calculate delay in minutes for each movement
delays_df = station_df.withColumn(
    "delay_minutes",
    (unix_timestamp(col("departure_actual")) - unix_timestamp(col("departure_planned"))) / 60
).withColumn(
    "date",
    to_date(col("departure_planned"))
)

# Compute average delay per day
daily_avg_delay = delays_df.groupBy("date") \
    .agg(
        avg("delay_minutes").alias("avg_delay_minutes"),
        count("*").alias("num_departures")
    ) \
    .orderBy("date")
daily_avg_delay.show(100, truncate=False)


# Stop Spark
spark.stop()