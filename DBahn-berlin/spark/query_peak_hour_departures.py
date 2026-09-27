"""
Task 3.3: Average number of train departures per station during peak hours.

Peak hours are 07:00-09:00 and 17:00-19:00, i.e. planned departure hours
7, 8, 17 and 18. For every station the script counts non-cancelled peak-hour
departures per day and then averages these daily counts over all days:
    station_xml_name | avg_daily_peak_departures | num_days

Rows are first de-duplicated on (stop_id, trip_date). The ETL appends a new
row for every timetable change, so the same stop can appear several times.

Before running, update JAVA_HOME and OUTPUT_PARQUET below.
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, avg, count, hour

# Set environment variables for Java and PySpark
os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Path to the Parquet file written by the ETL job
OUTPUT_PARQUET = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/staging_movements.parquet"
# Not used by this query (it covers all stations); kept for consistency with query_avg_daily_delay.py
STATION_NAME = "Berlin Zoologischer Garten"

# Create Spark session
spark = SparkSession.builder \
    .appName("GetStationNames") \
    .master("local[*]") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet(OUTPUT_PARQUET)
# Keep a single row per stop event (timetable changes create duplicates)
df_deduped = df.dropDuplicates(["stop_id", "trip_date"])

# Filter for departures that are not cancelled and have a planned departure time
departures_with_hour = df_deduped.filter(
    col("departure_planned").isNotNull() &
    ((col("departure_status").isNull()) | (col("departure_status") != "c"))
).withColumn(
    "departure_hour", 
    hour(col("departure_planned"))
).withColumn(
    "date",
    to_date(col("departure_planned"))
)

# Filter for peak hours (7-8 for morning, 17-18 for evening)
# hour() 7 and 8 together cover 07:00-08:59, and 17 and 18 cover 17:00-18:59
peak_hour_departures = departures_with_hour.filter(
    (col("departure_hour").isin([7, 8, 17, 18]))
)

# Count departures per station per day
daily_departures_per_station = peak_hour_departures.groupBy(
    "station_xml_name", 
    "date"
).agg(
    count("*").alias("departures_count")
)

# Calculate average departures per station across all days
avg_departures_per_station = daily_departures_per_station.groupBy(
    "station_xml_name"
).agg(
    avg("departures_count").alias("avg_daily_peak_departures"),
    count("date").alias("num_days")
).orderBy(
    col("avg_daily_peak_departures").desc()
)

# Show results
print("\nAverage Daily Peak Hour Departures per Station:")
avg_departures_per_station.show(100, truncate=False)

# Stop Spark
spark.stop()


