import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_date, avg, count

# Set environment variables for Java and PySpark
os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

OUTPUT_PARQUET = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/staging_movements.parquet"
#Change the parameter for station name as needed
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