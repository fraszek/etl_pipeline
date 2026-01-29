from pyspark.sql import SparkSession
import os
import sys
import pandas as pd

os.environ['JAVA_HOME'] = r'C:/Users/frane/.jdks/ms-17.0.17'
os.environ["HADOOP_HOME"] = r"C:/hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("ParquetExample") \
    .master("local[*]") \
    .getOrCreate()

parquet_path = r"C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/output.parquet"
    
# Read back with Spark
df_read = spark.read.parquet(parquet_path)
print("\n✓ Reading back the data:")
df_read.show()

spark.stop()
