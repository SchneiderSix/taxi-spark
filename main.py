import pyspark as ps
from pyspark.sql import SparkSession
import requests
from pathlib import Path

# Create session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("taxi") \
    .getOrCreate() \

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
local_path = "yellow_tripdata_2025-11.parquet"

if not Path(local_path).exists():
    response = requests.get(url)
    with open(local_path, "wb") as f:
        f.write(response.content)

df = spark.read.parquet(local_path)

# Schema with types
df.printSchema()

# Column names only
print(df.columns)

# First 5 rows as a table
df.show(5)

# First 5 rows vertically (easier to read with many columns)
df.show(5, vertical=True)

# Row count + column count
print(f"{df.count()} rows, {len(df.columns)} columns")

# Basic stats (count, mean, stddev, min, max)
df.describe().show()

input("Press Enter to stop Spark...")
spark.stop()
