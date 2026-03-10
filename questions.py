from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp
import requests
from pathlib import Path
import os

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

zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
zone_path = "taxi_zone_lookup.csv"
if not Path(zone_path).exists():
    print("Downloading zone lookup...")
    resp = requests.get(zone_url)
    with open(zone_path, "wb") as f:
        f.write(resp.content)

df = spark.read.parquet(local_path)

# Q1
print(f"Q1: {spark.version}")

# Q2
output_path = "yellow_2025_11_partitioned"
df.repartition(4).write.mode("overwrite").parquet(output_path)

parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
sizes = [os.path.getsize(os.path.join(output_path, f)) / (1024 * 1024) for f in parquet_files]

for f, s in zip(parquet_files, sizes):
    print(f"{f}: {s:.1} MB")
print(f"Q2 Average: {sum(sizes)/len(sizes):.1f} MB")

# Q3
nov15_trips = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
print(f"Q3 {nov15_trips}")

# Q4
df_with_duration = df.withColumn(
    "duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)
longest = df_with_duration.agg({"duration_hours": "max"}).collect()[0][0]
print(f"Q4 {longest:.1f} hours")

# Q6
zones = spark.read.option("header", "true").csv(zone_path)
zones.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

least_frequent = spark.sql("""
    SELECT z.Zone, COUNT(*) as cnt
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY cnt ASC
    LIMIT 5
""")
print("Q6")
least_frequent.show(truncate=False)

input("Press Enter to stop Spark...")
spark.stop()
