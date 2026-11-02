
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("NYC Taxi Zone Join")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

# Read the cleaned taxi trips and zone lookup
taxi_df = spark.read.parquet(
    str(PROCESSED_DIR / "yellow_trips_cleaned")
)

zones_df = spark.read.parquet(
    str(PROCESSED_DIR / "taxi_zone_lookup")
)

print("========== BEFORE JOIN ==========")
print("Taxi trips:", taxi_df.count())
print("Taxi zones:", zones_df.count())

# Rename lookup columns to make the join clearer
zones_df = zones_df.select(
    col("LocationID").alias("PULocationID"),
    col("Zone").alias("pickup_zone"),
    col("Borough").alias("pickup_borough"),
    col("service_zone").alias("pickup_service_zone")
)

# Join taxi trips with the pickup zone information
joined_df = taxi_df.join(
    broadcast(zones_df),
    on="PULocationID",
    how="inner"
)

print("\n========== AFTER JOIN ==========")
print("Joined trips:", joined_df.count())

# Display a sample of the joined data
joined_df.select(
    "tpep_pickup_datetime",
    "PULocationID",
    "pickup_zone",
    "pickup_borough",
    "trip_distance"
).show(10, truncate=False)

# Save the joined dataset
output_path = PROCESSED_DIR / "yellow_trips_with_zones"

(
    joined_df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(str(output_path))
)

print("\nJoined dataset saved to:", output_path)

spark.stop()
