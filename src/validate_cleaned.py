
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("Validate Cleaned Taxi Data")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

cleaned_df = spark.read.parquet(
    str(PROCESSED_DIR / "yellow_trips_cleaned")
)

print("========== CLEANED DATA VALIDATION ==========")

total_count = cleaned_df.count()
print("Total cleaned trips:", total_count)

invalid_dates = cleaned_df.filter(
    col("tpep_dropoff_datetime")
    < col("tpep_pickup_datetime")
).count()

print("Trips with invalid dates:", invalid_dates)

null_pickup_dates = cleaned_df.filter(
    col("tpep_pickup_datetime").isNull()
).count()

print("Trips with missing pickup dates:", null_pickup_dates)

null_pickup_zones = cleaned_df.filter(
    col("PULocationID").isNull()
).count()

print("Trips with missing pickup zones:", null_pickup_zones)

print("\n========== VALIDATION SUMMARY ==========")
print("No invalid dates:", invalid_dates == 0)
print("No missing pickup dates:", null_pickup_dates == 0)
print("No missing pickup zones:", null_pickup_zones == 0)

spark.stop()
