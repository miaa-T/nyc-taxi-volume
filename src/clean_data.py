
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("NYC Taxi Data Cleaning")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

# Read the ingested taxi data
taxi_df = spark.read.parquet(
    str(PROCESSED_DIR / "yellow_trips")
)

print("========== INITIAL DATA ==========")

initial_count = taxi_df.count()
print("Initial number of trips:", initial_count)

# Identify trips with invalid dates
invalid_dates_df = taxi_df.filter(
    col("tpep_dropoff_datetime")
    < col("tpep_pickup_datetime")
)

invalid_count = invalid_dates_df.count()

print("\n========== INVALID DATES ==========")
print("Trips with invalid dates:", invalid_count)

# Remove trips with invalid dates
cleaned_df = taxi_df.filter(
    col("tpep_dropoff_datetime")
    >= col("tpep_pickup_datetime")
)

cleaned_count = cleaned_df.count()

print("\n========== CLEANING SUMMARY ==========")
print("Initial trips:", initial_count)
print("Removed trips:", invalid_count)
print("Remaining trips:", cleaned_count)
print("Check:", initial_count - invalid_count == cleaned_count)

# Save cleaned data separately
output_path = PROCESSED_DIR / "yellow_trips_cleaned"

(
    cleaned_df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(str(output_path))
)

print("\nCleaned dataset saved to:", output_path)

spark.stop()
