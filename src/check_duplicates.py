
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, coalesce, lit

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("Precise Duplicate Check")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

# Read only January 2022
taxi_df = (
    spark.read.parquet(str(PROCESSED_DIR / "yellow_trips"))
    .filter((col("year") == 2022) & (col("month") == 1))
)

total_rows = taxi_df.count()
print("========== DATASET ==========")
print("January 2022 total rows:", total_rows)

# Check 1: duplicates based on the six columns
selected_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance"
]

print("\n========== SIX-COLUMN DUPLICATE CHECK ==========")

duplicates_six = (
    taxi_df
    .groupBy(*selected_columns)
    .agg(count("*").alias("occurrences"))
    .filter(col("occurrences") > 1)
)

stats_six = duplicates_six.agg(
    count("*").alias("duplicate_groups"),
    coalesce(
        spark_sum(col("occurrences") - 1), lit(0)
    ).alias("extra_rows")
).first()

print("Duplicate groups:", stats_six["duplicate_groups"])
print("Extra rows in these groups:", stats_six["extra_rows"])

# Check 2: exact duplicates across all columns
print("\n========== EXACT DUPLICATE CHECK ==========")

all_columns = taxi_df.columns

duplicates_exact = (
    taxi_df
    .groupBy(*all_columns)
    .agg(count("*").alias("occurrences"))
    .filter(col("occurrences") > 1)
)

stats_exact = duplicates_exact.agg(
    count("*").alias("duplicate_groups"),
    coalesce(
        spark_sum(col("occurrences") - 1), lit(0)
    ).alias("extra_rows")
).first()

print("Exact duplicate groups:", stats_exact["duplicate_groups"])
print("Extra rows in these groups:", stats_exact["extra_rows"])

print("\nDuplicate verification completed.")

spark.stop()
