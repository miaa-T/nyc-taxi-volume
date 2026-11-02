
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("Taxi Data Quality Profiling")
    .master("local[*]")
    .getOrCreate()
)

taxi_df = spark.read.parquet(str(PROCESSED_DIR / "yellow_trips"))

print("========== DATA QUALITY PROFILE ==========")

# Count missing values in important columns
columns_to_check = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount"
]

quality_checks = []

for column_name in columns_to_check:
    quality_checks.append(
        spark_sum(
            when(col(column_name).isNull(), 1).otherwise(0)
        ).alias(f"{column_name}_nulls")
    )

# Count potentially invalid records
quality_checks.extend([
    spark_sum(
        when(
            col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"),
            1
        ).otherwise(0)
    ).alias("invalid_trip_dates"),

    spark_sum(
        when(col("trip_distance") < 0, 1).otherwise(0)
    ).alias("negative_distances"),

    spark_sum(
        when(col("trip_distance") == 0, 1).otherwise(0)
    ).alias("zero_distances"),

    spark_sum(
        when(col("fare_amount") < 0, 1).otherwise(0)
    ).alias("negative_fares"),

    spark_sum(
        when(col("total_amount") < 0, 1).otherwise(0)
    ).alias("negative_total_amounts")
])

results = taxi_df.agg(*quality_checks)

results.show(truncate=False)

spark.stop()
