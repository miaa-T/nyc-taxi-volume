
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"

spark = (
    SparkSession.builder
    .appName("NYC Taxi Schema Inspection")
    .master("local[*]")
    .getOrCreate()
)

# Inspect one monthly Parquet file
parquet_file = RAW_DIR / "yellow_tripdata_2022-01.parquet"

print("\n========== YELLOW TAXI SCHEMA ==========")

taxi_df = spark.read.parquet(str(parquet_file))

taxi_df.printSchema()

print("\n========== SAMPLE TAXI RECORDS ==========")
taxi_df.show(5, truncate=False)

# Inspect the Taxi Zone Lookup CSV
lookup_file = RAW_DIR / "taxi_zone_lookup.csv"

print("\n========== TAXI ZONE LOOKUP ==========")

lookup_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(str(lookup_file))
)

lookup_df.printSchema()
lookup_df.show(5, truncate=False)

spark.stop()

