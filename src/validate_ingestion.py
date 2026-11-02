from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

spark = (
    SparkSession.builder
    .appName("Validate Taxi Ingestion")
    .master("local[*]")
    .getOrCreate()
)

# Read processed taxi data
taxi_df = spark.read.parquet(str(PROCESSED_DIR / "yellow_trips"))

# Read taxi zone lookup
zones_df = spark.read.parquet(str(PROCESSED_DIR / "taxi_zone_lookup"))

print("========== TAXI DATA ==========")
taxi_df.printSchema()
print("Total taxi records:", taxi_df.count())
taxi_df.show(5, truncate=False)

print("\n========== TAXI ZONES ==========")
zones_df.printSchema()
print("Total taxi zones:", zones_df.count())
zones_df.show(5, truncate=False)

print("\n========== MONTHLY DISTRIBUTION ==========")
taxi_df.groupBy("year", "month") \
    .count() \
    .orderBy("year", "month") \
    .show(36)

spark.stop()