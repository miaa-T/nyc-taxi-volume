
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    IntegerType,
    TimestampNTZType
)

# Project directories
PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

TAXI_OUTPUT = PROCESSED_DIR / "yellow_trips"
LOOKUP_OUTPUT = PROCESSED_DIR / "taxi_zone_lookup"

# Start Spark

spark = (
    SparkSession.builder
    .appName("NYC Taxi Data Ingestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)


# Explicit schema for Yellow Taxi records
taxi_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

# Explicit schema for Taxi Zone Lookup
lookup_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

# Process the 36 monthly files
first_file = True

for year in range(2022, 2025):
    for month in range(1, 13):

        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        input_path = RAW_DIR / filename

        if not input_path.exists():
            raise FileNotFoundError(f"Missing input file: {input_path}")

        print(f"\nProcessing {filename}...")

        

        # Read using the physical schema stored in this monthly Parquet file
        taxi_df = spark.read.parquet(str(input_path))

        # Standardize column types using our explicit target schema
        taxi_df = taxi_df.select([
            col(field.name).cast(field.dataType).alias(field.name)
            for field in taxi_schema.fields
        ])


        # Add year and month columns
        taxi_df = (
            taxi_df
            .withColumn("year", lit(year))
            .withColumn("month", lit(month))
        )

        # First file creates the output; subsequent files append
        write_mode = "overwrite" if first_file else "append"

        (
            taxi_df.write
            .mode(write_mode)
            .partitionBy("year", "month")
            .parquet(str(TAXI_OUTPUT))
        )

        first_file = False
        print(f"Finished {filename}")

# Process Taxi Zone Lookup
print("\nProcessing Taxi Zone Lookup...")

lookup_df = (
    spark.read
    .schema(lookup_schema)
    .option("header", True)
    .option("mode", "FAILFAST")
    .csv(str(RAW_DIR / "taxi_zone_lookup.csv"))
)

lookup_df.write.mode("overwrite").parquet(str(LOOKUP_OUTPUT))

print("\nIngestion completed successfully.")
print(f"Taxi records saved to: {TAXI_OUTPUT}")
print(f"Zone lookup saved to: {LOOKUP_OUTPUT}")

spark.stop()
