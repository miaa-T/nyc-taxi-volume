from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
RESULTS_DIR = PROJECT_DIR / "results"

spark = (
    SparkSession.builder
    .appName("NYC Taxi Demand Analysis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "24")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

# Read the cleaned and joined dataset
taxi_df = spark.read.parquet(
    str(PROCESSED_DIR / "yellow_trips_with_zones")
)

# Register the dataset as a SQL view
taxi_df.createOrReplaceTempView("taxi_trips")

print("========== ANALYSIS 1: DEMAND BY ZONE ==========")

zone_demand = spark.sql("""
    SELECT
        pickup_borough,
        pickup_zone,
        COUNT(*) AS total_pickups
    FROM taxi_trips
    GROUP BY pickup_borough, pickup_zone
    ORDER BY total_pickups DESC
""")

zone_demand.show(20, truncate=False)

print("\n========== ANALYSIS 2: DEMAND BY HOUR ==========")

hourly_demand = spark.sql("""
    SELECT
        HOUR(tpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS total_pickups
    FROM taxi_trips
    GROUP BY HOUR(tpep_pickup_datetime)
    ORDER BY pickup_hour
""")

hourly_demand.show(24)

print("\n========== ANALYSIS 3: DEMAND BY DAY OF WEEK ==========")

daily_demand = spark.sql("""
    SELECT
        CASE DAYOFWEEK(tpep_pickup_datetime)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_of_week,
        DAYOFWEEK(tpep_pickup_datetime) AS day_number,
        COUNT(*) AS total_pickups
    FROM taxi_trips
    GROUP BY
        DAYOFWEEK(tpep_pickup_datetime),
        CASE DAYOFWEEK(tpep_pickup_datetime)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END
    ORDER BY day_number
""")

daily_demand.show(7, truncate=False)

# Save aggregated results
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

zone_demand.write.mode("overwrite").option(
    "header", True
).csv(str(RESULTS_DIR / "demand_by_zone"))

hourly_demand.write.mode("overwrite").option(
    "header", True
).csv(str(RESULTS_DIR / "demand_by_hour"))

daily_demand.write.mode("overwrite").option(
    "header", True
).csv(str(RESULTS_DIR / "demand_by_day"))

print("\nAll demand analysis results saved in:", RESULTS_DIR)

spark.stop()
