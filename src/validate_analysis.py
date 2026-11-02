
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

PROJECT_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_DIR / "results"

spark = (
    SparkSession.builder
    .appName("Validate Demand Analysis")
    .master("local[2]")
    .getOrCreate()
)

expected_total = 119118381

analyses = {
    "Demand by zone": "demand_by_zone",
    "Demand by hour": "demand_by_hour",
    "Demand by day": "demand_by_day"
}

print("========== ANALYSIS RESULTS VALIDATION ==========")

for analysis_name, folder_name in analyses.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(RESULTS_DIR / folder_name))
    )

    actual_total = df.agg(
        spark_sum("total_pickups")
    ).first()[0]

    print(f"\n{analysis_name}")
    print("Calculated total:", actual_total)
    print("Expected total:", expected_total)
    print("Validation:", actual_total == expected_total)

spark.stop()
