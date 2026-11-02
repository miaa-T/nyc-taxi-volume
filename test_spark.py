from pyspark.sql import SparkSession

# Start Spark
spark = (
    SparkSession.builder
    .appName("NYC Taxi Project Test")
    .master("local[*]")
    .getOrCreate()
)

# Create a small test dataset
data = [
    ("Manhattan", 120),
    ("Brooklyn", 85),
    ("Queens", 60),
]

df = spark.createDataFrame(data, ["borough", "trips"])

# Register it as a temporary SQL table
df.createOrReplaceTempView("taxi_trips")

# Run a Spark SQL query
result = spark.sql("""
    SELECT borough, trips
    FROM taxi_trips
    WHERE trips > 70
    ORDER BY trips DESC
""")

result.show()

spark.stop()