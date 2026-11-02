
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_DIR / "results"
OUTPUT_DIR = RESULTS_DIR / "figures"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def read_spark_csv(folder_name):
    """Read all Spark CSV part files from a result folder."""
    folder_path = RESULTS_DIR / folder_name

    csv_files = list(folder_path.glob("part-*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV part files found in {folder_path}"
        )

    return pd.concat(
        [pd.read_csv(file) for file in csv_files],
        ignore_index=True
    )


# Load the three analysis results
zone_df = read_spark_csv("demand_by_zone")
hour_df = read_spark_csv("demand_by_hour")
day_df = read_spark_csv("demand_by_day")

# Convert columns to numeric values
zone_df["total_pickups"] = pd.to_numeric(
    zone_df["total_pickups"]
)

hour_df["pickup_hour"] = pd.to_numeric(
    hour_df["pickup_hour"]
)
hour_df["total_pickups"] = pd.to_numeric(
    hour_df["total_pickups"]
)

day_df["total_pickups"] = pd.to_numeric(
    day_df["total_pickups"]
)

# 1. Top 10 pickup zones
top_zones = zone_df.nlargest(10, "total_pickups").sort_values(
    "total_pickups"
)

plt.figure(figsize=(11, 6))
plt.barh(
    top_zones["pickup_zone"],
    top_zones["total_pickups"]
)
plt.title("Top 10 NYC Yellow Taxi Pickup Zones")
plt.xlabel("Number of pickups")
plt.ylabel("Pickup zone")
plt.tight_layout()
plt.savefig(
    OUTPUT_DIR / "top_10_pickup_zones.png",
    dpi=300,
    bbox_inches="tight"
)
plt.close()

# 2. Pickup demand by hour
hour_df = hour_df.sort_values("pickup_hour")

plt.figure(figsize=(11, 6))
plt.plot(
    hour_df["pickup_hour"],
    hour_df["total_pickups"],
    marker="o"
)
plt.xticks(range(24))
plt.title("NYC Yellow Taxi Pickup Demand by Hour")
plt.xlabel("Hour of the day")
plt.ylabel("Number of pickups")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    OUTPUT_DIR / "demand_by_hour.png",
    dpi=300,
    bbox_inches="tight"
)
plt.close()

# 3. Pickup demand by day of the week
day_order = [
    "Monday", "Tuesday", "Wednesday",
    "Thursday", "Friday", "Saturday", "Sunday"
]

day_df["day_of_week"] = pd.Categorical(
    day_df["day_of_week"],
    categories=day_order,
    ordered=True
)
day_df = day_df.sort_values("day_of_week")

plt.figure(figsize=(10, 6))
plt.bar(
    day_df["day_of_week"].astype(str),
    day_df["total_pickups"]
)
plt.title("NYC Yellow Taxi Pickup Demand by Day of the Week")
plt.xlabel("Day of the week")
plt.ylabel("Number of pickups")
plt.xticks(rotation=30)
plt.tight_layout()
plt.savefig(
    OUTPUT_DIR / "demand_by_day.png",
    dpi=300,
    bbox_inches="tight"
)
plt.close()

print("========== GRAPH GENERATION ==========")
print("Generated graphs:")
for filename in [
    "top_10_pickup_zones.png",
    "demand_by_hour.png",
    "demand_by_day.png"
]:
    print(OUTPUT_DIR / filename)

print("\nAll graphs generated successfully.")

