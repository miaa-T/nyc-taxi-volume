# NYC Yellow Taxi Demand Analysis

## 1. Project Overview

This project analyses New York City Yellow Taxi trip records using Apache Spark and Spark SQL.

The main objective is to understand how yellow taxi pickup demand varies across different taxi zones, hours of the day, and days of the week.

**Research question:**

How does yellow taxi activity vary over time and across New York City taxi zones?

**Part 1 – Volume sub-question:**

How does yellow taxi pickup demand vary by taxi zone, hour of the day, and day of the week?

## 2. Datasets

Two datasets published by the New York City Taxi and Limousine Commission (NYC TLC) are used.

### NYC Yellow Taxi Trip Records

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Period:** January 2022 – December 2024
- **Format:** Monthly Parquet files
- **Number of files:** 36

### Taxi Zone Lookup

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Format:** CSV
- **Content:** Zone identifiers, zone names, boroughs, and service zones.
- **Purpose:** Associates pickup location IDs with readable geographic information.

The total size of the downloaded raw data is approximately **1.81 GB**, including the 36 monthly trip files and the zone lookup file.

The taxi records are joined with the lookup table using `PULocationID = LocationID`.

The original data is published by NYC TLC. Users should consult the source's current terms and data-use information before redistribution.

## 3. Technologies

- Python 3.11
- Apache Spark / PySpark 3.5.8
- Spark SQL
- Docker
- Parquet
- Pandas and Matplotlib for result visualisation

## 4. Project Structure

```text
nyc-taxi-volume/
├── data/
│   ├── raw/
│   └── processed/
├── scripts/
│   └── download_data.py
├── src/
│   ├── ingestion.py
│   ├── inspect_data.py
│   ├── profile_data.py
│   ├── check_duplicates.py
│   ├── clean_data.py
│   ├── validate_cleaned.py
│   ├── join_datasets.py
│   ├── analyze_demand.py
│   ├── validate_analysis.py
│   └── plot_results.py
├── results/
│   ├── figures/
│   ├── main.tex
│   └── main.pdf
├── Dockerfile
├── requirements.txt
└── README.md
```

## 5. Data Processing Pipeline

The project follows these main steps:

1. Download the 36 monthly Yellow Taxi Parquet files and the Taxi Zone Lookup CSV.
2. Inspect the source data and define explicit schemas.
3. Ingest the data into Spark and store it in partitioned Parquet format.
4. Profile the data to identify missing values and anomalies.
5. Remove trips with drop-off timestamps earlier than pickup timestamps.
6. Validate the cleaned dataset.
7. Join taxi trips with the Taxi Zone Lookup table.
8. Perform demand analysis using Spark SQL.
9. Validate the aggregated results.
10. Generate visualisations using Python and Matplotlib.

## 6. Data Cleaning

The initial dataset contained **119,136,044 trips**.

A total of **17,663 records** with drop-off timestamps earlier than pickup timestamps were removed.

The cleaned dataset contains **119,118,381 trips**.

The duplicate investigation performed on January 2022 data found no exact duplicate records across all columns. Records sharing values across six selected columns were retained because matching those fields does not necessarily mean that the records represent identical trips.

Other anomalies, such as missing passenger counts, zero distances, and negative fare amounts, were not automatically removed because they were not required for the pickup-demand analysis.

## 7. Results

The analysis produced three main results:

- **By pickup zone:** JFK Airport recorded the highest number of pickups, with **5,897,623 trips**.
- **By hour:** 6 PM (18:00) was the busiest hour, with **8,457,664 pickups**.
- **By day:** Thursday recorded the highest number of pickups, with **18,561,008 trips**.

These figures represent cumulative totals for the January 2022 – December 2024 period.

All three aggregations were validated against the total cleaned dataset of **119,118,381 trips**.

The generated figures are stored in `results/figures/`.

## 8. Project Report

The project report is available in PDF format in the `results/` directory.

- **PDF report:** [`main.pdf`](results/main.pdf)
- **LaTeX source:** [`main.tex`](results/main.tex)

## 9. Reproducibility

The project uses Docker to provide a consistent execution environment.

### Dataset acquisition

The raw datasets are included in the repository and stored using **Git LFS** due to their large size. The processed datasets are generated locally by the pipeline and are not included in the repository.

If you are cloning the repository, make sure Git LFS is installed and that the LFS files have been downloaded.

To download the datasets directly from the original source instead, run:

```powershell
python scripts/download_data.py
```

### Build the Docker image

```powershell
docker build -t nyc-taxi-spark .
```

### Run the ingestion pipeline

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/ingestion.py
```

### Run the cleaning step

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/clean_data.py
```

### Validate the cleaned dataset

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/validate_cleaned.py
```

### Join the datasets

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/join_datasets.py
```

### Run the demand analysis

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/analyze_demand.py
```

### Validate the analysis

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/validate_analysis.py
```

### Generate the figures

```powershell
docker run --rm -v "${PWD}:/app" -w /app nyc-taxi-spark python src/plot_results.py
```

