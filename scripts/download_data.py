
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import csv
import time

# Project directories
PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Official NYC TLC download location
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

# Download years and months
YEARS = range(2022, 2025)
MONTHS = range(1, 13)

# Download report
manifest_path = RAW_DIR / "download_manifest.csv"
manifest = []

def download_file(url, destination):
    """Download a file, retrying if a connection fails."""

    if destination.exists() and destination.stat().st_size > 0:
        print(f"Already downloaded: {destination.name}")
        return "already_exists"

    temporary_file = destination.with_suffix(destination.suffix + ".part")

    for attempt in range(1, 4):
        try:
            print(f"Downloading {destination.name} (attempt {attempt}/3)...")

            request = Request(
                url,
                headers={"User-Agent": "NYC-Taxi-Volume-Project/1.0"}
            )

            with urlopen(request, timeout=120) as response:
                with open(temporary_file, "wb") as output:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)

            if temporary_file.stat().st_size == 0:
                raise ValueError("Downloaded file is empty")

            temporary_file.replace(destination)
            print(f"Downloaded: {destination.name}")
            return "downloaded"

        except Exception as error:
            print(f"Error: {error}")

            if temporary_file.exists():
                temporary_file.unlink()

            if attempt < 3:
                time.sleep(3)

    raise RuntimeError(f"Failed to download {url}")


# Download monthly Yellow Taxi Parquet files
for year in YEARS:
    for month in MONTHS:
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{BASE_URL}/trip-data/{filename}"
        destination = RAW_DIR / filename

        status = download_file(url, destination)

        manifest.append({
            "filename": filename,
            "status": status,
            "size_bytes": destination.stat().st_size
        })


# Download Taxi Zone Lookup CSV
lookup_filename = "taxi_zone_lookup.csv"
lookup_url = f"{BASE_URL}/misc/taxi_zone_lookup.csv"
lookup_destination = RAW_DIR / lookup_filename

status = download_file(lookup_url, lookup_destination)

manifest.append({
    "filename": lookup_filename,
    "status": status,
    "size_bytes": lookup_destination.stat().st_size
})


# Save download manifest
with open(manifest_path, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=["filename", "status", "size_bytes"]
    )
    writer.writeheader()
    writer.writerows(manifest)


# Calculate total size of downloaded files
total_bytes = sum(item["size_bytes"] for item in manifest)
total_gb = total_bytes / (1024 ** 3)

print("\n========== DOWNLOAD SUMMARY ==========")
print(f"Files recorded: {len(manifest)}")
print(f"Total size: {total_gb:.3f} GB")
print(f"Manifest saved to: {manifest_path}")
print("======================================")
