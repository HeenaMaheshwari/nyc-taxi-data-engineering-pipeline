# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion: NYC Taxi Data -> Landing Volume
# MAGIC
# MAGIC This notebook simulates a real-world "daily batch drop" pattern:
# MAGIC 1. Downloads one month of NYC Yellow Taxi trip data (public, no auth needed)
# MAGIC 2. Splits it into daily chunks based on pickup date
# MAGIC 3. Writes each day as a separate Parquet file into the landing Volume
# MAGIC
# MAGIC Autoloader (Step 4, Bronze layer) will pick these files up incrementally,
# MAGIC exactly like it would with files landing daily in a production system.

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

import requests
import pandas as pd
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC Change `MONTHS_TO_LOAD` to pull more months later. Start with one month
# MAGIC while you build out the rest of the pipeline -- easier to debug with a
# MAGIC smaller volume of data.

# COMMAND ----------

CATALOG = "nyc_taxi_catalog"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/raw_taxi_data"

# Each entry is a (year, month) to download from the NYC TLC public dataset
MONTHS_TO_LOAD = [
    (2024, 1),
]

TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download the raw monthly file
# MAGIC We download to the notebook's local temp storage first, then process it.
# MAGIC This keeps network I/O separate from the Volume write step, which makes
# MAGIC debugging easier if either step fails.

# COMMAND ----------

def download_month(year: int, month: int) -> str:
    url = TLC_URL_TEMPLATE.format(year=year, month=month)
    local_path = f"/tmp/yellow_tripdata_{year}-{month:02d}.parquet"

    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Saved to {local_path}")
    return local_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Split into daily chunks and write to the Volume
# MAGIC We read the monthly Parquet file, group rows by the *date* portion of
# MAGIC `tpep_pickup_datetime`, and write one Parquet file per day. This is what
# MAGIC lets us simulate "new data arriving daily" even though the source is a
# MAGIC single monthly file.
# MAGIC
# MAGIC Note: a real production pickup-time column can contain a small number of
# MAGIC bad/out-of-range dates (e.g. year 2003 typos in the source system). We
# MAGIC don't silently drop them here -- filtering bad records is a Silver-layer
# MAGIC job, not an ingestion-layer job. Ingestion's only responsibility is to
# MAGIC faithfully land what the source gave us.

# COMMAND ----------

def split_and_upload(local_path: str, year: int, month: int):
    df = pd.read_parquet(local_path)

    df["pickup_date"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.date

    unique_dates = sorted(df["pickup_date"].unique())
    print(f"Found {len(unique_dates)} distinct pickup dates in {year}-{month:02d}")

    for pickup_date in unique_dates:
        daily_df = df[df["pickup_date"] == pickup_date].drop(columns=["pickup_date"])

        file_name = f"taxi_{pickup_date}.parquet"
        volume_file_path = f"{LANDING_VOLUME_PATH}/{file_name}"

        daily_df.to_parquet(volume_file_path, index=False)
        print(f"  Wrote {len(daily_df):>6} rows -> {volume_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run it
# MAGIC This loops over every (year, month) configured above. Re-running this
# MAGIC notebook for a month you've already loaded will just overwrite that
# MAGIC month's daily files -- safe to re-run while you're developing.

# COMMAND ----------

for year, month in MONTHS_TO_LOAD:
    local_path = download_month(year, month)
    split_and_upload(local_path, year, month)

print("Ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC List what landed in the Volume to confirm the daily files are there.

# COMMAND ----------

files = dbutils.fs.ls(LANDING_VOLUME_PATH)
print(f"{len(files)} files in landing volume:")
for f in files[:10]:
    print(" ", f.name)
