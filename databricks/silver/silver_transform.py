# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean, Validate, Enrich
# MAGIC
# MAGIC Silver layer responsibilities:
# MAGIC - Remove records that are clearly invalid (bad dates, impossible values)
# MAGIC - Deduplicate
# MAGIC - Enrich with the taxi zone lookup dimension table
# MAGIC - Produce a clean, analysis-ready table that Gold can safely aggregate
# MAGIC
# MAGIC We load the *entire* Bronze table each run here (batch, not streaming)
# MAGIC since Silver's transformations (dedup, joins) need to reason over the
# MAGIC full dataset, not just newly-arrived rows. For a larger production
# MAGIC dataset you'd eventually make this incremental too (e.g. using Delta
# MAGIC Change Data Feed) -- worth mentioning as a "next steps" in your README.

# COMMAND ----------

CATALOG = "nyc_taxi_catalog"
BRONZE_TABLE = f"{CATALOG}.bronze.taxi_trips_raw"
SILVER_TABLE = f"{CATALOG}.silver.taxi_trips_clean"
ZONE_LOOKUP_TABLE = f"{CATALOG}.silver.taxi_zone_lookup"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load the taxi zone lookup dimension table
# MAGIC This is a small, static reference table -- we download it once and
# MAGIC save it as its own Delta table in the silver schema. Dimension tables
# MAGIC like this typically don't need Autoloader/streaming since they change
# MAGIC rarely, if ever.

# COMMAND ----------

import pandas as pd

zone_lookup_pd = pd.read_csv(ZONE_LOOKUP_URL)
zone_lookup_df = spark.createDataFrame(zone_lookup_pd)

zone_lookup_df.write.format("delta").mode("overwrite").saveAsTable(ZONE_LOOKUP_TABLE)

print(f"Loaded {zone_lookup_df.count()} zones into {ZONE_LOOKUP_TABLE}")
display(zone_lookup_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Bronze data
# MAGIC Plain batch read -- Silver reasons over the whole table, not just new
# MAGIC arrivals.

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
print(f"Bronze row count: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Filter out clearly invalid records
# MAGIC
# MAGIC We define "valid" for this dataset as:
# MAGIC - Pickup date falls within the month we're loading (catches the 2002/
# MAGIC   2009/2023 outliers we saw during ingestion)
# MAGIC - Dropoff is after pickup (a trip can't end before it starts)
# MAGIC - Trip distance and fare amount are non-negative
# MAGIC - Passenger count is at least 1
# MAGIC
# MAGIC Rather than silently dropping rows, we count how many fail each rule --
# MAGIC this gives you real numbers to cite ("Silver layer filtered out X% of
# MAGIC records for data quality reasons") which is a great, concrete detail
# MAGIC for a project writeup or interview answer.

# COMMAND ----------

from pyspark.sql.functions import col, year, month

TARGET_YEAR = 2024
TARGET_MONTH = 1

total_rows = bronze_df.count()

valid_date_df = bronze_df.filter(
    (year(col("tpep_pickup_datetime")) == TARGET_YEAR)
    & (month(col("tpep_pickup_datetime")) == TARGET_MONTH)
)
print(f"Dropped {total_rows - valid_date_df.count()} rows with pickup dates outside {TARGET_YEAR}-{TARGET_MONTH:02d}")

valid_trip_df = valid_date_df.filter(
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    & (col("trip_distance") >= 0)
    & (col("fare_amount") >= 0)
    & (col("passenger_count") >= 1)
)
print(f"Dropped {valid_date_df.count() - valid_trip_df.count()} rows failing trip validity checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deduplicate
# MAGIC A row is considered a duplicate if every business-relevant column
# MAGIC matches -- we exclude the Bronze metadata columns (`_ingested_at`,
# MAGIC `_source_file`) from the comparison since those will always differ.

# COMMAND ----------

business_columns = [c for c in valid_trip_df.columns if not c.startswith("_")]
deduped_df = valid_trip_df.dropDuplicates(business_columns)

print(f"Dropped {valid_trip_df.count() - deduped_df.count()} exact duplicate rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enrich with zone names
# MAGIC Join twice against the zone lookup -- once for pickup location, once
# MAGIC for dropoff -- so both ends of the trip get readable borough/zone
# MAGIC names. We alias columns to avoid ambiguity after two joins against the
# MAGIC same lookup table.

# COMMAND ----------

pickup_zones = zone_lookup_df.select(
    col("LocationID").alias("PULocationID"),
    col("Borough").alias("pickup_borough"),
    col("Zone").alias("pickup_zone"),
)

dropoff_zones = zone_lookup_df.select(
    col("LocationID").alias("DOLocationID"),
    col("Borough").alias("dropoff_borough"),
    col("Zone").alias("dropoff_zone"),
)

enriched_df = (
    deduped_df.join(pickup_zones, on="PULocationID", how="left")
    .join(dropoff_zones, on="DOLocationID", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write the Silver table
# MAGIC Full overwrite for now, matching our batch (non-incremental) approach
# MAGIC above. As noted, making this incremental is a natural "v2" enhancement
# MAGIC once the pipeline works end to end.

# COMMAND ----------

enriched_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"Silver table written: {SILVER_TABLE}")
print(f"Final row count: {enriched_df.count()}")
display(enriched_df.limit(5))
