# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Incremental Ingestion via Autoloader
# MAGIC
# MAGIC This notebook reads new Parquet files from the landing Volume using
# MAGIC Databricks Autoloader (`cloudFiles`) and writes them, essentially
# MAGIC as-is, into a Bronze Delta table.
# MAGIC
# MAGIC Bronze layer responsibilities (and ONLY these):
# MAGIC - Preserve raw data exactly as received (schema enforcement only)
# MAGIC - Add ingestion metadata (timestamp, source file name)
# MAGIC - Track incremental progress so re-runs only process new files
# MAGIC
# MAGIC Bronze does NOT clean, filter, or validate data -- that is Silver's job.
# MAGIC This separation is deliberate: Bronze is your permanent, replayable
# MAGIC record of exactly what the source system sent you.

# COMMAND ----------

CATALOG = "nyc_taxi_catalog"
LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw_taxi_data"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/landing/checkpoints/bronze_taxi"
BRONZE_TABLE = f"{CATALOG}.bronze.taxi_trips_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read new files with Autoloader
# MAGIC `cloudFiles.format` tells Autoloader what file type to expect.
# MAGIC `cloudFiles.schemaLocation` is where Autoloader persists the schema it
# MAGIC inferred, so it doesn't have to re-infer it (and so schema drift across
# MAGIC files can be detected) on every run.

# COMMAND ----------

raw_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add ingestion metadata
# MAGIC `_ingested_at` records when Bronze processed the row (useful for
# MAGIC debugging and for Silver/Gold to reason about pipeline latency).
# MAGIC `_source_file` records which file the row came from -- handy for
# MAGIC tracing a bad record back to its origin.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# Note: input_file_name() is a legacy Spark function that Unity Catalog
# rejects (UC_COMMAND_NOT_SUPPORTED) because UC enforces its own governed
# file-lineage API instead. col("_metadata.file_path") is the UC-native
# equivalent.
bronze_df = raw_stream_df.withColumn("_ingested_at", current_timestamp()).withColumn(
    "_source_file", col("_metadata.file_path")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to the Bronze Delta table
# MAGIC `trigger(availableNow=True)` makes this behave like a batch job: it
# MAGIC processes everything currently available in the source, then stops --
# MAGIC rather than running forever like a typical streaming job. This is the
# MAGIC right pattern for a scheduled Airflow-triggered pipeline.
# MAGIC
# MAGIC `outputMode("append")` because Bronze is purely additive -- we never
# MAGIC update or delete Bronze rows, only add new ones. This is what makes
# MAGIC Bronze a reliable, replayable audit log of everything ever ingested.

# COMMAND ----------

query = (
    bronze_df.writeStream.format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/checkpoint")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print("Bronze ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

df = spark.table(BRONZE_TABLE)
print(f"Bronze table row count: {df.count()}")
display(df.limit(5))
