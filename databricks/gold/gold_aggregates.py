# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Business Aggregates
# MAGIC
# MAGIC Gold layer responsibilities:
# MAGIC - Pre-aggregate Silver data into purpose-built tables
# MAGIC - Each table answers a specific business question
# MAGIC - These are what the dashboard (and later, Genie) will query directly
# MAGIC
# MAGIC We build four Gold tables here, each looking at the trip data from a
# MAGIC different angle: time trend, spatial (zone), time-of-day pattern, and
# MAGIC categorical (payment type) breakdown.

# COMMAND ----------

CATALOG = "nyc_taxi_catalog"
SILVER_TABLE = f"{CATALOG}.silver.taxi_trips_clean"

DAILY_SUMMARY_TABLE = f"{CATALOG}.gold.daily_summary"
REVENUE_BY_ZONE_TABLE = f"{CATALOG}.gold.revenue_by_zone"
HOURLY_PATTERNS_TABLE = f"{CATALOG}.gold.hourly_patterns"
PAYMENT_BREAKDOWN_TABLE = f"{CATALOG}.gold.payment_type_breakdown"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
print(f"Silver row count: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 1: Daily summary
# MAGIC One row per calendar day: trip count, total revenue, average fare,
# MAGIC average distance. This is the table a "trend over time" line chart on
# MAGIC your dashboard will read from.

# COMMAND ----------

from pyspark.sql.functions import to_date, count, sum as _sum, avg, round as _round

daily_summary_df = (
    silver_df.withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("trip_date")
    .agg(
        count("*").alias("trip_count"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(avg("fare_amount"), 2).alias("avg_fare"),
        _round(avg("trip_distance"), 2).alias("avg_distance"),
    )
    .orderBy("trip_date")
)

daily_summary_df.write.format("delta").mode("overwrite").saveAsTable(DAILY_SUMMARY_TABLE)
print(f"Wrote {daily_summary_df.count()} rows to {DAILY_SUMMARY_TABLE}")
display(daily_summary_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 2: Revenue by zone
# MAGIC One row per pickup borough + zone: trip count, total revenue, average
# MAGIC fare. This powers a "where is demand/revenue coming from" view --
# MAGIC useful for a map or bar chart on the dashboard.

# COMMAND ----------

revenue_by_zone_df = (
    silver_df.groupBy("pickup_borough", "pickup_zone")
    .agg(
        count("*").alias("trip_count"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(avg("fare_amount"), 2).alias("avg_fare"),
    )
    .orderBy(col("total_revenue").desc())
)

revenue_by_zone_df.write.format("delta").mode("overwrite").saveAsTable(REVENUE_BY_ZONE_TABLE)
print(f"Wrote {revenue_by_zone_df.count()} rows to {REVENUE_BY_ZONE_TABLE}")
display(revenue_by_zone_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 3: Hourly patterns
# MAGIC One row per hour of day (0-23): trip count and average fare, averaged
# MAGIC across all days in the dataset. This shows demand patterns -- e.g.
# MAGIC rush hour spikes -- a classic "operational" insight taxi/rideshare
# MAGIC companies actually use for driver allocation.

# COMMAND ----------

from pyspark.sql.functions import hour

hourly_patterns_df = (
    silver_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour")
    .agg(
        count("*").alias("trip_count"),
        _round(avg("fare_amount"), 2).alias("avg_fare"),
    )
    .orderBy("pickup_hour")
)

hourly_patterns_df.write.format("delta").mode("overwrite").saveAsTable(HOURLY_PATTERNS_TABLE)
print(f"Wrote {hourly_patterns_df.count()} rows to {HOURLY_PATTERNS_TABLE}")
display(hourly_patterns_df.limit(24))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 4: Payment type breakdown
# MAGIC One row per payment type: trip count, total revenue, average tip.
# MAGIC NYC TLC encodes payment_type as an integer code -- we map it to
# MAGIC readable labels here since Gold tables should be self-explanatory to
# MAGIC whoever reads them on the dashboard, not require a lookup elsewhere.

# COMMAND ----------

from pyspark.sql.functions import when

payment_labeled_df = silver_df.withColumn(
    "payment_type_label",
    when(col("payment_type") == 1, "Credit card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided trip")
    .otherwise("Other"),
)

payment_breakdown_df = (
    payment_labeled_df.groupBy("payment_type_label")
    .agg(
        count("*").alias("trip_count"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(avg("tip_amount"), 2).alias("avg_tip"),
    )
    .orderBy(col("trip_count").desc())
)

payment_breakdown_df.write.format("delta").mode("overwrite").saveAsTable(PAYMENT_BREAKDOWN_TABLE)
print(f"Wrote {payment_breakdown_df.count()} rows to {PAYMENT_BREAKDOWN_TABLE}")
display(payment_breakdown_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC All four Gold tables are now available under `nyc_taxi_catalog.gold`:
# MAGIC - `daily_summary` -- time trend
# MAGIC - `revenue_by_zone` -- spatial breakdown
# MAGIC - `hourly_patterns` -- time-of-day demand
# MAGIC - `payment_type_breakdown` -- categorical breakdown
# MAGIC
# MAGIC These are what the dashboard (Step 6) and Genie (Step 9) will query.
