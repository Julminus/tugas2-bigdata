# Databricks notebook source
# MAGIC %md
# MAGIC # DLT · Silver layer — `silver_taxi`
# MAGIC Cleaning with the Spark DataFrame API:
# MAGIC * **Type conversion**: `try_cast` strings to timestamp/double/int (junk such as `"N/A"` becomes null instead of failing the update)
# MAGIC * **Duplicates**: `dropDuplicates` on the full business key
# MAGIC * **Missing values**: rows missing critical fields are dropped by expectations; a missing zip is filled with `0` (= unknown)
# MAGIC * **Derived features**: duration, date, hour, day-of-week (used by Gold and by the ML model)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="silver_taxi",
    comment="Cleaned, typed and de-duplicated NYC taxi trips",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({
    "valid_pickup_ts": "pickup_ts IS NOT NULL",
    "valid_dropoff_ts": "dropoff_ts IS NOT NULL AND dropoff_ts > pickup_ts",
    "valid_distance": "trip_distance IS NOT NULL AND trip_distance > 0 AND trip_distance < 100",
    "valid_fare": "fare_amount IS NOT NULL AND fare_amount > 0 AND fare_amount < 500",
    "valid_duration": "trip_duration_min <= 180",
})
def silver_taxi():
    return (
        spark.read.table("bronze_taxi")
        .select(
            F.expr("try_cast(tpep_pickup_datetime AS timestamp)").alias("pickup_ts"),
            F.expr("try_cast(tpep_dropoff_datetime AS timestamp)").alias("dropoff_ts"),
            F.expr("try_cast(trip_distance AS double)").alias("trip_distance"),
            F.expr("try_cast(fare_amount AS double)").alias("fare_amount"),
            F.expr("try_cast(pickup_zip AS int)").alias("pickup_zip"),
            F.expr("try_cast(dropoff_zip AS int)").alias("dropoff_zip"),
            "_source_file",
            "_ingested_at",
        )
        .dropDuplicates(["pickup_ts", "dropoff_ts", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"])
        .fillna({"pickup_zip": 0, "dropoff_zip": 0})
        .withColumn("trip_duration_min", F.round((F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60, 2))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_dow", F.dayofweek("pickup_ts"))
    )
