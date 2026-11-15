# Databricks notebook source
# MAGIC %md
# MAGIC # DLT · Gold layer — `gold_taxi`
# MAGIC Analytics-ready KPIs aggregated per pickup date and hour; read by the dashboard (SQL warehouse).

# COMMAND ----------

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_taxi",
    comment="Hourly NYC taxi KPIs (trips, revenue, averages), dashboard-ready",
    table_properties={"quality": "gold"},
)
def gold_taxi():
    return (
        spark.read.table("silver_taxi")
        .groupBy("pickup_date", "pickup_hour", "pickup_dow")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.sum("fare_amount") / F.sum("trip_distance"), 2).alias("fare_per_mile"),
            F.max("_ingested_at").alias("last_ingested_at"),
        )
    )
