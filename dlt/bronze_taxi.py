# Databricks notebook source
# MAGIC %md
# MAGIC # DLT · Bronze layer — `bronze_taxi`
# MAGIC Incremental ingestion of raw CSV files from the landing Volume with **Auto Loader** (`cloudFiles`).
# MAGIC Auto Loader checkpoints which files were already processed, so every pipeline update reads **only new files**
# MAGIC (append, no full overwrite). The path comes from the pipeline configuration key `source_path` (no hardcoded path).
# MAGIC All columns stay as raw strings; typing happens in Silver.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="bronze_taxi",
    comment="Raw NYC taxi trips, incrementally ingested from CSV files via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_taxi():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(spark.conf.get("source_path"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
