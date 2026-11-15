# Databricks notebook source
# MAGIC %md
# MAGIC # 00a · Export full dataset to one CSV (one-off)
# MAGIC Writes the whole open dataset `samples.nyctaxi.trips` (21,932 trips) to a **single CSV file** in a Volume.
# MAGIC Download that file from Catalog Explorer and put it in Google Drive; `00b_pull_from_gdrive` then pulls it back
# MAGIC into Databricks from Drive. Run this once — it does not touch the landing folder or the DLT pipeline.

# COMMAND ----------

dbutils.widgets.text("source_table", "samples.nyctaxi.trips")
dbutils.widgets.text("export_dir", "/Volumes/workspace/tugas2/raw/export")

# COMMAND ----------

import os

src = dbutils.widgets.get("source_table")
export_dir = dbutils.widgets.get("export_dir")
os.makedirs(export_dir, exist_ok=True)

pdf = spark.table(src).orderBy("tpep_pickup_datetime").toPandas()
for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
    pdf[c] = pdf[c].dt.strftime("%Y-%m-%d %H:%M:%S")

path = f"{export_dir}/nyctaxi_trips.csv"
pdf.to_csv(path, index=False)
print(f"wrote {len(pdf):,} rows, {os.path.getsize(path) / 1e6:.2f} MB -> {path}")
display(pdf.head(5))
