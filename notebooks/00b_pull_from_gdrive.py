# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Pull dataset from Google Drive → Databricks
# MAGIC Downloads the NYC taxi CSV stored in Google Drive into the Volume `raw/gdrive/`, then loads it into the
# MAGIC Delta table `workspace.tugas2.gdrive_taxi_trips`.
# MAGIC
# MAGIC **Drive setup:** the file must be shared as *Anyone with the link → Viewer*. Put its file ID (the part between
# MAGIC `/d/` and `/view` in the share link) in the `gdrive_file_id` widget. No credentials are stored here.
# MAGIC
# MAGIC Separate from the scheduled job: it does not write to the landing folder, so `bronze_taxi` is unaffected.

# COMMAND ----------

dbutils.widgets.text("gdrive_file_id", "1qdFdNSagZw27kni-QjAXtlHG0ZrtgDsx")  # My Drive/tugas2/nyctaxi_trips.csv
dbutils.widgets.text("target_dir", "/Volumes/workspace/tugas2/raw/gdrive")
dbutils.widgets.text("target_table", "workspace.tugas2.gdrive_taxi_trips")

# COMMAND ----------

import hashlib
import os

import requests
from pyspark.sql import functions as F

file_id = dbutils.widgets.get("gdrive_file_id").strip()
target_dir = dbutils.widgets.get("target_dir")
target_table = dbutils.widgets.get("target_table")
assert file_id, "Set the gdrive_file_id widget (from the Drive share link)."

EXPECTED_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"]

# COMMAND ----------

# MAGIC %md ### 1 · Download from Google Drive

# COMMAND ----------

try:
    r = requests.get(
        "https://drive.usercontent.google.com/download",
        params={"id": file_id, "export": "download", "confirm": "t"},  # confirm=t skips the large-file warning page
        timeout=120,
    )
except requests.exceptions.RequestException as e:
    raise RuntimeError(
        "Could not reach Google Drive from this workspace. Databricks Free Edition limits outbound internet to "
        "trusted domains. Fallback: upload the CSV to the Volume by hand (Catalog -> workspace.tugas2 -> raw -> "
        f"Upload) at {target_dir}/nyctaxi_trips.csv and run from step 2."
    ) from e

r.raise_for_status()
body = r.content
if body.lstrip()[:1] == b"<":  # Drive returns an HTML page instead of the file when it is not shared publicly
    raise RuntimeError("Drive returned HTML, not CSV: share the file as 'Anyone with the link -> Viewer' and check the file ID.")

header = body.split(b"\n", 1)[0].decode().strip().split(",")
missing = [c for c in EXPECTED_COLS if c not in header]
assert not missing, f"CSV is missing columns {missing}; header was {header}"

os.makedirs(target_dir, exist_ok=True)
path = f"{target_dir}/nyctaxi_trips.csv"
with open(path, "wb") as f:
    f.write(body)
print(f"downloaded {len(body) / 1e6:.2f} MB, md5 {hashlib.md5(body).hexdigest()} -> {path}")

# COMMAND ----------

# MAGIC %md ### 2 · Load into a Delta table

# COMMAND ----------

df = (
    spark.read.option("header", "true").csv(path)
    .select(
        F.to_timestamp("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        F.to_timestamp("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("pickup_zip").cast("int").alias("pickup_zip"),
        F.col("dropoff_zip").cast("int").alias("dropoff_zip"),
    )
    .withColumn("_source", F.lit(f"gdrive:{file_id}"))
    .withColumn("_pulled_at", F.current_timestamp())
)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

n = spark.table(target_table).count()
print(f"{target_table}: {n:,} rows")
display(spark.table(target_table).limit(5))
