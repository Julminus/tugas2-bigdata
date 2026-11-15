# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Feed new CSV batch (simulated incremental source)
# MAGIC Each run drops **new** CSV file(s) into the landing Volume, replaying the open dataset
# MAGIC `samples.nyctaxi.trips` one pickup-day per file. After the 60 source days are used up the replay
# MAGIC loops, shifted forward by 9 whole weeks, so the source never runs dry and weekday patterns stay intact.
# MAGIC A few dirty rows (nulls, junk strings, duplicates) are injected on purpose so the Silver layer has real cleaning to do.

# COMMAND ----------

dbutils.widgets.text("source_table", "samples.nyctaxi.trips")
dbutils.widgets.text("landing_path", "/Volumes/workspace/tugas2/raw/landing")
dbutils.widgets.text("days", "1")

# COMMAND ----------

import datetime as dt
import os

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

src = dbutils.widgets.get("source_table")
landing = dbutils.widgets.get("landing_path")
days = int(dbutils.widgets.get("days"))
os.makedirs(landing, exist_ok=True)

trips = spark.table(src)
b = trips.agg(F.min(F.to_date("tpep_pickup_datetime")).alias("lo"), F.max(F.to_date("tpep_pickup_datetime")).alias("hi")).first()
span = (b.hi - b.lo).days + 1
cycle = -(-span // 7) * 7  # replay period rounded up to whole weeks -> weekday patterns survive the wrap
done = len([f for f in os.listdir(landing) if f.endswith(".csv")])  # files already delivered = replay cursor

written = []
for n in range(done, done + days):
    day = b.lo + dt.timedelta(days=n % span)
    shift = pd.Timedelta(days=(n // span) * cycle)
    pdf = trips.where(F.to_date("tpep_pickup_datetime") == F.lit(day)).toPandas()
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        pdf[c] = (pdf[c] + shift).dt.strftime("%Y-%m-%d %H:%M:%S")
    pdf = pdf.astype(object)  # keep ints as ints when injecting None

    rng = np.random.default_rng(n)
    idx = rng.choice(len(pdf), size=12, replace=False)
    pdf.loc[idx[:4], "fare_amount"] = None      # missing critical value -> dropped in Silver
    pdf.loc[idx[4:8], "trip_distance"] = "N/A"  # junk string -> try_cast null -> dropped in Silver
    pdf.loc[idx[8:], "pickup_zip"] = None       # missing non-critical value -> filled in Silver
    pdf = pd.concat([pdf, pdf.sample(8, random_state=n)])  # exact duplicates -> removed in Silver

    path = f"{landing}/taxi_{n:05d}_{(day + dt.timedelta(days=shift.days)).isoformat()}.csv"
    pdf.to_csv(path, index=False)
    written.append(path)
    print(f"wrote {len(pdf):>4} rows -> {path}")

dbutils.notebook.exit(",".join(written))
