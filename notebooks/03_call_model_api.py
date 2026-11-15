# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Call model API (post-pipeline service call)
# MAGIC Scores the most recently ingested Silver rows through the Model Serving REST endpoint
# MAGIC and appends actual vs. predicted fares to `taxi_predictions`. Retries cover scale-from-zero cold starts.

# COMMAND ----------

dbutils.widgets.text("endpoint_name", "taxi-fare-endpoint")
dbutils.widgets.text("silver_table", "workspace.tugas2.silver_taxi")
dbutils.widgets.text("predictions_table", "workspace.tugas2.taxi_predictions")

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

endpoint = dbutils.widgets.get("endpoint_name")
FEATURES = ["trip_distance", "trip_duration_min", "pickup_hour", "pickup_dow", "pickup_zip", "dropoff_zip"]

rows = (spark.table(dbutils.widgets.get("silver_table"))
        .orderBy(F.col("_ingested_at").desc(), F.col("pickup_ts").desc())
        .limit(10).toPandas())
X = rows[FEATURES].astype("float64")

w = WorkspaceClient()
for attempt in range(1, 11):
    try:
        resp = w.api_client.do("POST", f"/serving-endpoints/{endpoint}/invocations",
                               body={"dataframe_split": {"columns": FEATURES, "data": X.values.tolist()}})
        break
    except Exception as e:
        if attempt == 10:
            raise
        print(f"attempt {attempt} failed ({str(e)[:120]}); endpoint may be scaling from zero, retry in 30s")
        time.sleep(30)

out = rows[["pickup_ts", "fare_amount", *FEATURES, "_source_file"]].assign(
    predicted_fare=[float(p) for p in resp["predictions"]], endpoint=endpoint)
(spark.createDataFrame(out).withColumn("scored_at", F.current_timestamp())
 .write.mode("append").option("mergeSchema", "true").saveAsTable(dbutils.widgets.get("predictions_table")))
display(out)
