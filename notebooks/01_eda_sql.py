# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Exploratory data analysis with Spark SQL
# MAGIC Explores the NYC taxi dataset **pulled from Google Drive** (`workspace.tugas2.gdrive_taxi_trips`, loaded by `00b_pull_from_gdrive`)
# MAGIC before it goes through the DLT pipeline. Every finding is saved as a **SQL table** (`workspace.tugas2.eda_*`) created with Spark,
# MAGIC so the results can be queried later and reused in the SQL editor or the dashboard.
# MAGIC
# MAGIC | Table | What it answers |
# MAGIC |---|---|
# MAGIC | `eda_overview` | How big is the dataset, what period does it cover? |
# MAGIC | `eda_column_profile` | Nulls, min/max/mean/stddev per column |
# MAGIC | `eda_data_quality` | How many rows break the rules the Silver layer enforces? |
# MAGIC | `eda_fare_distribution` | How are fares distributed? |
# MAGIC | `eda_distance_bands` | How do fare, duration and speed change with distance? |
# MAGIC | `eda_hour_weekday` | When do trips happen, and does the time change the fare? |
# MAGIC | `eda_top_zips` | Busiest pickup and dropoff zip codes |
# MAGIC | `eda_feature_correlation` | Which features relate most to the fare (input for the ML model)? |

# COMMAND ----------

dbutils.widgets.text("source_table", "workspace.tugas2.gdrive_taxi_trips")
dbutils.widgets.text("target_schema", "workspace.tugas2")
src = dbutils.widgets.get("source_table")
tgt = dbutils.widgets.get("target_schema")

# Temp view with the derived columns used throughout the analysis
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW trips AS
SELECT *,
  ROUND((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60, 2) AS trip_duration_min,
  hour(tpep_pickup_datetime) AS pickup_hour,
  dayofweek(tpep_pickup_datetime) AS pickup_dow,
  to_date(tpep_pickup_datetime) AS pickup_date
FROM {src}
""")
display(spark.sql("SELECT * FROM trips LIMIT 10"))

# COMMAND ----------

# MAGIC %md ## 1 · Overview — size and time range

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt}.eda_overview AS
SELECT COUNT(*) AS trips,
       COUNT(DISTINCT pickup_date) AS days,
       MIN(tpep_pickup_datetime) AS first_pickup,
       MAX(tpep_pickup_datetime) AS last_pickup,
       ROUND(SUM(fare_amount), 2) AS total_fare,
       ROUND(AVG(fare_amount), 2) AS avg_fare,
       ROUND(AVG(trip_distance), 2) AS avg_distance_mi,
       ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM trips
""")
display(spark.table(f"{tgt}.eda_overview"))

# COMMAND ----------

# MAGIC %md ## 2 · Column profile — nulls and value ranges

# COMMAND ----------

num_cols = ["trip_distance", "fare_amount", "trip_duration_min", "pickup_zip", "dropoff_zip", "pickup_hour", "pickup_dow"]
profile_sql = "\nUNION ALL\n".join(
    f"""SELECT '{c}' AS column_name, COUNT(*) - COUNT({c}) AS nulls, ROUND(MIN({c}), 2) AS min, ROUND(MAX({c}), 2) AS max,
       ROUND(AVG({c}), 2) AS mean, ROUND(STDDEV({c}), 2) AS stddev, COUNT(DISTINCT {c}) AS distinct_values FROM trips"""
    for c in num_cols
)
spark.sql(f"CREATE OR REPLACE TABLE {tgt}.eda_column_profile AS {profile_sql}")
display(spark.table(f"{tgt}.eda_column_profile"))

# COMMAND ----------

# MAGIC %md ## 3 · Data quality — rows the Silver layer will drop or fix
# MAGIC These are the same rules as the expectations in `dlt/silver_taxi.py`, so this table explains *why* Silver has fewer rows than Bronze.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt}.eda_data_quality AS
SELECT rule, violations, ROUND(100 * violations / total, 3) AS pct_of_rows, action_in_silver FROM (
  SELECT COUNT(*) AS total,
    COUNT_IF(tpep_pickup_datetime IS NULL) AS missing_pickup_time,
    COUNT_IF(tpep_dropoff_datetime IS NULL OR tpep_dropoff_datetime <= tpep_pickup_datetime) AS invalid_dropoff_time,
    COUNT_IF(trip_distance IS NULL OR trip_distance <= 0 OR trip_distance >= 100) AS invalid_distance,
    COUNT_IF(fare_amount IS NULL OR fare_amount <= 0 OR fare_amount >= 500) AS invalid_fare,
    COUNT_IF(trip_duration_min > 180) AS duration_over_3h,
    COUNT_IF(pickup_zip IS NULL OR dropoff_zip IS NULL) AS missing_zip,
    COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {src})) AS exact_duplicates
  FROM trips
) s LATERAL VIEW STACK(7,
  'missing pickup time', missing_pickup_time, 'drop row',
  'dropoff missing or before pickup', invalid_dropoff_time, 'drop row',
  'distance <= 0 or >= 100 mi', invalid_distance, 'drop row',
  'fare <= 0 or >= 500 USD', invalid_fare, 'drop row',
  'duration > 180 min', duration_over_3h, 'drop row',
  'missing pickup/dropoff zip', missing_zip, 'fill with 0',
  'exact duplicate rows', exact_duplicates, 'dropDuplicates'
) q AS rule, violations, action_in_silver
""")
display(spark.table(f"{tgt}.eda_data_quality"))

# COMMAND ----------

# MAGIC %md ## 4 · Fare distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.tugas2.eda_fare_distribution AS
# MAGIC SELECT CASE WHEN fare_amount < 5 THEN '01) under 5 USD' WHEN fare_amount < 10 THEN '02) 5–10 USD'
# MAGIC             WHEN fare_amount < 15 THEN '03) 10–15 USD' WHEN fare_amount < 20 THEN '04) 15–20 USD'
# MAGIC             WHEN fare_amount < 30 THEN '05) 20–30 USD' WHEN fare_amount < 50 THEN '06) 30–50 USD'
# MAGIC             ELSE '07) 50+ USD' END AS fare_band,
# MAGIC        COUNT(*) AS trips,
# MAGIC        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips
# MAGIC FROM workspace.tugas2.gdrive_taxi_trips
# MAGIC WHERE fare_amount IS NOT NULL
# MAGIC GROUP BY 1 ORDER BY 1;
# MAGIC
# MAGIC SELECT * FROM workspace.tugas2.eda_fare_distribution;

# COMMAND ----------

# MAGIC %md ## 5 · Distance bands — fare, duration and speed

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt}.eda_distance_bands AS
SELECT CASE WHEN trip_distance < 1 THEN '1) under 1 mi' WHEN trip_distance < 3 THEN '2) 1–3 mi'
            WHEN trip_distance < 6 THEN '3) 3–6 mi'     WHEN trip_distance < 10 THEN '4) 6–10 mi'
            ELSE '5) 10+ mi' END AS distance_band,
       COUNT(*) AS trips,
       ROUND(AVG(fare_amount), 2) AS avg_fare,
       ROUND(AVG(trip_duration_min), 1) AS avg_duration_min,
       ROUND(AVG(fare_amount / trip_distance), 2) AS avg_fare_per_mile,
       ROUND(AVG(trip_distance / (trip_duration_min / 60)), 1) AS avg_speed_mph
FROM trips
WHERE trip_distance > 0 AND trip_duration_min > 0 AND fare_amount > 0
GROUP BY 1 ORDER BY 1
""")
display(spark.table(f"{tgt}.eda_distance_bands"))

# COMMAND ----------

# MAGIC %md ## 6 · Time patterns — hour of day × day of week

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt}.eda_hour_weekday AS
SELECT pickup_dow,
       element_at(array('Sun','Mon','Tue','Wed','Thu','Fri','Sat'), pickup_dow) AS weekday,
       pickup_hour,
       COUNT(*) AS trips,
       ROUND(AVG(fare_amount), 2) AS avg_fare,
       ROUND(AVG(trip_distance / (trip_duration_min / 60)), 1) AS avg_speed_mph
FROM trips
WHERE trip_distance > 0 AND trip_duration_min > 0
GROUP BY pickup_dow, pickup_hour
ORDER BY pickup_dow, pickup_hour
""")
display(spark.sql(f"""
SELECT weekday, SUM(trips) AS trips, ROUND(SUM(avg_fare * trips) / SUM(trips), 2) AS avg_fare
FROM {tgt}.eda_hour_weekday GROUP BY pickup_dow, weekday ORDER BY pickup_dow
"""))

# COMMAND ----------

# MAGIC %md ## 7 · Busiest zip codes

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt}.eda_top_zips AS
SELECT * FROM (
  SELECT 'pickup' AS direction, pickup_zip AS zip, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare,
         ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM trips WHERE pickup_zip IS NOT NULL GROUP BY pickup_zip
  UNION ALL
  SELECT 'dropoff', dropoff_zip, COUNT(*), ROUND(AVG(fare_amount), 2), ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)
  FROM trips WHERE dropoff_zip IS NOT NULL GROUP BY dropoff_zip
) WHERE rank <= 10
ORDER BY direction DESC, rank
""")
display(spark.table(f"{tgt}.eda_top_zips"))

# COMMAND ----------

# MAGIC %md ## 8 · Feature correlation with the fare
# MAGIC Guides feature selection for `02_train_model`: distance and duration dominate; hour and weekday barely relate to the fare.
# MAGIC Zip codes are labels, not quantities — their correlation mostly reflects that the airport zips (11430 JFK, 11371 LGA)
# MAGIC are numerically large and have high fares, so treat those two numbers with care.

# COMMAND ----------

features = ["trip_distance", "trip_duration_min", "pickup_hour", "pickup_dow", "pickup_zip", "dropoff_zip"]
corr_sql = "\nUNION ALL\n".join(
    f"SELECT '{f}' AS feature, ROUND(corr({f}, fare_amount), 3) AS corr_with_fare FROM trips "
    f"WHERE trip_distance > 0 AND trip_distance < 100 AND fare_amount > 0 AND fare_amount < 500 AND trip_duration_min BETWEEN 0 AND 180"
    for f in features
)
spark.sql(f"CREATE OR REPLACE TABLE {tgt}.eda_feature_correlation AS SELECT * FROM ({corr_sql}) ORDER BY abs(corr_with_fare) DESC")
display(spark.table(f"{tgt}.eda_feature_correlation"))

# COMMAND ----------

# MAGIC %md ## Summary — tables created

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {tgt} LIKE 'eda_*'"))
