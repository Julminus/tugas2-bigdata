# Tugas 2 — NYC Taxi Lakehouse: Documentation

End-to-end Big Data project on **Databricks Free Edition**: incremental ETL (Bronze → Silver → Gold), a fare-prediction model with MLflow, a REST API via Model Serving, a dashboard, a scheduled job, a Google Drive data source, and Postman API tests.

> Detailed rubric evidence and the Indonesian write-up live in [`README.md`](README.md). This file is the quick technical overview.

---

## 1. Architecture

```
samples.nyctaxi.trips ─(00a export, one-off)─▶ Google Drive: My Drive/tugas2/nyctaxi_trips.csv   ◀── external source
                                                       │ HTTPS, public link
                                                       ▼
                                  00b_pull_from_gdrive (job task 1) ─▶ gdrive_taxi_trips (Delta) ─▶ 01_eda_sql ─▶ eda_* SQL tables
                                                                              │
                                                                              │ 00_feed_new_csv (job task 2: 1 day per run + dirty rows)
                                                                              ▼
                                                     /Volumes/workspace/tugas2/raw/landing/*.csv
        │ Auto Loader (incremental)
        ▼
bronze_taxi ─▶ silver_taxi (clean + features) ─▶ gold_taxi (date × hour KPIs) ─▶ v_taxi_daily_kpi ─▶ Dashboard
                    │
                    └─▶ 02_train_model (MLflow) ─▶ taxi_fare_model@champion ─▶ taxi-fare-endpoint (REST API)
                                                          │                         ▲
                                                          └─▶ ONNX (02b)            │
                                         03_call_model_api ─────────────────────────┤
                                         Postman collection (incl. trips from Drive)┘

Job tugas2_taxi_lakehouse_job (every 2 min): pull_from_gdrive ─▶ feed_new_csv ─▶ DLT pipeline ─▶ call_model_api
```

| Item | Value |
|---|---|
| Platform | Databricks Free Edition (serverless only) |
| Catalog / schema | `workspace.tugas2` |
| Dataset | NYC taxi trips (open dataset `samples.nyctaxi.trips`, 21,932 trips, Jan–Feb 2016), served to the pipeline from **Google Drive** |
| Use case | Regression on `fare_amount` + trip KPI dashboard |
| Workspace host | `https://dbc-ee445d02-3afe.cloud.databricks.com` |

## 2. Repository layout

| Path | Purpose |
|---|---|
| `dlt/bronze_taxi.py` | Bronze streaming table, Auto Loader over the landing Volume (`source_path` pipeline config) |
| `dlt/silver_taxi.py` | Typing (`try_cast`), dedup, null handling, expectations, derived features |
| `dlt/gold_taxi.py` | Aggregates per `pickup_date` × `pickup_hour` |
| `notebooks/00_feed_new_csv.py` | Simulated incremental source: takes one day from the Drive table (`source_table`) per run and writes it as a new CSV, with injected dirty rows |
| `notebooks/00a_export_full_csv.py` | One-off export of the full dataset to a single CSV (the file uploaded to Drive) |
| `notebooks/00b_pull_from_gdrive.py` | Pulls the CSV from Google Drive into a Volume and a Delta table |
| `notebooks/01_eda_sql.py` | Exploratory analysis with Spark SQL on the Drive data; saves 8 `eda_*` SQL tables |
| `notebooks/02_train_model.py` | Linear Regression vs Random Forest, MLflow tracking, UC registration with alias `champion` |
| `notebooks/02b_export_onnx.py` | ONNX export of the best model + ONNX Runtime check |
| `notebooks/03_call_model_api.py` | Post-pipeline scoring through the endpoint → `taxi_predictions` |
| `job/job_config.json` | Databricks Job definition (exported while schedule was PAUSED) |
| `dashboard/tugas2_taxi_dashboard.lvdash.json` | AI/BI dashboard definition (Overview + ML Model pages) |
| `postman/Tugas2_Taxi_Fare_API.postman_collection.json` | API test collection (5 requests) |
| `postman/Postman-Report.html` | Latest newman run report |
| `postman/POSTMAN_GUIDE.md` | How to run the API tests in Postman: variables, request bodies, troubleshooting |

## 3. Exploratory analysis (SQL)

`01_eda_sql` explores `gdrive_taxi_trips` with Spark SQL (`spark.sql` and `%sql` cells) and saves each result as a table in `workspace.tugas2`:

| Table | Key finding (15 Nov 2026 run) |
|---|---|
| `eda_overview` | 21,932 trips over 60 days (Jan 1 – Feb 29, 2016); average fare $12.35, 2.85 mi, 15.1 min |
| `eda_column_profile` | Nulls, min/max/mean/stddev/distinct count per column |
| `eda_data_quality` | Rows that break the Silver rules: 76 with an invalid distance, 33 over 3 hours, 10 with an invalid fare, 1 dropoff before pickup; no nulls or duplicates in the source |
| `eda_fare_distribution` | 45.5% of trips cost 5–10 USD; 2.4% cost 50+ USD |
| `eda_distance_bands` | Fare per mile falls from $9.39 (under 1 mi) to $3.04 (10+ mi); speed rises from 9 to 31 mph |
| `eda_hour_weekday` | Trips, average fare and speed per weekday × hour |
| `eda_top_zips` | 10 busiest pickup and dropoff zips |
| `eda_feature_correlation` | Correlation with fare: distance 0.947, duration 0.857, hour −0.011, weekday −0.004 (zip correlations mostly reflect high-numbered airport zips) |

These findings shaped the next steps: the data-quality rules became the Silver expectations, and the correlations explain why distance and duration dominate the model.

## 4. Data pipeline (DLT `tugas2_taxi_pipeline`)

**Bronze — `bronze_taxi`**: Auto Loader (`cloudFiles`, CSV, all columns as strings) reads only new files from `/Volumes/workspace/tugas2/raw/landing`; adds `_source_file`, `_ingested_at`. Append-only, no full overwrite.

**Silver — `silver_taxi`**: `try_cast` to timestamp/double/int, `dropDuplicates` on the full business key, missing zip → `0`, and drop-expectations for invalid timestamps, distance (0–100 mi), fare (0–500 USD) and duration (≤ 180 min). Derived features: `trip_duration_min`, `pickup_date`, `pickup_hour`, `pickup_dow` (1 = Sun … 7 = Sat).

**Gold — `gold_taxi`**: trips, revenue, average fare and similar KPIs per date and hour; `v_taxi_daily_kpi` view feeds the dashboard.

**Incremental proof**: each job run adds exactly one new file; earlier files keep their original `_ingested_at`.

```sql
SELECT _ingested_at, count(DISTINCT _source_file) files, count(*) rows
FROM workspace.tugas2.bronze_taxi GROUP BY _ingested_at ORDER BY _ingested_at;
```

## 5. Machine learning

| Model | RMSE | MAE | R² |
|---|---|---|---|
| **Random Forest** (best, tag `best_model=true`) | 1.782 | 0.429 | 0.966 |
| Linear Regression | 2.026 | 0.705 | 0.955 |

- Features: `trip_distance`, `trip_duration_min`, `pickup_hour`, `pickup_dow`, `pickup_zip`, `dropoff_zip`
- Registered model: `workspace.tugas2.taxi_fare_model` v1, alias `@champion`
- ONNX artifact: `onnx/taxi_fare_model.onnx` (difference vs sklearn = 0.00000)

## 6. Model API

| | |
|---|---|
| Endpoint | `taxi-fare-endpoint` — serves `taxi_fare_model` v1, size Small, scale-to-zero |
| Invocations | `POST {host}/serving-endpoints/taxi-fare-endpoint/invocations` |
| Health | `GET {host}/api/2.0/serving-endpoints/taxi-fare-endpoint` |
| Auth | `Authorization: Bearer <Databricks PAT>` (starts with `dapi`) |

```json
POST /serving-endpoints/taxi-fare-endpoint/invocations
{"dataframe_split": {"columns": ["trip_distance","trip_duration_min","pickup_hour","pickup_dow","pickup_zip","dropoff_zip"],
                     "data": [[1.4, 9.4, 21, 7, 10103, 10110]]}}
→ 200 {"predictions": [7.97]}
```

> **15 Nov 2026:** the endpoint had disappeared from the workspace (the model was intact). It was recreated with the same name and configuration and became READY in ~1 minute. Re-check it before the demo.

## 7. Google Drive data source

| | |
|---|---|
| File | `My Drive/tugas2/nyctaxi_trips.csv` — 21,932 rows, 1.34 MB, 6 columns |
| Link | https://drive.google.com/file/d/1qdFdNSagZw27kni-QjAXtlHG0ZrtgDsx/view |
| Sharing | Anyone with the link → Viewer (required for credential-free download) |
| Target | `/Volumes/workspace/tugas2/raw/gdrive/` → `workspace.tugas2.gdrive_taxi_trips` |

How it works:
1. `00a_export_full_csv` wrote the full table to one CSV; the file was uploaded to Drive.
2. `00b_pull_from_gdrive` downloads `https://drive.usercontent.google.com/download?id=<id>&export=download&confirm=t`, verifies it is CSV (not a Drive HTML page) with the expected columns, saves it to the Volume, and writes a typed Delta table with `_source` and `_pulled_at` columns. The file ID is a widget (`gdrive_file_id`).
3. Free Edition restricts outbound internet, but `drive.google.com`, `drive.usercontent.google.com` and `www.googleapis.com` were confirmed reachable from serverless.

Verified 15 Nov 2026: 4/4 cells succeeded, table = **21,932 rows**; Drive copy is byte-identical to the export.

**Part of the pipeline:** `00b` runs as the job's first task (`pull_from_gdrive`, job parameter `gdrive_file_id`), and `00_feed_new_csv` reads `gdrive_taxi_trips` (job parameter `source_table`, only the 6 original columns, so Bronze's schema is unchanged). Test run on 15 Nov 2026: all 4 tasks succeeded, the Drive pull refreshed the table, and a new file `taxi_00061_2016-03-05.csv` (346 rows) reached Bronze, then Silver and Gold. To switch back to the built-in source, set `source_table` to `samples.nyctaxi.trips`.

## 8. Postman tests

| # | Request | Checks |
|---|---|---|
| 1 | Health check | 200, state READY, serves `taxi_fare_model` |
| 2 | Predict — `dataframe_split`, 3 trips | 200, 3 positive numeric predictions |
| 3 | Predict — `dataframe_records`, 1 trip | 200, 1 numeric prediction |
| 4 | Fetch dataset from Google Drive (no auth) | 200, CSV not HTML, 6 columns, 21,932 rows, builds features for 10 trips like `silver_taxi` |
| 5 | Predict the 10 Drive trips | 200, one positive prediction per trip, MAE vs actual fare < $3 |

**Last run (15 Nov 2026): 5/5 requests, 18/18 assertions passed, all 200 OK.** Drive trips: MAE **$0.42** (in-sample sanity check — these trips were part of the training source).

Run it (step-by-step guide with every variable and request body: [`postman/POSTMAN_GUIDE.md`](postman/POSTMAN_GUIDE.md)):
- **Postman app**: import the collection, set the collection variable `token` (current value only), *Run collection*.
- **CLI**:
  ```bash
  npx -p newman@6 -p newman-reporter-htmlextra@1 newman run postman/Tugas2_Taxi_Fare_API.postman_collection.json \
    --env-var "token=$(cat ~/.databricks_tugas2_token)" --timeout-request 300000 \
    -r cli,htmlextra --reporter-htmlextra-export postman/Postman-Report.html \
    --reporter-htmlextra-skipHeaders Authorization --reporter-htmlextra-skipEnvironmentVars token \
    --reporter-htmlextra-hideResponseBody "4. Google Drive - fetch dataset (CSV)"
  ```

## 9. Dashboard

`Tugas2 NYC Taxi Dashboard` ([published](https://dbc-ee445d02-3afe.cloud.databricks.com/dashboardsv3/01f1dcd54d3b1dffaf675d8ff582a80f/published?o=7474660676014887)) has two pages:

- **Overview**: Gold-layer KPIs: total trips, revenue, average fare, days loaded, daily trend, revenue, trips by hour, fare per mile by weekday, latest API predictions.
- **ML Model** (added 15 Nov 2026):
  - **What-if predictor**: six inputs (distance, duration, pickup hour, day of week 1=Sun…7=Sat, pickup/dropoff zip). The dashboard calls the live endpoint through `ai_query('taxi-fare-endpoint', named_struct(...))` and shows the predicted fare next to the average real fare and count of similar historical trips in `silver_taxi` (±20% distance, ±25%/min 3 min duration). Example: 2.5 mi / 12 min / Fri 18:00 → **$10.74** predicted vs **$10.76** across 1,830 similar trips.
  - **Fare vs distance curve**: predicted fare for 1–20 miles at the scenario's speed, hour, day and zips (20 endpoint calls per input change).
  - **Training results**: Random Forest vs Linear Regression (RMSE, MAE, R² from MLflow; static values).
  - **Live API performance** from `taxi_predictions`: trips scored, MAE, RMSE, batches, last scored; actual-vs-predicted scatter; MAE per batch; MAE by distance band.

Inputs are text boxes (dashboard parameters), not sliders. The first prediction after the endpoint has been idle can take about a minute because of scale-to-zero. If `taxi-fare-endpoint` is missing, the what-if tiles and curve error out, but the other charts still load.

## 10. Automation

Job `tugas2_taxi_lakehouse_job`: `pull_from_gdrive` → `feed_new_csv` → `dlt_bronze_silver_gold` → `call_model_api`, cron `0 0/2 * * * ?` (Asia/Jakarta), `max_concurrent_runs = 1`, parameters `landing_path`, `endpoint_name`, `source_table` and `gdrive_file_id`. The schedule is **PAUSED** to save Free Edition compute.

## 11. Demo checklist (16 Nov)

1. Confirm `taxi-fare-endpoint` exists and is READY (Postman request 1).
2. ~30 min before: send one prediction to wake the endpoint (cold start can take minutes).
3. Resume the job schedule (Job page → *Schedules & Triggers* → **Resume**); pause it again afterwards.
4. Open the dashboard's **ML Model** page and try a scenario (e.g. 10.5 mi / 32 min → about $32.90). Then on **Overview**, after one job run, confirm "Total trips" / "Days loaded" increase.
5. Show the Drive source: the Drive file, then a job run whose first task `pull_from_gdrive` refreshes `gdrive_taxi_trips`. Open `01_eda_sql` for the SQL exploration.
6. Screenshots still needed: MLflow experiment, dashboard, job config, catalog, DLT graph, Postman run.

## 12. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Postman 401 "Credential was not sent or was of an unsupported type" | Not a Databricks PAT — generate one under Settings → Developer → Access tokens (`dapi…`) |
| Endpoint `RESOURCE_DOES_NOT_EXIST` | Endpoint deleted — recreate `taxi-fare-endpoint` serving `workspace.tugas2.taxi_fare_model` v1, Small, scale-to-zero |
| First prediction very slow / times out | Scale-to-zero cold start — retry after a few minutes |
| `00b` says Drive returned HTML | File is not shared as *Anyone with the link → Viewer*, or wrong file ID |
| `00b` cannot reach Google Drive | Free Edition egress block — upload the CSV to `raw/gdrive/` manually and run from step 2 |

## 13. Security notes

- No tokens are stored in the repo; the Postman collection's `token` variable is empty and the HTML report hides the `Authorization` header and the `token` variable.
- Keep the PAT in a private local file (e.g. `~/.databricks_tugas2_token`, `chmod 600`) or in Postman's *current value* only. Revoke tokens that were shared anywhere, and after the demo.
- The Drive file is public-by-link; it only contains the open `samples.nyctaxi.trips` data.
- Personal identifiers are replaced with the placeholder `databricks-user@example.com` (notebook paths in `job/job_config.json`, the endpoint `creator` field in the Postman report). Replace it with your Databricks username when re-importing the job.
