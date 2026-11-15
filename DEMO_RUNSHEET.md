# Demo run sheet — Tugas 2 (Monday 16 Nov 2026)

A step-by-step script for the presentation: what to open, what to click, what to say, and what to do if something breaks.
Total demo time ≈ **15 minutes**, plus Q&A.

> Workspace: `https://dbc-ee445d02-3afe.cloud.databricks.com` · all links below include `?o=7474660676014887`.
> Numbers in this sheet were measured on 15 Nov 2026. **Read the live numbers aloud during the demo**, because they grow with every job run.

---

## T-30 min · Setup

| ✓ | Step | How |
|---|---|---|
| ☐ | **1. Check the API exists** | Postman → request **1. Health check** → expect `200` and `"ready": "READY"`. If you get `404`, see *Backup plans* below |
| ☐ | **2. Wake the endpoint** | Postman → request **3. Predict – single trip** → wait for `200` (the first call after idle can take 1–3 min) |
| ☐ | **3. Resume the job schedule** | [Job page](https://dbc-ee445d02-3afe.cloud.databricks.com/jobs/152115249867558?o=7474660676014887) → *Schedules & Triggers* → **Resume** |
| ☐ | **4. Confirm one run succeeds** | Wait ~3 min → the newest run shows 4 green tasks |
| ☐ | **5. Warm the dashboard** | Open both pages once ([Overview](https://dbc-ee445d02-3afe.cloud.databricks.com/dashboardsv3/01f1dcd54d3b1dffaf675d8ff582a80f/published?o=7474660676014887), [ML Model](https://dbc-ee445d02-3afe.cloud.databricks.com/dashboardsv3/01f1dcd54d3b1dffaf675d8ff582a80f/published/pages/ml_model?o=7474660676014887)) so the warehouse is running |
| ☐ | **6. Take backup screenshots** | MLflow experiment, dashboard (both pages), job runs, Catalog, pipeline graph, Postman run. If something fails live, show these |
| ☐ | **7. Open the tabs below in order** | One browser window, left to right |

### Tabs (open in this order)

| # | Tab | Link |
|---|---|---|
| 1 | Google Drive file | https://drive.google.com/file/d/1qdFdNSagZw27kni-QjAXtlHG0ZrtgDsx/view |
| 2 | GitHub repo | https://github.com/Julminus/tugas2-bigdata |
| 3 | Notebook `00b_pull_from_gdrive` | https://dbc-ee445d02-3afe.cloud.databricks.com/editor/notebooks/4371288052670094?o=7474660676014887 |
| 4 | Notebook `01_eda_sql` | https://dbc-ee445d02-3afe.cloud.databricks.com/editor/notebooks/1451313524093620?o=7474660676014887 |
| 5 | DLT pipeline | https://dbc-ee445d02-3afe.cloud.databricks.com/pipelines/49a872dc-89ab-4298-bb68-d5b6146e37ad?o=7474660676014887 |
| 6 | Catalog `workspace.tugas2` | https://dbc-ee445d02-3afe.cloud.databricks.com/explore/data/workspace/tugas2?o=7474660676014887 |
| 7 | SQL Editor (queries from the *Proof queries* section pasted in) | Sidebar → **SQL Editor** |
| 8 | Job | https://dbc-ee445d02-3afe.cloud.databricks.com/jobs/152115249867558?o=7474660676014887 |
| 9 | MLflow experiment | https://dbc-ee445d02-3afe.cloud.databricks.com/ml/experiments/832902894235823?o=7474660676014887 |
| 10 | Registered model | https://dbc-ee445d02-3afe.cloud.databricks.com/explore/data/models/workspace/tugas2/taxi_fare_model?o=7474660676014887 |
| 11 | Serving endpoint | https://dbc-ee445d02-3afe.cloud.databricks.com/ml/endpoints/taxi-fare-endpoint?o=7474660676014887 |
| 12 | Dashboard | https://dbc-ee445d02-3afe.cloud.databricks.com/dashboardsv3/01f1dcd54d3b1dffaf675d8ff582a80f/published?o=7474660676014887 |
| — | **Postman app** (separate window) | Collection *Tugas 2 – Taxi Fare Model API*, `token` filled in |

---

## Demo flow

### 0 · Intro (1 min)
**Say:** "Case study: NYC taxi trips. We built an end-to-end lakehouse on Databricks: external data from Google Drive → DLT pipeline (Bronze/Silver/Gold) → a fare-prediction model → REST API tested in Postman → dashboard, all automated by a job running every 2 minutes."
**Show:** the architecture diagram in the README (GitHub tab).

### 1 · External data source: Google Drive (1.5 min) · *rubric: G-Drive, Table, Volume*
1. **Tab 1 (Drive):** `My Drive/tugas2/nyctaxi_trips.csv`, an open dataset of 21,932 trips (Jan–Feb 2016), shared as *Anyone with the link → Viewer*.
2. **Tab 3 (`00b_pull_from_gdrive`):** download → checks (CSV, not HTML; 6 columns) → saved to **Volume** `raw/gdrive/` → Delta **table** `gdrive_taxi_trips`.
3. **Say:** "This notebook is the job's first task, so every run pulls the latest file from Drive. The code is also on GitHub."

### 2 · Exploratory analysis with Spark SQL (2 min) · *rubric: SQL*
**Tab 4 (`01_eda_sql`)**, scroll through and point at:
- **Overview:** 21,932 trips, 60 days, average fare $12.35.
- **Data quality:** 76 invalid distances, 33 trips over 3 h, 10 invalid fares. **Say:** "These became the Silver cleaning rules."
- **Distance bands:** fare per mile falls from $9.39 to $3.04 as trips get longer.
- **Feature correlation:** distance 0.95, duration 0.86, hour/weekday ≈ 0. **Say:** "This guided the model's features."
- **Summary cell:** 8 SQL tables `eda_*` created with Spark.

### 3 · ETL pipeline: Bronze → Silver → Gold (2.5 min) · *rubric: ETL (DLT)*
1. **Tab 5 (pipeline graph):** `bronze_taxi` → `silver_taxi` → `gold_taxi`; last update COMPLETED.
   - **Bronze:** Auto Loader reads **only new CSV files** from the landing Volume (incremental, no overwrite).
   - **Silver:** Spark DataFrame: `try_cast`, `dropDuplicates`, `fillna`, expectations that drop invalid rows, derived features.
   - **Gold:** KPIs per date × hour for the dashboard.
2. **Tab 7 (SQL Editor):** run **Proof query A** (incremental). **Say:** "Each job run adds exactly one new file; old files keep their original ingest time, so nothing is overwritten."
3. Run **Proof query B** (counts): Bronze > Silver (dirty rows removed), Gold total = Silver.

### 4 · Job: automation and proof it works (1.5 min) · *rubric: Jobs (Workflow)*
**Tab 8 (Job):**
- **Tasks:** `pull_from_gdrive` → `feed_new_csv` → `dlt_bronze_silver_gold` → `call_model_api`.
- **Schedule:** cron `0 0/2 * * * ?` (every 2 min, Asia/Jakarta).
- **Runs tab:** a list of green `PERIODIC` runs, including runs from during the demo. Click one to show 4 green tasks.
- **Say:** "A run takes about 2 min 45 s, so runs queue back to back. That's fine because `max_concurrent_runs = 1`."

### 5 · Machine learning: MLflow (1.5 min) · *rubric: AI/ML*
**Tab 9 (Experiment):** two runs trained on **Silver** data.

| Model | RMSE | MAE | R² |
|---|---|---|---|
| **Random Forest** (tag `best_model=true`) | 1.782 | 0.429 | 0.966 |
| Linear Regression | 2.026 | 0.705 | 0.955 |

**Tab 10 (Model):** `workspace.tugas2.taxi_fare_model` v1, alias **`@champion`** (+ ONNX export).

### 6 · Serving and Postman (2 min) · *rubric: Models & Serving, Testing*
1. **Tab 11 (Endpoint):** `taxi-fare-endpoint` state **Ready**, serving `taxi_fare_model` v1.
2. **Postman app:** click the collection → **Run** → *Run Tugas 2 …* → expect **5/5 requests, 18/18 tests passed**.
   - Request 2: batch of 3 trips. Request 3: single trip (e.g. 2.5 mi / 12 min, Fri 18:00 → about $10.7).
   - Requests 4–5: download the dataset **from Google Drive**, predict 10 real trips, compare with the real fares (MAE ≈ $0.42).
3. **Optional, live:** edit request 3's body (e.g. `trip_distance` 10.5, `trip_duration_min` 32) → *Send* → about $32.9.

### 7 · Dashboard (2 min) · *rubric: Dashboard (Gold + custom SQL)*
**Tab 12:**
- **Overview page:** Gold KPIs + custom SQL view `v_taxi_daily_kpi`. Refresh and point at **Total trips / Days loaded** growing after a job run.
- **ML Model page:** type a scenario: distance **10.5**, duration **32** → predicted fare **≈ $32.90** vs **$32.81** average of 431 similar real trips. Point at the fare-vs-distance curve, the model comparison, and live API metrics (MAE ≈ $0.26).

### 8 · Wrap-up (30 s)
**Say:** "Every requirement is covered: Drive → SQL EDA → DLT 3 layers → scheduled job → MLflow model → API endpoint → Postman → dashboard. Code and documentation are on GitHub."

---

## Proof queries (paste into the SQL Editor beforehand)

**A · Incremental ingestion**
```sql
SELECT _ingested_at, count(DISTINCT _source_file) AS files, count(*) AS rows
FROM workspace.tugas2.bronze_taxi GROUP BY _ingested_at ORDER BY _ingested_at DESC LIMIT 10;
```

**B · Layer counts**
```sql
SELECT (SELECT count(*) FROM workspace.tugas2.bronze_taxi)     AS bronze_rows,
       (SELECT count(*) FROM workspace.tugas2.silver_taxi)     AS silver_rows,
       (SELECT sum(trips) FROM workspace.tugas2.gold_taxi)     AS gold_total_trips,
       (SELECT count(*) FROM workspace.tugas2.taxi_predictions) AS api_predictions;
```
*15 Nov reference: 23,092 · 21,976 · 21,976 · 80 (will be higher on demo day).*

**C · Latest Drive pull**
```sql
SELECT _source, max(_pulled_at) AS pulled_at, count(*) AS rows
FROM workspace.tugas2.gdrive_taxi_trips GROUP BY _source;
```

---

## Backup plans

| Problem | What to do |
|---|---|
| Endpoint `404` / missing | Serving → *Create serving endpoint* → name `taxi-fare-endpoint`, entity `workspace.tugas2.taxi_fare_model` v1, size Small, scale-to-zero on. Ready in about 1 min. Meanwhile show the MLflow and Postman report screenshots |
| First API call times out | Cold start: send again after 1 min |
| Job task `pull_from_gdrive` fails (Drive unreachable) | Job → *Edit parameters* → `source_table` = `samples.nyctaxi.trips` → *Run now*. Explain that the Drive table is a cached copy |
| Pipeline update slow | Show the last completed update and Proof query A. Runs take about 3 min in total |
| Dashboard ML tiles error | The endpoint is down or cold; the other charts still work. Show the Postman run instead |
| Postman `401` | `token` variable is empty or expired; paste a fresh `dapi…` token into *Current value* |

## Likely questions

| Question | Answer |
|---|---|
| Why not `INSERT OVERWRITE` in Bronze? | The assignment needs incremental loading without full overwrite; Auto Loader checkpoints processed files, so each run reads only new files |
| Where do new CSV files come from? | The dataset is static, so `00_feed_new_csv` replays one pickup day per run from the Drive data (looping with a 9-week shift) and injects dirty rows so Silver has real cleaning to do |
| Why Random Forest? | Lower RMSE/MAE and higher R² than Linear Regression; the relationship isn't purely linear (fare per mile drops with distance) |
| Which features matter most? | Distance and duration (~98% of importance). Hour and weekday barely matter, as the EDA correlations already showed |
| Why is the Drive file public? | The serverless notebook downloads without credentials; the file only contains an open dataset |
| How does the API scale? | Model Serving, size Small, scale-to-zero: cheap when idle, but cold start on the first call |
| How is it tested? | Postman: 18 assertions (status, schema, positive fares, error vs real fares); also run from the CLI with newman (HTML report in the repo) |

---

## After the demo
- ☐ **Pause the job schedule** (Job → *Schedules & Triggers* → **Pause**) to save Free Edition compute.
- ☐ **Revoke the Postman token** (Settings → Developer → Access tokens).
- ☐ Optional: delete `~/.databricks_tugas2_token` on the laptop.
