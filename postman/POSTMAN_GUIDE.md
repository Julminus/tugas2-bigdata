# Postman guide — Taxi Fare API

How to test the deployed fare-prediction model (`taxi-fare-endpoint`) and the Google Drive dataset from Postman, using [`Tugas2_Taxi_Fare_API.postman_collection.json`](Tugas2_Taxi_Fare_API.postman_collection.json).

## Quick start

1. **Import**: Postman → *Import* → choose `postman/Tugas2_Taxi_Fare_API.postman_collection.json`.
2. **Set the token**: click the collection → *Variables* tab → `token` → paste your Databricks token in **Current value**. Leave *Initial value* empty, because that's the part saved in exports and shared workspaces. Click *Save*.
3. **Run**: click a request → *Send*, or the collection → *Run* → *Run Tugas 2 …* for all 5 at once.

**`token` is the only thing you must fill in.** Everything else has a working default.

### Getting a token
Databricks → avatar (top right) → **Settings** → **Developer** → **Access tokens** → **Generate new token**. Copy the value. It starts with `dapi` and is shown only once. Revoke it after the demo.

## Collection variables

| Variable | Fill in? | Default | What it is |
|---|---|---|---|
| `token` | **Yes** | *(empty)* | Databricks personal access token (`dapi…`). Sent as `Authorization: Bearer {{token}}` to Databricks requests only |
| `host` | No | `https://dbc-ee445d02-3afe.cloud.databricks.com` | Databricks workspace URL |
| `endpoint_name` | No | `taxi-fare-endpoint` | Model Serving endpoint name |
| `gdrive_file_id` | No | `1qdFdNSagZw27kni-QjAXtlHG0ZrtgDsx` | Google Drive file ID of `nyctaxi_trips.csv` |
| `gdrive_sample_size` | Optional | `10` | How many trips request 4 picks from the Drive file |
| `gdrive_rows` | **Don't edit** | `[]` | Set automatically by request 4 (features for request 5) |
| `gdrive_fares` | **Don't edit** | `[]` | Set automatically by request 4 (real fares to compare in request 5) |

## Requests

| # | Name | Method & URL | Auth | Needs |
|---|---|---|---|---|
| 1 | Health check | `GET {{host}}/api/2.0/serving-endpoints/{{endpoint_name}}` | Bearer token | `token` |
| 2 | Predict — batch of 3 | `POST {{host}}/serving-endpoints/{{endpoint_name}}/invocations` | Bearer token | `token` |
| 3 | Predict — single trip | `POST {{host}}/serving-endpoints/{{endpoint_name}}/invocations` | Bearer token | `token` |
| 4 | Google Drive — fetch dataset | `GET https://drive.usercontent.google.com/download?id={{gdrive_file_id}}&export=download&confirm=t` | **None** (the token is not sent to Google) | nothing |
| 5 | Predict — trips from Drive | `POST {{host}}/serving-endpoints/{{endpoint_name}}/invocations` | Bearer token | `token` + **run request 4 first** |

Headers: requests 2, 3 and 5 send `Content-Type: application/json`. That's already set in the collection.

## Model input: the 6 features

Every prediction needs all 6 columns, spelled exactly like this. Numbers can be integers or decimals.

| Column | Meaning | Unit / range | Example |
|---|---|---|---|
| `trip_distance` | Trip length | miles, > 0 (training data: < 100) | `2.5` |
| `trip_duration_min` | Pickup → dropoff time | minutes, > 0 (training data: ≤ 180) | `12.0` |
| `pickup_hour` | Hour of pickup | `0`–`23` | `18` |
| `pickup_dow` | Day of week | `1`=Sun, `2`=Mon, … `7`=Sat | `6` (Friday) |
| `pickup_zip` | Pickup NYC zip code | 5-digit zip; `0` = unknown | `10001` |
| `dropoff_zip` | Dropoff NYC zip code | 5-digit zip; `0` = unknown | `10018` |

The API accepts two body formats:

**`dataframe_split`** (request 2: list the column names once, then one row per trip in the same order):
```json
{
  "dataframe_split": {
    "columns": ["trip_distance", "trip_duration_min", "pickup_hour", "pickup_dow", "pickup_zip", "dropoff_zip"],
    "data": [
      [1.4, 9.4, 21, 7, 10103, 10110],
      [3.2, 15.0, 8, 2, 10023, 10001]
    ]
  }
}
```

**`dataframe_records`** (request 3: one object per trip):
```json
{
  "dataframe_records": [
    {"trip_distance": 2.5, "trip_duration_min": 12.0, "pickup_hour": 18, "pickup_dow": 6, "pickup_zip": 10001, "dropoff_zip": 10018}
  ]
}
```

**Response:** one predicted fare in USD per trip, in input order:
```json
{"predictions": [7.97, 11.84]}
```

To try your own scenario, edit the body of request 3 and click *Send*.

## What each request checks

| # | Tests (shown in the *Test Results* tab) |
|---|---|
| 1 | Status 200 · endpoint state `READY` · serves `taxi_fare_model` |
| 2 | Status 200 · has `predictions` · 3 numeric values · all positive |
| 3 | Status 200 · exactly 1 numeric prediction |
| 4 | Status 200 · CSV, not an HTML page · the 6 expected columns · 21,932 trips · features built for the sample trips |
| 5 | Status 200 · one prediction per Drive trip · all positive · mean absolute error vs the real fare < $3 |

Request 5 also prints *actual vs predicted* per trip in the Postman **Console** (View → Show Postman Console). Last run (15 Nov 2026): 18/18 tests passed, Drive-trip error $0.42.

## Troubleshooting

| You see | Meaning | Fix |
|---|---|---|
| `401` "Credential was not sent or was of an unsupported type" | `token` is empty or isn't a Databricks PAT | Paste a `dapi…` token into the `token` **Current value** and save |
| `403` | The token's user can't use the endpoint | Use a token from a workspace user with access to `taxi-fare-endpoint` |
| `404` / `RESOURCE_DOES_NOT_EXIST` | The endpoint was deleted | Recreate `taxi-fare-endpoint` serving `workspace.tugas2.taxi_fare_model` v1 (Small, scale-to-zero) |
| `400` "Failed to enforce schema of data" | A column is missing or misspelled | Send all 6 columns with the exact names above |
| `400` "Failed to convert column … to type 'float64'" | A value isn't a number (e.g. `"abc"`) | Use plain numbers, no quotes |
| `400` "Found array with 0 sample(s)" on request 5 | Request 5 ran before request 4, so `gdrive_rows` is empty | Send request 4 first, or run the whole collection |
| Request hangs or times out on the first call | Scale-to-zero cold start after idle | Wait a minute and send again; raise Settings → *Request timeout* if needed |
| Request 4 fails "Drive returned the CSV, not an HTML page" | The Drive file isn't public or the ID is wrong | Share the file as *Anyone with the link → Viewer*; check `gdrive_file_id` |

## Running it from the command line (optional)

```bash
npx -p newman@6 -p newman-reporter-htmlextra@1 newman run postman/Tugas2_Taxi_Fare_API.postman_collection.json \
  --env-var "token=$(cat ~/.databricks_tugas2_token)" --timeout-request 300000 \
  -r cli,htmlextra --reporter-htmlextra-export postman/Postman-Report.html \
  --reporter-htmlextra-skipHeaders Authorization --reporter-htmlextra-skipEnvironmentVars token \
  --reporter-htmlextra-hideResponseBody "4. Google Drive - fetch dataset (CSV)"
```
