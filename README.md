# Tugas 2 — Big Data & Databricks End-to-End (NYC Taxi Lakehouse)

Dataset: **`samples.nyctaxi.trips`** (open dataset bawaan Databricks, 21.932 trip taksi NYC Jan–Feb 2016).
Use case: **regresi tarif taksi (`fare_amount`)** + dashboard KPI perjalanan.
Platform: Databricks Free Edition (serverless) — Unity Catalog `workspace`, schema baru **`workspace.tugas2`** (terpisah dari `default`).

```
samples.nyctaxi.trips ──(00_feed_new_csv, 1 file CSV baru / run)──▶ /Volumes/workspace/tugas2/raw/landing/*.csv
        │ Auto Loader (incremental)
        ▼
 bronze_taxi (streaming table) ─▶ silver_taxi (cleaning) ─▶ gold_taxi (agregasi per tanggal+jam) ─▶ v_taxi_daily_kpi (view) ─▶ Dashboard
                                        │
                                        ├─▶ 02_train_model (MLflow: LinearRegression vs RandomForest) ─▶ UC model taxi_fare_model@champion ─▶ ONNX (02b)
                                        │                                                                        │
                                        └──────────── 03_call_model_api ◀── Model Serving endpoint taxi-fare-endpoint ◀┘
Databricks Job (cron tiap 2 menit): feed_new_csv ─▶ dlt_bronze_silver_gold ─▶ call_model_api
```

## Link (copy-paste ready)

Host: `https://dbc-ee445d02-3afe.cloud.databricks.com` (workspace `?o=7474660676014887`)

| Komponen | URL |
|---|---|
| Workspace folder (notebooks) | https://dbc-ee445d02-3afe.cloud.databricks.com/browse/folders/832902894235815?o=7474660676014887 |
| DLT pipeline `tugas2_taxi_pipeline` | https://dbc-ee445d02-3afe.cloud.databricks.com/pipelines/49a872dc-89ab-4298-bb68-d5b6146e37ad?o=7474660676014887 |
| Catalog (tabel bronze/silver/gold) | https://dbc-ee445d02-3afe.cloud.databricks.com/explore/data/workspace/tugas2?o=7474660676014887 |
| MLflow experiment `tugas2_taxi_fare` | https://dbc-ee445d02-3afe.cloud.databricks.com/ml/experiments/832902894235823?o=7474660676014887 |
| Registered model `workspace.tugas2.taxi_fare_model` | https://dbc-ee445d02-3afe.cloud.databricks.com/explore/data/models/workspace/tugas2/taxi_fare_model?o=7474660676014887 |
| Serving endpoint (UI) | https://dbc-ee445d02-3afe.cloud.databricks.com/ml/endpoints/taxi-fare-endpoint?o=7474660676014887 |
| **API-Endpoint-URL (invocations)** | `https://dbc-ee445d02-3afe.cloud.databricks.com/serving-endpoints/taxi-fare-endpoint/invocations` |
| API health check | `https://dbc-ee445d02-3afe.cloud.databricks.com/api/2.0/serving-endpoints/taxi-fare-endpoint` |
| Job `tugas2_taxi_lakehouse_job` | https://dbc-ee445d02-3afe.cloud.databricks.com/jobs/152115249867558?o=7474660676014887 |
| Dashboard (published) | https://dbc-ee445d02-3afe.cloud.databricks.com/dashboardsv3/01f1dcd54d3b1dffaf675d8ff582a80f/published?o=7474660676014887 |
| Dataset di Google Drive (`nyctaxi_trips.csv`) | https://drive.google.com/file/d/1qdFdNSagZw27kni-QjAXtlHG0ZrtgDsx/view |
| Notebook `00b_pull_from_gdrive` | https://dbc-ee445d02-3afe.cloud.databricks.com/editor/notebooks/4371288052670094?o=7474660676014887 |
| Tabel `workspace.tugas2.gdrive_taxi_trips` | https://dbc-ee445d02-3afe.cloud.databricks.com/explore/data/workspace/tugas2/gdrive_taxi_trips?o=7474660676014887 |

Contoh request API (header `Authorization: Bearer <PAT>`):
```json
POST /serving-endpoints/taxi-fare-endpoint/invocations
{"dataframe_split": {"columns": ["trip_distance","trip_duration_min","pickup_hour","pickup_dow","pickup_zip","dropoff_zip"],
                     "data": [[1.4, 9.4, 21, 7, 10103, 10110]]}}
→ 200 {"predictions": [7.97]}
```

## Isi folder

| File | Isi |
|---|---|
| `dlt/bronze_taxi.py` | Definisi DLT Bronze — Auto Loader `cloudFiles` CSV, path dari config pipeline `source_path` |
| `dlt/silver_taxi.py` | Definisi DLT Silver — Spark DataFrame: `try_cast`, `dropDuplicates`, `fillna`, expectations (drop null/invalid) |
| `dlt/gold_taxi.py` | Definisi DLT Gold — agregasi per `pickup_date`, `pickup_hour` (trips, revenue, avg fare, dll.) |
| `notebooks/00_feed_new_csv.py` | Simulasi sumber incremental: tiap run menaruh 1 file CSV baru (1 hari data) + baris kotor |
| `notebooks/00a_export_full_csv.py` | Sekali jalan: ekspor seluruh `samples.nyctaxi.trips` ke 1 file CSV di Volume `raw/export/` (sumber file yang diunggah ke Google Drive) |
| `notebooks/00b_pull_from_gdrive.py` | Tarik CSV dari Google Drive (link publik) ke Volume `raw/gdrive/`, lalu load ke tabel Delta `gdrive_taxi_trips` |
| `notebooks/02_train_model.py` | MLflow: Linear Regression vs Random Forest, MAE/RMSE/R², tag `best_model`, register UC alias `champion` |
| `notebooks/02b_export_onnx.py` | Export model terbaik ke ONNX + verifikasi ONNX Runtime, log ke run terbaik |
| `notebooks/03_call_model_api.py` | Post-pipeline service call ke endpoint, hasil disimpan ke `taxi_predictions` |
| `job/job_config.json` | Export settings Databricks Job (Workflow-Job-Config) |
| `dashboard/tugas2_taxi_dashboard.lvdash.json` | Definisi dashboard (AI/BI) |
| `postman/Tugas2_Taxi_Fare_API.postman_collection.json` | Postman collection: health check, 2 jenis request prediksi, + ambil dataset dari Google Drive (request 4) lalu prediksi trip-trip dari Drive (request 5) |
| `postman/Postman-Report.html` | Laporan run collection (newman, 15 Nov 2026: 5 request, 18/18 assertion lulus, semua 200 OK; header Authorization & token disembunyikan) |

## Bukti per rubrik (hasil verifikasi 10 Nov 2026)

| Komponen | Status | Bukti |
|---|---|---|
| ETL 3 layer | ✅ | `bronze_taxi`, `silver_taxi`, `gold_taxi` di `workspace.tugas2` (DLT serverless, update COMPLETED) |
| Incremental | ✅ | Lihat bagian *Incremental* di bawah |
| Cleaning Silver | ✅ | Load awal: Bronze 10.753 baris (30 file, termasuk 240 duplikat sengaja) → Silver 10.215 baris; 0 null di kolom penting, 0 duplikat |
| Gold | ✅ | 700 baris agregat (tanggal×jam), `SUM(trips)` = 10.215 = jumlah Silver |
| AI/ML | ✅ | Random Forest **RMSE 1,782 · MAE 0,429 · R² 0,966** (best, tag `best_model=true`) vs Linear Regression RMSE 2,026 · MAE 0,705 · R² 0,955 |
| Model artifact | ✅ | `workspace.tugas2.taxi_fare_model` v1 alias `@champion` (MLflow 3 logged model) + `onnx/taxi_fare_model.onnx` (selisih vs sklearn = 0,00000) |
| API deployment | ✅ | `taxi-fare-endpoint` state **READY**, invocations → HTTP 200 (endpoint sempat hilang, dibuat ulang 15 Nov 2026 dengan konfigurasi sama: `taxi_fare_model` v1, Small, scale-to-zero) |
| Postman | ✅ | 5 request, 18/18 assertion lulus, semua `200 OK` (`postman/Postman-Report.html`, 15 Nov 2026) — termasuk 10 trip dari Google Drive: MAE prediksi vs tarif asli $0,42 |
| Dashboard | ✅ published | 5 dataset custom SQL (4 dari `gold_taxi`/view `v_taxi_daily_kpi`, 1 dari `taxi_predictions`) — semua query teruji jalan; tampilan widget (counter, line, bar, table) **belum dicek visual** (lihat langkah manual 1) |
| Automation | ✅ | Cron `0 0/2 * * * ?` (Asia/Jakarta), `max_concurrent_runs=1`, history berisi run `PERIODIC` berulang |

### Incremental
Job berjalan 6× berturut-turut tiap 2 menit (trigger `PERIODIC`, 14:36–14:46 WIB, semua SUCCESS ±110 detik). Isi `bronze_taxi` per waktu ingest (UTC) setelah 5 run pertama (run ke-6 menambah `taxi_00035` dengan pola yang sama):

| `_ingested_at` | jumlah file | baris | nama file |
|---|---|---|---|
| 06:53:24 | 30 | 10.753 | `taxi_00000` … `taxi_00029` (load awal) |
| 07:37:06 | 1 | 362 | `taxi_00030_2016-01-31.csv` |
| 07:39:03 | 1 | 327 | `taxi_00031_2016-02-01.csv` |
| 07:41:01 | 1 | 326 | `taxi_00032_2016-02-02.csv` |
| 07:43:04 | 1 | 361 | `taxi_00033_2016-02-03.csv` |
| 07:45:01 | 1 | 405 | `taxi_00034_2016-02-04.csv` |

Setiap run hanya memproses **1 file baru**; 30 file awal tetap dengan timestamp ingest 06:53:24 (tidak ada full overwrite). Silver naik ke 11.904 baris, Gold ke 35 hari (`SUM(trips)` = 11.904), dan `taxi_predictions` berisi 6 batch skor dari API (MAE ≈ 0,2 USD).
Setelah itu replay sengaja diuji sampai titik putar ulang (file `taxi_00036`…`taxi_00060`, dijalankan manual): file ke-60 berlanjut sebagai `2016-03-04` (hari Jumat, sama dengan 2016-01-01), pipeline COMPLETED. Landing kini berisi 61 file.
Query bukti (jalankan di SQL editor):
```sql
SELECT _ingested_at, count(DISTINCT _source_file) files, count(*) rows
FROM workspace.tugas2.bronze_taxi GROUP BY _ingested_at ORDER BY _ingested_at;
```

## Sumber data Google Drive
Dataset lengkap juga disimpan di Google Drive dan ditarik kembali ke Databricks, sebagai bukti integrasi sumber data eksternal.

```
samples.nyctaxi.trips ──(00a, sekali)──▶ nyctaxi_trips.csv ──(upload)──▶ Google Drive: My Drive/tugas2/nyctaxi_trips.csv
                                                                              │ HTTPS (link publik, tanpa kredensial)
                                                                              ▼
                     00b_pull_from_gdrive ──▶ /Volumes/workspace/tugas2/raw/gdrive/nyctaxi_trips.csv ──▶ workspace.tugas2.gdrive_taxi_trips
```

* **File di Drive:** `nyctaxi_trips.csv` — 21.932 baris, 1,34 MB, 6 kolom (`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `pickup_zip`, `dropoff_zip`). Akses: *Anyone with the link → Viewer* (wajib, karena notebook mengunduh tanpa login; datanya open dataset jadi aman dibuka publik).
* **Cara tarik:** `00b_pull_from_gdrive` mengunduh dari `https://drive.usercontent.google.com/download?id=<file_id>&export=download&confirm=t`, mengecek bahwa hasilnya CSV (bukan halaman HTML Drive) dan kolomnya lengkap, menyimpan ke Volume, lalu menulis tabel Delta dengan tipe kolom yang benar + kolom `_source` (`gdrive:<file_id>`) dan `_pulled_at`. File ID diatur lewat widget `gdrive_file_id` (default sudah terisi), jadi tidak ada path/ID yang di-hardcode di logika.
* **Konektivitas:** Databricks Free Edition membatasi internet keluar ke domain tertentu; sudah diuji 15 Nov 2026 — `drive.google.com` (200), `drive.usercontent.google.com` dan `www.googleapis.com` bisa dijangkau dari serverless.
* **Hasil verifikasi (15 Nov 2026):** notebook dijalankan, 4/4 cell sukses, `workspace.tugas2.gdrive_taxi_trips` = **21.932 baris** (sama dengan sumber). File unduhan identik byte-per-byte dengan file ekspor.
* **Terpisah dari Job:** `00b` tidak menulis ke folder `landing`, jadi `bronze_taxi` dan job terjadwal tidak terpengaruh. Menjalankan ulang akan memuat ulang tabel dari isi file Drive saat itu (overwrite tabel `gdrive_taxi_trips` saja).
* **Jika Drive tidak bisa dijangkau / file berubah akses:** notebook berhenti dengan pesan jelas. Cadangan: unggah CSV manual ke Volume `raw/gdrive/` (Catalog → `workspace.tugas2` → `raw` → *Upload to this volume*) lalu jalankan dari langkah 2.

Query bukti:
```sql
SELECT _source, max(_pulled_at) pulled_at, count(*) rows FROM workspace.tugas2.gdrive_taxi_trips GROUP BY _source;
```

## Catatan desain / penyimpangan dari instruksi
* **`INSERT OVERWRITE` tidak dipakai** di Bronze: instruksi 2.1 meminta INSERT OVERWRITE tetapi sekaligus melarang *Full Overwrite* dan mewajibkan incremental. Yang dipakai adalah DLT streaming table + Auto Loader (checkpoint file yang sudah diproses), yang memenuhi kriteria pass/fail "Bronze membaca file baru secara incremental".
* **Tanpa hardcode path**: path landing berasal dari konfigurasi pipeline (`source_path`) dan job parameter (`landing_path`, `endpoint_name`).
* **Sumber data incremental**: karena dataset open-access bersifat statis, `00_feed_new_csv` me-replay data per hari pickup (60 hari, lalu berulang dengan geser 9 minggu — 2016-03-04 dst. — sehingga tidak pernah habis dan pola hari tetap sama), plus sengaja menyisipkan null, string `"N/A"`, dan duplikat agar Silver benar-benar membersihkan data.
* **Model serving**: format serving = MLflow sklearn pyfunc (Databricks Model Serving, scale-to-zero). ONNX diekspor sebagai artifact standar terpisah. Karena scale-to-zero, request pertama setelah idle bisa butuh beberapa menit (cold start) — **panggil endpoint sekali beberapa menit sebelum demo**.
* **Nilai tambah (image data)** tidak dikerjakan.

## Yang harus dilakukan manual oleh tim
0. **Cek dashboard dulu**: buka link Dashboard, pastikan setiap widget tampil (terutama counter "Last ingested" dan tabel prediksi). Jalankan 1 run job, lalu refresh dashboard dan pastikan angka "Total trips"/"Days loaded" bertambah. Widget yang tampil aneh bisa diperbaiki langsung di editor dashboard (Edit draft → Publish).
1. **Screenshot** (tidak bisa dibuat otomatis): MLflow experiment (2 run + tag `best_model`) → simpan sebagai `MLflow-Experiments-Log.pdf`; Dashboard → `Dashboard-Screenshot.png`; halaman Job (Schedule + Runs history) → `Workflow-Job-Config.png`; Catalog `workspace.tugas2`; DLT pipeline graph.
2. **Postman manual**: import `postman/Tugas2_Taxi_Fare_API.postman_collection.json`, isi variable `token` (Personal Access Token, *jangan* disimpan di file yang dikumpulkan), klik *Run collection* → screenshot → `Postman-Report.png`.
3. **Schedule job saat ini PAUSED** (sengaja, setelah 5 run sukses terverifikasi) supaya kuota compute harian Free Edition tidak habis — job tiap 2 menit = compute serverless hampir non-stop. **Aktifkan lagi ±30 menit sebelum demo 16 Nov**: (a) panaskan endpoint dulu (kirim request Postman sekali, tunggu sampai 200 — cold start scale-to-zero bisa beberapa menit), lalu (b) buka halaman Job → panel *Schedules & Triggers* → **Resume**. Setelah demo, *Pause* lagi. (`job/job_config.json` diekspor saat status PAUSED; selain itu konfigurasinya identik.)
4. Bagikan workspace/folder ke anggota tim (Share → Can View/Run) bila diperlukan.

## Laporan Progres Mid-Term (Selasa, 10 Nov 2026)
* **Bronze** — selesai: DLT streaming table dengan Auto Loader, ingest incremental dari Volume, terverifikasi file baru terdeteksi per run tanpa full overwrite.
* **Silver** — selesai: type conversion (`try_cast`), deduplikasi, penanganan missing value (drop via expectations + `fillna` zip), fitur turunan (durasi, jam, hari).
* **Gold** — selesai: agregasi KPI per tanggal+jam, siap dibaca SQL warehouse.
* **MLflow** — selesai: 2 model dilatih & dievaluasi, best model (Random Forest, RMSE 1,78) ter-register di Unity Catalog dan sudah di-serve sebagai REST API.
* Sisa untuk demo 16 Nov: screenshot deliverables, uji Postman manual oleh tim, aktifkan schedule job menjelang demo.
