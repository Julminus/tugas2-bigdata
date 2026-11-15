# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Train fare model with MLflow (Regression)
# MAGIC * Input: `silver_taxi` · Target: `fare_amount` · Features: distance, duration, hour, day-of-week, pickup/dropoff zip
# MAGIC * Candidates: **Linear Regression** vs **Random Forest**, evaluated with MAE / RMSE / R²
# MAGIC * Best model (lowest RMSE) is tagged `best_model=true` and registered to Unity Catalog with alias `champion`
# MAGIC   (served by the Model Serving endpoint). ONNX export: see `02b_export_onnx`.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow-skinny[databricks]"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "tugas2")
dbutils.widgets.text("model_name", "taxi_fare_model")

# COMMAND ----------

import mlflow
import numpy as np
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

cat, sch = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
model_name = f"{cat}.{sch}.{dbutils.widgets.get('model_name')}"
FEATURES = ["trip_distance", "trip_duration_min", "pickup_hour", "pickup_dow", "pickup_zip", "dropoff_zip"]
TARGET = "fare_amount"
# Pin the training env's numeric stack into the model so the serving container rebuilds the same ABI-compatible env
from importlib.metadata import version
PINS = [f"{p}=={version(p)}" for p in ("numpy", "pandas", "scipy", "pyarrow")]

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/tugas2_taxi_fare")
print("mlflow", mlflow.__version__)

pdf = spark.table(f"{cat}.{sch}.silver_taxi").select(*FEATURES, TARGET).toPandas().astype("float64")
X_train, X_test, y_train, y_test = train_test_split(pdf[FEATURES], pdf[TARGET], test_size=0.2, random_state=42)
print(f"train={len(X_train)} test={len(X_test)}")

# COMMAND ----------

candidates = {
    "linear_regression": LinearRegression(),
    "random_forest": RandomForestRegressor(n_estimators=100, max_depth=12, min_samples_leaf=3, n_jobs=-1, random_state=42),
}
results = {}
for name, model in candidates.items():
    with mlflow.start_run(run_name=name) as run:
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        metrics = {
            "mae": mean_absolute_error(y_test, pred),
            "rmse": float(np.sqrt(mean_squared_error(y_test, pred))),
            "r2": r2_score(y_test, pred),
        }
        mlflow.log_params({"model_type": name, "features": ",".join(FEATURES), "train_rows": len(X_train), **model.get_params()})
        mlflow.log_metrics(metrics)
        info = mlflow.sklearn.log_model(model, name="model", signature=infer_signature(X_test, pred), input_example=X_test.head(3),
                                        extra_pip_requirements=PINS)
        results[name] = {"run_id": run.info.run_id, "model_uri": info.model_uri, **metrics}
        print(f"{name:18s} MAE={metrics['mae']:.3f} RMSE={metrics['rmse']:.3f} R2={metrics['r2']:.3f}")

best_name = min(results, key=lambda k: results[k]["rmse"])
best = results[best_name]
print(f"Best model: {best_name} (RMSE={best['rmse']:.3f})")

# COMMAND ----------

# Register best model in Unity Catalog, alias it `champion` (used by the serving endpoint), then tag the runs
client = mlflow.MlflowClient()
mv = mlflow.register_model(best["model_uri"], model_name)
client.set_registered_model_alias(model_name, "champion", mv.version)
for name, r in results.items():
    client.set_tag(r["run_id"], "best_model", str(name == best_name).lower())
client.set_tag(best["run_id"], "registered_version", mv.version)
print(f"Registered {model_name} v{mv.version} as @champion")

dbutils.notebook.exit(f"{best_name}|v{mv.version}|rmse={best['rmse']:.3f}")
