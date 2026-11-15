# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · Export champion model to ONNX
# MAGIC Loads `taxi_fare_model@champion`, converts it to **ONNX** (skl2onnx), verifies the ONNX Runtime output
# MAGIC against sklearn on Silver data, and logs `onnx/taxi_fare_model.onnx` to the best MLflow run.

# COMMAND ----------

# Freeze the preinstalled core libs so the ONNX tooling cannot up/downgrade them (that breaks pandas/numpy ABI)
from importlib.metadata import PackageNotFoundError, version

pins = []
for p in ["numpy", "pandas", "scipy", "scikit-learn", "protobuf", "pyarrow"]:
    try:
        pins.append(f"{p}=={version(p)}")
    except PackageNotFoundError:
        pass
open("/tmp/constraints.txt", "w").write("\n".join(pins))
print(pins)

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow-skinny[databricks]" skl2onnx onnxruntime -c /tmp/constraints.txt
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("model_name", "workspace.tugas2.taxi_fare_model")
dbutils.widgets.text("silver_table", "workspace.tugas2.silver_taxi")

# COMMAND ----------

import mlflow
import numpy as np
import onnxruntime as ort
from skl2onnx import to_onnx

FEATURES = ["trip_distance", "trip_duration_min", "pickup_hour", "pickup_dow", "pickup_zip", "dropoff_zip"]
model_name = dbutils.widgets.get("model_name")
mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(model_name, "champion")
model = mlflow.sklearn.load_model(f"models:/{model_name}@champion")

X = spark.table(dbutils.widgets.get("silver_table")).select(*FEATURES).limit(2000).toPandas().astype("float64")
onx = to_onnx(model, X.values[:1].astype(np.float32), target_opset=17)
sess = ort.InferenceSession(onx.SerializeToString())
onnx_pred = sess.run(None, {sess.get_inputs()[0].name: X.values.astype(np.float32)})[0].ravel()
diff = float(np.mean(np.abs(onnx_pred - model.predict(X))))
assert diff < 0.05, f"ONNX output drifts from sklearn: mean abs diff {diff}"

path = "/tmp/taxi_fare_model.onnx"
with open(path, "wb") as f:
    f.write(onx.SerializeToString())
with mlflow.start_run(run_id=mv.run_id):
    mlflow.log_artifact(path, artifact_path="onnx")
    mlflow.log_metric("onnx_mean_abs_diff", diff)
print(f"ONNX export OK for v{mv.version} (run {mv.run_id}); mean abs diff vs sklearn = {diff:.5f}")
dbutils.notebook.exit(f"onnx ok v{mv.version} diff={diff:.5f}")
