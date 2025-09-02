# accessing pipeline name from config_pipeline_name file and accessing id of the pipeline with that name and then using databricks api for getting logs for job
import requests
import json
import pandas as pd

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

with open('config_pipeline_name.json') as f:
    config = json.load(f)

pipeline_name = config["pipeline_name"]

DATABRICKS_HOST = "https://adb-3958501268417377.17.azuredatabricks.net"
DATABRICKS_TOKEN = "*********************************"

headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

resp = requests.get(f"{DATABRICKS_HOST}/api/2.0/pipelines", headers=headers)
resp.raise_for_status()
pipelines = resp.json().get("statuses", [])

pipeline_id = next((p["pipeline_id"] for p in pipelines if p["name"] == pipeline_name), None)

if not pipeline_id:
    raise ValueError(f"Pipeline {pipeline_name} not found!")

print(f"Pipeline ID: {pipeline_id}")

logs_resp = requests.get(
    f"{DATABRICKS_HOST}/api/2.0/pipelines/{pipeline_id}/events",
    headers=headers
)
logs_resp.raise_for_status()
logs = logs_resp.json().get("events", [])

if not logs:
    print("No logs found for this pipeline.")
else:
    print(f"Fetched {len(logs)} logs for pipeline {pipeline_name}.")

logs_df = pd.json_normalize(logs, sep="_")

expected_cols = [
    "id",
    "timestamp",
    "message",
    "origin.cloud",
    "origin.pipeline_id",
    "origin.pipeline_name"
]

available_cols = [col for col in expected_cols if col in logs_df.columns]

logs_df = logs_df[available_cols]

spark_df = spark.createDataFrame(logs_df)

spark_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("pipeline_logs")
