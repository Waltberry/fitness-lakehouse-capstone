# Databricks notebook source
# ---------------------------
# 09-stream-test: End-to-end STREAMING validation via Databricks Jobs API
#
# Flow:
#  1) Clean environment (drops DB, deletes landing/checkpoints).
#  2) Create a one-off Job that runs 07-run in streaming mode.
#  3) Start the Job and wait until it is RUNNING.
#  4) Load helpers, seed lookups, produce deterministic test data.
#  5) Sleep → validate Bronze/Silver/Gold for set 1.
#  6) Produce more data (set 2), sleep → validate again.
#  7) Cancel the running Job and delete it.
# ---------------------------

# === Widgets (UI controls) ===
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("Host", "", "Databricks Workspace URL (e.g., https://adb-xxx.azuredatabricks.net)")
dbutils.widgets.text("AccessToken", "", "Personal Access Token (PAT)")

env = dbutils.widgets.get("Environment")
host = dbutils.widgets.get("Host").rstrip("/")          # normalize trailing slash
token = dbutils.widgets.get("AccessToken")

# COMMAND ----------

# Load setup utilities
# MAGIC %run ./02-setup

# COMMAND ----------

# 1) Start with a clean slate so this test is reproducible
SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

# ---- Jobs API helpers --------------------------------------------------------
import json
import time
import requests
from typing import Any, Dict, Optional

def _api_url(path: str) -> str:
    """Build a full API URL from a relative path."""
    return f"{host}{path}"

def _headers() -> Dict[str, str]:
    """Standard JSON headers for Databricks Jobs API."""
    return {"Content-Type": "application/json"}

def _post(path: str, payload: Dict[str, Any]) -> requests.Response:
    """POST wrapper with JSON body and PAT auth."""
    return requests.post(_api_url(path), headers=_headers(), data=json.dumps(payload), auth=("token", token))

def _get(path: str, payload: Dict[str, Any]) -> requests.Response:
    """GET wrapper with JSON body and PAT auth."""
    return requests.get(_api_url(path), headers=_headers(), data=json.dumps(payload), auth=("token", token))

def _ensure_ok(resp: requests.Response, action: str) -> Dict[str, Any]:
    """Raise a helpful error if the API call failed; otherwise return parsed JSON."""
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if not resp.ok:
        raise RuntimeError(f"{action} failed (HTTP {resp.status_code}): {data}")
    return data

# COMMAND ----------

def build_job_payload(env: str) -> Dict[str, Any]:
    """
    Construct a single-task 'stream-test' job that runs 07-run from the workspace.
    Uses a single-node cluster to keep things lightweight.
    """
    return {
        "name": "stream-test",
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "tasks": [
            {
                "task_key": "stream-test-task",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "/Repos/SBIT/SBIT/07-run",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    # Keep aligned with your runtime; single-node is fine for tests
                    "spark_version": "13.3.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1,
                    },
                    "node_type_id": "Standard_DS4_v2",
                    "driver_node_type_id": "Standard_DS4_v2",
                    "custom_tags": {"ResourceClass": "SingleNode"},
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0,
                },
            }
        ],
    }

def create_job(env: str) -> int:
    """Create the Job in the workspace; return job_id."""
    payload = build_job_payload(env)
    resp = _post("/api/2.1/jobs/create", payload)
    data = _ensure_ok(resp, "Job creation")
    job_id = int(data["job_id"])
    print(f"Created Job {job_id}")
    return job_id

def start_job(job_id: int, env: str, processing_time: str = "1 seconds") -> int:
    """
    Trigger the job in STREAM mode by passing notebook parameters.
    Returns the run_id.
    """
    run_payload = {
        "job_id": job_id,
        "notebook_params": {"Environment": env, "RunType": "stream", "ProcessingTime": processing_time},
    }
    resp = _post("/api/2.1/jobs/run-now", run_payload)
    data = _ensure_ok(resp, "Job run start")
    run_id = int(data["run_id"])
    print(f"Started Job run {run_id}")
    return run_id

def wait_until_running(run_id: int, poll_seconds: int = 20, timeout_seconds: int = 20 * 60) -> None:
    """
    Poll the run until the first task enters the RUNNING state or we time out.
    """
    print("Waiting for job to start ...")
    start = time.time()
    state = "PENDING"
    payload = {"run_id": run_id}

    while True:
        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Timed out waiting for run {run_id} to start.")
        time.sleep(poll_seconds)
        resp = _get("/api/2.1/jobs/runs/get", payload)
        data = _ensure_ok(resp, "Get run status")
        state = data["tasks"][0]["state"]["life_cycle_state"]
        print(f"Job state: {state}")
        if state in ("RUNNING", "TERMINATING", "TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            # RUNNING = good; the others indicate it's already finishing/finished or errored
            break

def cancel_run(run_id: int) -> None:
    """Cancel a running job run."""
    resp = _post("/api/2.1/jobs/runs/cancel", {"run_id": run_id})
    _ensure_ok(resp, "Cancel run")
    print(f"Canceled Job run {run_id}")

def delete_job(job_id: int) -> None:
    """Delete the job definition from the workspace."""
    resp = _post("/api/2.1/jobs/delete", {"job_id": job_id})
    _ensure_ok(resp, "Delete job")
    print(f"Deleted Job {job_id}")

# COMMAND ----------

# Create & start the streaming job, then wait for it to be RUNNING.
job_id = create_job(env)
run_id = start_job(job_id, env, processing_time="1 seconds")
wait_until_running(run_id, poll_seconds=20, timeout_seconds=20 * 60)

# COMMAND ----------

# Bring in the rest of the repo (history loader, producer, layer classes)
# MAGIC %run ./03-history-loader
# COMMAND ----------
# MAGIC %run ./10-producer
# COMMAND ----------
# MAGIC %run ./04-bronze
# COMMAND ----------
# MAGIC %run ./05-silver
# COMMAND ----------
# MAGIC %run ./06-gold

# COMMAND ----------

# Instantiate helpers used for validation
HL = HistoryLoader(env)
PR = Producer()
BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

# Give the orchestrator job time to initialize & run its setup/history load steps.
print("Sleeping 2 minutes to allow setup and history loader to complete...")
time.sleep(2 * 60)

# Validate setup + history (date dimension)
SH.validate()
HL.validate()

# ----- Increment 1 ------------------------------------------------------------
# Produce first deterministic batch into landing zone and validate raw counts.
PR.produce(1)
PR.validate(1)

print("Sleeping 2 minutes to allow micro-batches to ingest set 1...")
time.sleep(2 * 60)

# Validate Bronze/Silver/Gold tables for set 1.
BZ.validate(1)
SL.validate(1)
GL.validate(1)

# ----- Increment 2 ------------------------------------------------------------
# Produce the second deterministic batch and validate raw counts.
PR.produce(2)
PR.validate(2)

print("Sleeping 2 minutes to allow micro-batches to ingest set 2...")
time.sleep(2 * 60)

# Validate again after set 2.
BZ.validate(2)
SL.validate(2)
GL.validate(2)

# COMMAND ----------

# Tear down the temporary streaming job regardless of test outcome
try:
    cancel_run(run_id)
except Exception as e:
    print(f"Cancel run failed or already canceled: {e}")

try:
    delete_job(job_id)
except Exception as e:
    print(f"Delete job failed or already deleted: {e}")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")




# # Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# dbutils.widgets.text("Host", "", "Databricks Workspace URL")
# dbutils.widgets.text("AccessToken", "", "Secure Access Token")

# # COMMAND ----------

# env = dbutils.widgets.get("Environment")
# host = dbutils.widgets.get("Host")
# token = dbutils.widgets.get("AccessToken")

# # COMMAND ----------

# # MAGIC %run ./02-setup

# # COMMAND ----------

# SH = SetupHelper(env)
# SH.cleanup()

# # COMMAND ----------

# job_payload = \
# {
#         "name": "stream-test",
#         "webhook_notifications": {},
#         "timeout_seconds": 0,
#         "max_concurrent_runs": 1,
#         "tasks": [
#             {
#                 "task_key": "stream-test-task",
#                 "run_if": "ALL_SUCCESS",
#                 "notebook_task": {
#                     "notebook_path": "/Repos/SBIT/SBIT/07-run",
#                     "source": "WORKSPACE"
#                 },
#                 "job_cluster_key": "Job_cluster",
#                 "timeout_seconds": 0,
#                 "email_notifications": {}
#             }
#         ],
#         "job_clusters": [
#             {
#                 "job_cluster_key": "Job_cluster",
#                 "new_cluster": {
#                     "spark_version": "13.3.x-scala2.12",
#                     "spark_conf": {
#                         "spark.databricks.delta.preview.enabled": "true",
#                         "spark.master": "local[*, 4]",
#                         "spark.databricks.cluster.profile": "singleNode"
#                     },
#                     "azure_attributes": {
#                         "first_on_demand": 1,
#                         "availability": "ON_DEMAND_AZURE",
#                         "spot_bid_max_price": -1
#                     },
#                     "node_type_id": "Standard_DS4_v2",
#                     "driver_node_type_id": "Standard_DS4_v2",
#                     "custom_tags": {
#                         "ResourceClass": "SingleNode"
#                     },
#                     "data_security_mode": "SINGLE_USER",
#                     "runtime_engine": "STANDARD",
#                     "num_workers": 0
#                 }
#             }
#         ],
#         "format": "MULTI_TASK"
#     }

# # COMMAND ----------

# # Create a streaming job
# import requests
# import json
# create_response = requests.post(host + '/api/2.1/jobs/create', data=json.dumps(job_payload), auth=("token", token))
# print(f"Response: {create_response}")
# job_id = json.loads(create_response.content.decode('utf-8'))["job_id"]
# print(f"Created Job {job_id}")

# # COMMAND ----------

# # Trigger the streaming job
# run_payload = {"job_id": job_id, "notebook_params": {"Environment":env, "RunType": "stream", "ProcessingTime": "1 seconds"}}
# run_response = requests.post(host + '/api/2.1/jobs/run-now', data=json.dumps(run_payload), auth=("token", token))
# run_id = json.loads(run_response.content.decode('utf-8'))["run_id"]
# print(f"Started Job run {run_id}")

# # COMMAND ----------

# # Wait until job starts
# import time
# status_payload = {"run_id": run_id}
# job_status="PENDING"
# while job_status == "PENDING":
#     time.sleep(20)
#     status_job_response = requests.get(host + '/api/2.1/jobs/runs/get', data=json.dumps(status_payload), auth=("token", token))
#     job_status = json.loads(status_job_response.content.decode('utf-8'))["tasks"][0]["state"]["life_cycle_state"]  
#     print(job_status)    

# # COMMAND ----------

# # MAGIC %run ./03-history-loader

# # COMMAND ----------

# # MAGIC %run ./10-producer

# # COMMAND ----------

# # MAGIC %run ./04-bronze

# # COMMAND ----------

# # MAGIC %run ./05-silver

# # COMMAND ----------

# # MAGIC %run ./06-gold

# # COMMAND ----------

# import time

# print("Sleep for 2 minutes and let setup and history loader finish...")
# time.sleep(2*60)

# #Validate setup and history load
# HL = HistoryLoader(env)
# PR = Producer()
# BZ = Bronze(env)
# SL = Silver(env)
# GL = Gold(env)

# SH.validate()
# HL.validate()

# #Produce some incremantal
# PR.produce(1)
# PR.validate(1)

# # COMMAND ----------

# print("Sleep for 2 minutes and let microbatch pickup the data...")
# time.sleep(2*60)

# #Validate bronze, silver and gold layer 
# BZ.validate(1)
# SL.validate(1)
# GL.validate(1)
 

# #Produce some incremantal data and wait for micro batch
# PR.produce(2)
# PR.validate(2)

# # COMMAND ----------

# print("Sleep for 2 minutes and let microbatch pickup the data...")
# time.sleep(2*60)

# #Validate bronze, silver and gold layer 
# BZ.validate(2)
# SL.validate(2)
# GL.validate(2)

# # COMMAND ----------

# #Terminate the streaming Job
# cancel_payload = {"run_id": run_id}
# cancel_response = requests.post(host + '/api/2.1/jobs/runs/cancel', data=json.dumps(cancel_payload), auth=("token", token))
# print(f"Canceled Job run {run_id}. Status {cancel_response}")

# # COMMAND ----------

# #Delete the Job
# delete_job_payload = {"job_id": job_id}
# delete_job_response = requests.post(host + '/api/2.1/jobs/delete', data=json.dumps(delete_job_payload), auth=("token", token))
# print(f"Canceled Job run {run_id}. Status {delete_job_response}")

# # COMMAND ----------

# dbutils.notebook.exit("SUCCESS")
