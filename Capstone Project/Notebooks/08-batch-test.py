# Databricks notebook source
# ---------------------------
# 08-batch-test: End-to-end batch validation
# ---------------------------
# Purpose:
# • Clean environment
# • Create schema + seed lookups
# • Produce deterministic test data (set 1 & 2)
# • Run pipeline in batch mode (availableNow)
# • Validate Bronze/Silver/Gold outputs
# • Clean up
# ---------------------------

# === Widgets (env selector) ===
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# Load setup helpers
# MAGIC %run ./02-setup

# COMMAND ----------

def _clean_environment(env: str) -> None:
    """
    Drop the application database (CASCADE) and remove landing/checkpoint files.

    This ensures a pristine state so the test is reproducible.
    """
    SH = SetupHelper(env)
    SH.cleanup()

# COMMAND ----------

# First: hard reset the environment.
_clean_environment(env)

# COMMAND ----------

def _run_pipeline_once(env: str, timeout_seconds: int = 600) -> None:
    """
    Execute the orchestrator notebook (07-run) in batch mode (availableNow).

    Parameters
    ----------
    env : str
        Unity Catalog catalog (e.g., 'dev', 'test', 'prod').
    timeout_seconds : int
        Max allowed runtime for the invoked notebook.
    """
    dbutils.notebook.run(
        "./07-run",
        timeout_seconds,
        {"Environment": env, "RunType": "once"}  # availableNow / batch
    )

# COMMAND ----------

# Initial run to create DB/tables, seed lookups, and verify.
_run_pipeline_once(env)

# COMMAND ----------

# Seed validation: date dimension etc.
# MAGIC %run ./03-history-loader

# COMMAND ----------

HL = HistoryLoader(env)
SH = SetupHelper(env)

# Validate that setup + history load succeeded during the first run
SH.validate()
HL.validate()

# COMMAND ----------

# Test data producer + validators
# MAGIC %run ./10-producer

# COMMAND ----------

def _produce_and_validate_set(set_id: int) -> None:
    """
    Produce a deterministic test set into the landing zone and validate raw counts.

    Parameters
    ----------
    set_id : int
        1 or 2 (as currently supported by the Producer).
    """
    PR = Producer()
    PR.produce(set_id)
    PR.validate(set_id)

# COMMAND ----------

# Load layer classes for validation helpers
# MAGIC %run ./04-bronze
# COMMAND ----------
# MAGIC %run ./05-silver
# COMMAND ----------
# MAGIC %run ./06-gold

# COMMAND ----------

def _instantiate_layers(env: str):
    """
    Create Bronze/Silver/Gold instances for the selected environment.
    """
    return Bronze(env), Silver(env), Gold(env)

# COMMAND ----------

def _validate_layers(env: str, produced_sets: int) -> None:
    """
    Run layer-level validations against deterministic expectations.

    Parameters
    ----------
    env : str
        Target catalog.
    produced_sets : int
        Number of produced data sets (1 or 2).
    """
    BZ, SL, GL = _instantiate_layers(env)
    BZ.validate(produced_sets)
    SL.validate(produced_sets)
    GL.validate(produced_sets)

# COMMAND ----------

# ===== Run test: SET 1 =====
_produce_and_validate_set(1)
_run_pipeline_once(env)         # batch catch-up for set 1
_validate_layers(env, produced_sets=1)

# COMMAND ----------

# ===== Run test: SET 2 =====
_produce_and_validate_set(2)
_run_pipeline_once(env)         # batch catch-up for set 2
_validate_layers(env, produced_sets=2)

# COMMAND ----------

# Final cleanup so the test is idempotent across executions.
_clean_environment(env)







# # Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# env = dbutils.widgets.get("Environment")

# # COMMAND ----------

# # MAGIC %run ./02-setup

# # COMMAND ----------

# SH = SetupHelper(env)
# SH.cleanup()

# # COMMAND ----------

# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# # COMMAND ----------

# # MAGIC %run ./03-history-loader

# # COMMAND ----------

# HL = HistoryLoader(env)
# SH.validate()
# HL.validate()

# # COMMAND ----------

# # MAGIC %run ./10-producer

# # COMMAND ----------

# PR =Producer()
# PR.produce(1)
# PR.validate(1)
# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# # COMMAND ----------

# # MAGIC %run ./04-bronze

# # COMMAND ----------

# # MAGIC %run ./05-silver

# # COMMAND ----------

# # MAGIC %run ./06-gold

# # COMMAND ----------

# BZ = Bronze(env)
# SL = Silver(env)
# GL = Gold(env)
# BZ.validate(1)
# SL.validate(1)
# GL.validate(1)

# # COMMAND ----------

# PR.produce(2)
# PR.validate(2)
# dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# # COMMAND ----------

# BZ.validate(2)
# SL.validate(2)
# GL.validate(2)
# SH.cleanup()
