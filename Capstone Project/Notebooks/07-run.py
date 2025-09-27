# Databricks notebook source
# ---------------------------
#  Orchestrator: Run Bronze → Silver → Gold
#  - Reads widgets (env/run mode)
#  - Configures Spark/Delta for streaming
#  - Ensures DB/tables are created + seeded on first run
#  - Starts Bronze (ingest), Silver (upserts), Gold (aggregates)
# ---------------------------

# === Widgets (UI controls) ===
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("RunType", "once", "Set 'once' to run batch/availableNow; anything else for streaming")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Micro-batch interval (e.g., '5 seconds')")

# COMMAND ----------

def _parse_widgets():
    """
    Read notebook widgets and normalize values for downstream code.

    Returns
    -------
    env : str
        Unity Catalog catalog/environment (e.g., 'dev', 'test', 'prod').
    once : bool
        True → run in batch catch-up mode (availableNow), False → continuous streaming.
    processing_time : str
        Trigger interval when once=False (e.g., '5 seconds').
    """
    env = dbutils.widgets.get("Environment")
    once = dbutils.widgets.get("RunType") == "once"
    processing_time = dbutils.widgets.get("ProcessingTime")

    if once:
        print("Starting SBIT in BATCH mode (availableNow).")
    else:
        print(f"Starting SBIT in STREAM mode with micro-batch interval = {processing_time}.")

    return env, once, processing_time


# COMMAND ----------

def _configure_spark_for_streaming():
    """
    Set Spark/Delta configs that are beneficial for structured streaming jobs.

    Notes
    -----
    • spark.sql.shuffle.partitions          : sized to cluster (use defaultParallelism here)
    • delta.optimizeWrite / autoCompact     : keep file sizes healthy over time
    • RocksDB state store                   : robust state backend for joins/aggregations
    """
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
    )


# COMMAND ----------

# Bring in setup and seeding helpers
# MAGIC %run ./02-setup
# COMMAND ----------
# MAGIC %run ./03-history-loader

# COMMAND ----------

def _ensure_setup(env: str):
    """
    Ensure the database/tables exist; if not, create them and load seed/lookup data.

    Steps
    -----
    1) Instantiate SetupHelper + HistoryLoader for the chosen catalog.
    2) If database is missing:
         • create all Bronze/Silver/Gold tables/views
         • validate schema presence
         • load/validate lookup history (e.g., date dimension)
       Else:
         • switch USE to the existing database
    """
    SH = SetupHelper(env)
    HL = HistoryLoader(env)

    # Has the app DB already been created?
    db_exists = (
        spark.sql(f"SHOW DATABASES IN {SH.catalog}")
        .filter(f"databaseName == '{SH.db_name}'")
        .count()
        == 1
    )

    if not db_exists:
        SH.setup()
        SH.validate()
        HL.load_history()
        HL.validate()
    else:
        spark.sql(f"USE {SH.catalog}.{SH.db_name}")

    return SH, HL


# COMMAND ----------

# Bring in layer implementations after environment is ready
# MAGIC %run ./04-bronze
# COMMAND ----------
# MAGIC %run ./05-silver
# COMMAND ----------
# MAGIC %run ./06-gold

# COMMAND ----------

def _run_layers(env: str, once: bool, processing_time: str):
    """
    Instantiate Bronze/Silver/Gold and execute the pipeline.

    Execution order
    ---------------
    1) Bronze.consume(...)         : Ingest raw CSV/JSON → Bronze Delta (append + checkpoints)
    2) Silver.upsert(...)          : Dedupe/CDC/joins MERGE → curated Silver facts/dims
    3) Gold.upsert(...)            : Aggregates per session → analytics-ready facts

    Parameters
    ----------
    env : str
        Target catalog (matches where SetupHelper created the DB).
    once : bool
        True for availableNow (bounded batch), False for continuous streaming.
    processing_time : str
        Micro-batch trigger interval when once=False (e.g., "5 seconds").
    """
    BZ = Bronze(env)
    SL = Silver(env)
    GL = Gold(env)

    # --- Bronze: raw ingest to append-only Delta ---
    BZ.consume(once, processing_time)

    # --- Silver: idempotent MERGE upserts + sessionization ---
    SL.upsert(once, processing_time)

    # --- Gold: aggregates/enrichment for analytics ---
    GL.upsert(once, processing_time)


# COMMAND ----------

# ===============Main orchestration================
env, once, processing_time = _parse_widgets()
_configure_spark_for_streaming()
SH, HL = _ensure_setup(env)
_run_layers(env, once, processing_time)
# ================================================




# # Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
# dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# # COMMAND ----------

# env = dbutils.widgets.get("Environment")
# once = True if dbutils.widgets.get("RunType")=="once" else False
# processing_time = dbutils.widgets.get("ProcessingTime")
# if once:
#     print(f"Starting sbit in batch mode.")
# else:
#     print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# # COMMAND ----------

# spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
# spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
# spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
# spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# # COMMAND ----------

# # MAGIC %run ./02-setup

# # COMMAND ----------

# # MAGIC %run ./03-history-loader

# # COMMAND ----------

# SH = SetupHelper(env)
# HL = HistoryLoader(env)

# # COMMAND ----------

# setup_required = spark.sql(f"SHOW DATABASES IN {SH.catalog}").filter(f"databaseName == '{SH.db_name}'").count() != 1
# if setup_required:
#     SH.setup()
#     SH.validate()
#     HL.load_history()
#     HL.validate()
# else:
#     spark.sql(f"USE {SH.catalog}.{SH.db_name}")

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

# # COMMAND ----------

# BZ.consume(once, processing_time)

# # COMMAND ----------

# SL.upsert(once, processing_time)

# # COMMAND ----------

# GL.upsert(once, processing_time)
