# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

# from typing import Optional


class Upserter:
    """
    Thin foreachBatch adapter that runs a SQL MERGE statement.

    How it works
    ------------
    • Each micro-batch DataFrame is registered as a TEMP VIEW.
    • The provided MERGE SQL (target <- temp view) is executed in the same SparkSession.

    Why use this?
    -------------
    • Keeps your streaming logic in PySpark, while using Delta's MERGE for idempotent upserts.
    """

    def __init__(self, merge_query: str, temp_view_name: str) -> None:
        """
        Parameters
        ----------
        merge_query : str
            A complete MERGE INTO ... USING <temp view> ... statement.
        temp_view_name : str
            Name of the temp view to expose the micro-batch as.
        """
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id: int) -> None:
        """
        foreachBatch entrypoint.

        Parameters
        ----------
        df_micro_batch : pyspark.sql.DataFrame
            Micro-batch DataFrame to upsert.
        batch_id : int
            Micro-batch id (not used, but part of foreachBatch signature).
        """
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        # Execute MERGE in the same session (Delta Lake requires a session-level SQL call)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)


# COMMAND ----------

class Gold:
    """
    Gold layer (analytics-ready) builder.

    Responsibilities
    ----------------
    • Read curated Silver tables as streaming inputs.
    • Aggregate to stable, query-friendly facts (e.g., per-session BPM stats).
    • Enrich facts with small dimensions (demographics).
    • Provide validation against known-good fixtures.

    Tables / Views
    --------------
    • workout_bpm_summary (Delta, fact table)
    • gym_summary (VIEW) — validated here but created in the setup module
    """

    def __init__(self, env: str) -> None:
        conf = Config()
        self.test_data_dir: str = f"{conf.base_dir_data}/test_data"
        self.checkpoint_base: str = f"{conf.base_dir_checkpoint}/checkpoints"
        self.catalog: str = env
        self.db_name: str = conf.db_name
        self.maxFilesPerTrigger: int = int(conf.maxFilesPerTrigger)  # available if you want to enable later
        spark.sql(f"USE {self.catalog}.{self.db_name}")

    # --------------------------
    # Helpers
    # --------------------------
    def _qt(self, table: str) -> str:
        """Fully qualified table name '<catalog>.<db>.<table>'."""
        return f"{self.catalog}.{self.db_name}.{table}"

    def _ckpt(self, leaf: str) -> str:
        """Checkpoint path for a given stream leaf."""
        return f"{self.checkpoint_base}/{leaf}"

    @staticmethod
    def _set_pool(name: str) -> None:
        """Assign a scheduler pool (useful for multi-stream QoS)."""
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)

    # --------------------------
    # Facts
    # --------------------------
    def upsert_workout_bpm_summary(
        self,
        once: bool = True,
        processing_time: str = "15 seconds",
        startingVersion: int = 0,
    ):
        """
        Build/maintain `workout_bpm_summary` with per-session BPM stats.

        What it does
        ------------
        • Reads `workout_bpm` (Silver) as a stream.
        • Watermarks on `end_time` (30s) so late aggregates have a bounded window.
        • Groups by (user_id, workout_id, session_id, end_time) to compute min/avg/max/count.
        • Joins demographics from `user_bins` (Silver).
        • MERGE is insert-only (sessions are immutable once they complete).

        Triggering
        ----------
        • `once=True` → runs in `availableNow` mode (bounded catch-up).
        • Otherwise, runs continuous micro-batches with `processing_time`.

        Notes
        -----
        • We use `outputMode("append")` because each aggregate row is written once per session.
        """
        from pyspark.sql import functions as F

        # Insert-only MERGE: a completed session is immutable (idempotent)
        merge_sql = f"""
        MERGE INTO {self._qt("workout_bpm_summary")} a
        USING workout_bpm_summary_delta b
        ON  a.user_id    = b.user_id
        AND a.workout_id = b.workout_id
        AND a.session_id = b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(merge_sql, "workout_bpm_summary_delta")

        # Small dimension lookup (demographics)
        df_users = spark.read.table(self._qt("user_bins"))

        # Stream: aggregate BPM points per completed session
        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(self._qt("workout_bpm"))
            # Watermark bounds late-arriving BPM within a sensible window
            .withWatermark("end_time", "30 seconds")
            .groupBy("user_id", "workout_id", "session_id", "end_time")
            .agg(
                F.min("heartrate").alias("min_bpm"),
                F.avg("heartrate").alias("avg_bpm"),
                F.max("heartrate").alias("max_bpm"),
                F.count("heartrate").alias("num_recordings"),
            )
            .join(df_users, ["user_id"])  # enrich with demographics (Type-1 dim)
            .select(
                "workout_id", "session_id", "user_id",
                "age", "gender", "city", "state",
                "min_bpm", "avg_bpm", "max_bpm", "num_recordings",
            )
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)   # run MERGE per micro-batch
            .outputMode("append")            # each aggregate row written once
            .option("checkpointLocation", self._ckpt("workout_bpm_summary"))
            .queryName("workout_bpm_summary_upsert_stream")
        )

        self._set_pool("gold_p1")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Orchestration
    # --------------------------
    def upsert(self, once: bool = True, processing_time: str = "5 seconds") -> None:
        """
        Kick off all Gold streams and optionally wait for them to finish (availableNow).
        """
        import time
        start = int(time.time())
        print("\nExecuting gold layer upsert ...")

        self.upsert_workout_bpm_summary(once, processing_time)

        if once:
            for q in spark.streams.active:
                q.awaitTermination()

        print(f"Completed gold layer upsert in {int(time.time()) - start} seconds")

    # --------------------------
    # Validation helpers
    # --------------------------
    def assert_count(self, table_name: str, expected_count: int, filter_expr: str = "true") -> None:
        """
        Assert that `<table_name>` has exactly `expected_count` rows after applying `filter_expr`.
        """
        print(f"Validating record counts in {table_name}...", end="")
        actual = spark.read.table(self._qt(table_name)).where(filter_expr).count()
        assert actual == expected_count, (
            f"Expected {expected_count:,} records, found {actual:,} "
            f"in {table_name} WHERE {filter_expr}"
        )
        print(f"Found {actual:,} / Expected {expected_count:,} WHERE {filter_expr}: Success")

    def assert_rows(self, location_stem: str, table_name: str, sets: int) -> None:
        """
        Compare full table rows against a fixture parquet snapshot.

        Parameters
        ----------
        location_stem : str
            Base file stem under `test_data` (e.g., '7-gym_summary').
        table_name : str
            Fully qualified or session-resolved table/view name to read.
        sets : int
            Which snapshot variant to use (e.g., 1 or 2).
        """
        print(f"Validating records in {table_name}...", end="")
        expected = spark.read.format("parquet").load(f"{self.test_data_dir}/{location_stem}_{sets}.parquet").collect()
        actual = spark.table(table_name).collect()
        assert expected == actual, f"Expected data mismatches actual data in {table_name}"
        print(f"Expected data matches the actual data in {table_name}: Success")

    def validate(self, sets: int) -> None:
        """
        Validate Gold outputs against fixtures and simple counts.

        What it checks
        --------------
        • `gym_summary` VIEW matches the expected parquet snapshot for the given set.
        • `workout_bpm_summary` has a small expected count when sets > 1 (your current test).
        """
        import time
        start = int(time.time())
        print("\nValidating gold layer records...")

        # Byte-for-byte comparison with fixture parquet snapshot
        self.assert_rows("7-gym_summary", "gym_summary", sets)

        # Your test condition: only check count when more than one set is produced
        if sets > 1:
            self.assert_count("workout_bpm_summary", 2)

        print(f"Gold layer validation completed in {int(time.time()) - start} seconds")



# class Upserter:
#     def __init__(self, merge_query, temp_view_name):
#         self.merge_query = merge_query
#         self.temp_view_name = temp_view_name 
        
#     def upsert(self, df_micro_batch, batch_id):
#         df_micro_batch.createOrReplaceTempView(self.temp_view_name)
#         df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# # COMMAND ----------

# class Gold():
#     def __init__(self, env):
#         self.Conf = Config() 
#         self.test_data_dir = self.Conf.base_dir_data + "/test_data"
#         self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
#         self.catalog = env
#         self.db_name = self.Conf.db_name
#         self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
#         spark.sql(f"USE {self.catalog}.{self.db_name}")
        
#     def upsert_workout_bpm_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
        
#         #Idempotent - Once a workout session is complete, It doesn't change. So insert only the new records
#         query = f"""
#         MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
#         USING workout_bpm_summary_delta b
#         ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
#         WHEN NOT MATCHED THEN INSERT *
#         """
        
#         data_upserter=Upserter(query, "workout_bpm_summary_delta")
        
#         df_users = spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")
        
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          #.option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.workout_bpm")
#                          .withWatermark("end_time", "30 seconds")
#                          .groupBy("user_id", "workout_id", "session_id", "end_time")
#                          .agg(F.min("heartrate").alias("min_bpm"), F.mean("heartrate").alias("avg_bpm"), 
#                               F.max("heartrate").alias("max_bpm"), F.count("heartrate").alias("num_recordings"))                         
#                          .join(df_users, ["user_id"])
#                          .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings")
#                      )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("append")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
#                                  .queryName("workout_bpm_summary_upsert_stream")
#                         )
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "gold_p1")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
    
    
#     def upsert(self, once=True, processing_time="5 seconds"):
#         import time
#         start = int(time.time())
#         print(f"\nExecuting gold layer upsert ...")
#         self.upsert_workout_bpm_summary(once, processing_time)
#         if once:
#             for stream in spark.streams.active:
#                 stream.awaitTermination()
#         print(f"Completed gold layer upsert {int(time.time()) - start} seconds")
        
        
#     def assert_count(self, table_name, expected_count, filter="true"):
#         print(f"Validating record counts in {table_name}...", end='')
#         actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
#         assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
#         print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success") 
        
#     def assert_rows(self, location, table_name, sets):
#         print(f"Validating records in {table_name}...", end='')
#         expected_rows = spark.read.format("parquet").load(f"{self.test_data_dir}/{location}_{sets}.parquet").collect()
#         actual_rows = spark.table(table_name).collect()
#         assert expected_rows == actual_rows, f"Expected data mismatches with the actual data in {table_name}"
#         print(f"Expected data matches with the actual data in {table_name}: Success")
        
        
#     def validate(self, sets):
#         import time
#         start = int(time.time())
#         print(f"\nValidating gold layer records..." )       
#         self.assert_rows("7-gym_summary", "gym_summary", sets)       
#         if sets>1:
#             self.assert_count("workout_bpm_summary", 2)
#         print(f"Gold layer validation completed in {int(time.time()) - start} seconds")        
