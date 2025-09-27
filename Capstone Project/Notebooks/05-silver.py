# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

# from typing import Optional


class Upserter:
    """
    Thin foreachBatch adapter to run a SQL MERGE statement.

    How it works
    ------------
    • Each micro-batch DataFrame is materialized as a TEMP VIEW.
    • The provided MERGE SQL (target <- temp view) is executed in the same SparkSession.
    """

    def __init__(self, merge_query: str, temp_view_name: str) -> None:
        """
        Parameters
        ----------
        merge_query : str
            A complete MERGE INTO ... USING <temp view> ... statement.
        temp_view_name : str
            Name of the temp view used to expose the micro-batch DataFrame.
        """
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id: int) -> None:
        """
        foreachBatch entrypoint.

        Parameters
        ----------
        df_micro_batch : pyspark.sql.DataFrame
            The micro-batch to merge.
        batch_id : int
            Micro-batch id (unused here but part of the signature).
        """
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        # Execute the MERGE with the session tied to this DataFrame
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)


# COMMAND ----------

class CDCUpserter:
    """
    foreachBatch adapter for Change-Data-Capture (CDC) streams.

    Behavior
    --------
    • Filters to CDC rows with update_type in ('new','update').
    • Keeps only the newest record per id (by `sort_by` descending) in the micro-batch.
    • Exposes that deduped micro-batch as a TEMP VIEW and runs the provided MERGE.
    """

    def __init__(
        self,
        merge_query: str,
        temp_view_name: str,
        id_column: str,
        sort_by: str,
    ) -> None:
        """
        Parameters
        ----------
        merge_query : str
            MERGE statement to execute.
        temp_view_name : str
            Temp view name to register the micro-batch as.
        id_column : str
            Key to partition/window CDC rows by (e.g., 'user_id').
        sort_by : str
            Column to order by (newest first), e.g., 'updated'.
        """
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name
        self.id_column = id_column
        self.sort_by = sort_by

    def upsert(self, df_micro_batch, batch_id: int) -> None:
        """
        Apply CDC dedupe (rank 1 by id, newest first) then MERGE.
        """
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F

        window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())

        (df_micro_batch
            .filter(F.col("update_type").isin(["new", "update"]))  # allowed CDC actions
            .withColumn("rank", F.rank().over(window))             # newest rows get rank=1
            .filter("rank = 1")
            .drop("rank")
            .createOrReplaceTempView(self.temp_view_name)
        )
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)


# COMMAND ----------

class Silver:
    """
    Silver layer (curated) streaming upserts.

    Responsibilities
    ----------------
    • Consume Bronze tables (append-only) as streaming sources.
    • Enforce idempotency via dedupe keys + watermarks for late data.
    • Upsert into Silver Delta tables using MERGE in foreachBatch.
    • Build small Type-1 dimensions and sessionized facts.

    Design notes
    ------------
    • Watermarks bound state & define late-arrival windows (here: 30s).
    • Dedupe keys are chosen to prevent duplicates (e.g., (user_id,time)).
    • MERGE keys reflect uniqueness of targets (e.g., device_id+time for heart rate).
    • Some streams join other streams; we add state cleanup conditions (e.g., 3 hour caps).
    """

    def __init__(self, env: str) -> None:
        conf = Config()
        self.checkpoint_base: str = f"{conf.base_dir_checkpoint}/checkpoints"
        self.catalog: str = env
        self.db_name: str = conf.db_name
        self.maxFilesPerTrigger: int = int(conf.maxFilesPerTrigger)  # available if you wish to enable
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
    # Users (bronze.registered_users_bz → silver.users)
    # --------------------------
    def upsert_users(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        Insert-only upsert for users (registrations are immutable).

        MERGE logic
        -----------
        • NOT MATCHED → INSERT (*)
        • No UPDATE path (registration records are immutable here).

        Stream details
        --------------
        • Watermark on registration_timestamp (30s) to bound late data.
        • Dedupe on (user_id, device_id).
        """
        from pyspark.sql import functions as F

        query = f"""
            MERGE INTO {self._qt("users")} a
            USING users_delta b
            ON a.user_id = b.user_id
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "users_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)  # tolerate bronze maintenance on partitions
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(self._qt("registered_users_bz"))
            .selectExpr("user_id", "device_id", "mac_address",
                        "CAST(registration_timestamp AS timestamp) AS registration_timestamp")
            .withWatermark("registration_timestamp", "30 seconds")
            .dropDuplicates(["user_id", "device_id"])
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")  # we are ignoring deletes + deduping
            .option("checkpointLocation", self._ckpt("users"))
            .queryName("users_upsert_stream")
        )

        self._set_pool("silver_p2")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Gym logs (bronze.gym_logins_bz → silver.gym_logs)
    # --------------------------
    def upsert_gym_logs(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        Upsert login/logout records.

        MERGE logic
        -----------
        • MATCHED when (mac_address, gym, login) match exactly:
            UPDATE logout if (logout > login) AND (logout > existing logout)
        • NOT MATCHED → INSERT (*)

        Stream details
        --------------
        • Watermark on login (30s).
        • Dedupe on (mac_address, gym, login).
        """
        from pyspark.sql import functions as F

        query = f"""
            MERGE INTO {self._qt("gym_logs")} a
            USING gym_logs_delta b
            ON a.mac_address = b.mac_address AND a.gym = b.gym AND a.login = b.login
            WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
              THEN UPDATE SET logout = b.logout
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "gym_logs_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("gym_logins_bz"))
            .selectExpr("mac_address", "gym",
                        "CAST(login AS timestamp) AS login",
                        "CAST(logout AS timestamp) AS logout")
            .withWatermark("login", "30 seconds")
            .dropDuplicates(["mac_address", "gym", "login"])
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", self._ckpt("gym_logs"))
            .queryName("gym_logs_upsert_stream")
        )

        self._set_pool("silver_p3")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # User profile CDC (bronze.kafka_multiplex_bz → silver.user_profile)
    # --------------------------
    def upsert_user_profile(self, once: bool = False, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        CDC upsert for user profiles.

        MERGE logic
        -----------
        • MATCHED when user_id matches: UPDATE * if incoming updated > existing updated
        • NOT MATCHED → INSERT *

        CDC policy
        ----------
        • Only rows with update_type IN ('new','update') are considered.
        • Latest record per user_id (by updated desc) in the *micro-batch* wins.

        Stream details
        --------------
        • Watermark on updated (30s).
        • Dedupe on (user_id, updated).
        """
        from pyspark.sql import functions as F

        schema = """
            user_id BIGINT, update_type STRING, timestamp FLOAT,
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING,
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
        """

        query = f"""
            MERGE INTO {self._qt("user_profile")} a
            USING user_profile_cdc b
            ON a.user_id = b.user_id
            WHEN MATCHED AND a.updated < b.updated
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """
        upserter = CDCUpserter(query, "user_profile_cdc", "user_id", "updated")

        df_cdc = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("kafka_multiplex_bz"))
            .filter("topic = 'user_info'")
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
            .select("v.*")
            .select(
                "user_id",
                F.to_date("dob", "MM/dd/yyyy").alias("dob"),
                "sex", "gender", "first_name", "last_name",
                "address.street_address", "address.city", "address.state", "address.zip",
                F.col("timestamp").cast("timestamp").alias("updated"),
                "update_type",
            )
            .withWatermark("updated", "30 seconds")
            .dropDuplicates(["user_id", "updated"])
        )

        writer = (
            df_cdc.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", self._ckpt("user_profile"))
            .queryName("user_profile_stream")
        )

        self._set_pool("silver_p3")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Workouts (bronze.kafka_multiplex_bz → silver.workouts)
    # --------------------------
    def upsert_workouts(self, once: bool = False, processing_time: str = "10 seconds", startingVersion: int = 0):
        """
        Insert-only upsert for workout events.

        MERGE logic
        -----------
        • NOT MATCHED → INSERT (*) keyed by (user_id, time).

        Stream details
        --------------
        • Watermark on time (30s).
        • Dedupe on (user_id, time) to prevent duplicate actions at the same instant.
        """
        from pyspark.sql import functions as F

        schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"

        query = f"""
            MERGE INTO {self._qt("workouts")} a
            USING workouts_delta b
            ON a.user_id = b.user_id AND a.time = b.time
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "workouts_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("kafka_multiplex_bz"))
            .filter("topic = 'workout'")
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
            .select("v.*")
            .select(
                "user_id", "workout_id",
                F.col("timestamp").cast("timestamp").alias("time"),
                "action", "session_id",
            )
            .withWatermark("time", "30 seconds")
            .dropDuplicates(["user_id", "time"])
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", self._ckpt("workouts"))
            .queryName("workouts_upsert_stream")
        )

        self._set_pool("silver_p3")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Heart rate (bronze.kafka_multiplex_bz → silver.heart_rate)
    # --------------------------
    def upsert_heart_rate(self, once: bool = False, processing_time: str = "10 seconds", startingVersion: int = 0):
        """
        Insert-only upsert for BPM telemetry with validity flag.

        MERGE logic
        -----------
        • NOT MATCHED → INSERT (*) keyed by (device_id, time).

        Stream details
        --------------
        • Watermark on time (30s).
        • Dedupe on (device_id, time).
        • 'valid' marks positive BPM measurements only.
        """
        from pyspark.sql import functions as F

        schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"

        query = f"""
            MERGE INTO {self._qt("heart_rate")} a
            USING heart_rate_delta b
            ON a.device_id = b.device_id AND a.time = b.time
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "heart_rate_delta")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("kafka_multiplex_bz"))
            .filter("topic = 'bpm'")
            .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
            .select(
                "v.device_id", "v.time", "v.heartrate",
                (F.col("v.heartrate") > 0).alias("valid"),
            )
            .withWatermark("time", "30 seconds")
            .dropDuplicates(["device_id", "time"])
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", self._ckpt("heart_rate"))
            .queryName("heart_rate_upsert_stream")
        )

        self._set_pool("silver_p2")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Dimension helpers
    # --------------------------
    @staticmethod
    def age_bins(dob_col):
        """
        Compute an age bucket label from a DOB column.

        Bins
        ----
        under 18, 18-25, 25-35, 35-45, 45-55, 55-65, 65-75, 75-85, 85-95, 95+
        """
        from pyspark.sql import functions as F

        # Compute age in whole years (no alias here to keep comparisons clean)
        age_years = F.floor(F.months_between(F.current_date(), dob_col) / 12)

        return (
            F.when(age_years < 18, "under 18")
            .when((age_years >= 18) & (age_years < 25), "18-25")
            .when((age_years >= 25) & (age_years < 35), "25-35")
            .when((age_years >= 35) & (age_years < 45), "35-45")
            .when((age_years >= 45) & (age_years < 55), "45-55")
            .when((age_years >= 55) & (age_years < 65), "55-65")
            .when((age_years >= 65) & (age_years < 75), "65-75")
            .when((age_years >= 75) & (age_years < 85), "75-85")
            .when((age_years >= 85) & (age_years < 95), "85-95")
            .when(age_years >= 95, "95+")
            .otherwise("invalid age")
            .alias("age")
        )

    # --------------------------
    # User bins (silver.user_profile → silver.user_bins)
    # --------------------------
    def upsert_user_bins(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        Type-1 dimension maintenance for user bins (age/gender/city/state).

        MERGE logic
        -----------
        • MATCHED → UPDATE *
        • NOT MATCHED → INSERT *

        Stream details
        --------------
        • Stream directly from a Delta table that changes → use ignoreChanges=True.
        • Stateless stream-to-static join with `users` to ensure referential integrity.
        • No watermark (no time-based state in this transformation).
        """
        from pyspark.sql import functions as F

        query = f"""
            MERGE INTO {self._qt("user_bins")} a
            USING user_bins_delta b
            ON a.user_id = b.user_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "user_bins_delta")

        df_user_ids = spark.table(self._qt("users")).select("user_id")

        df_delta = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreChanges", True)  # streaming from a changing Delta table
            # .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
            .table(self._qt("user_profile"))
            .join(df_user_ids, ["user_id"], "left")
            .select("user_id", self.age_bins(F.col("dob")), "gender", "city", "state")
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", self._ckpt("user_bins"))
            .queryName("user_bins_upsert_stream")
        )

        self._set_pool("silver_p3")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Completed workouts (sessionize start/stop)
    # --------------------------
    def upsert_completed_workouts(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        Build completed workout sessions by pairing start/stop events.

        MERGE logic
        -----------
        • NOT MATCHED → INSERT (*) for unique (user_id, workout_id, session_id).

        Stream details
        --------------
        • Two input streams from `workouts`: start and stop.
        • Watermarks on start_time/end_time (30s).
        • State cleanup: only pair stops that occur within 3 hours of the start
          (prevents unbounded state in the join).
        """
        from pyspark.sql import functions as F

        query = f"""
            MERGE INTO {self._qt("completed_workouts")} a
            USING completed_workouts_delta b
            ON a.user_id = b.user_id
               AND a.workout_id = b.workout_id
               AND a.session_id = b.session_id
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "completed_workouts_delta")

        df_start = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("workouts"))
            .filter("action = 'start'")
            .selectExpr("user_id", "workout_id", "session_id", "time AS start_time")
            .withWatermark("start_time", "30 seconds")
        )

        df_stop = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("workouts"))
            .filter("action = 'stop'")
            .selectExpr("user_id", "workout_id", "session_id", "time AS end_time")
            .withWatermark("end_time", "30 seconds")
        )

        # Bounded-state join: stop must occur within 3 hours of start
        join_cond = [
            df_start.user_id == df_stop.user_id,
            df_start.workout_id == df_stop.workout_id,
            df_start.session_id == df_stop.session_id,
            df_stop.end_time < df_start.start_time + F.expr("INTERVAL 3 HOURS"),
        ]

        df_delta = (
            df_start.join(df_stop, join_cond)
            .select(df_start.user_id, df_start.workout_id, df_start.session_id,
                    df_start.start_time, df_stop.end_time)
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("append")
            .option("checkpointLocation", self._ckpt("completed_workouts"))
            .queryName("completed_workouts_upsert_stream")
        )

        self._set_pool("silver_p1")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Workout BPM (silver.heart_rate × silver.completed_workouts)
    # --------------------------
    def upsert_workout_bpm(self, once: bool = True, processing_time: str = "15 seconds", startingVersion: int = 0):
        """
        Attach valid BPM points to completed workout windows.

        MERGE logic
        -----------
        • NOT MATCHED → INSERT (*) keyed by (user_id, workout_id, session_id, time).

        Stream details
        --------------
        • Stream A: completed_workouts (with device_id via users).
        • Stream B: heart_rate (valid only).
        • Watermarks on end_time (A) and time (B) at 30s.
        • State cleanup: workout must end within 3 hours of the BPM record
          (bounds join state in both directions).
        """
        from pyspark.sql import functions as F

        query = f"""
            MERGE INTO {self._qt("workout_bpm")} a
            USING workout_bpm_delta b
            ON a.user_id = b.user_id
               AND a.workout_id = b.workout_id
               AND a.session_id = b.session_id
               AND a.time = b.time
            WHEN NOT MATCHED THEN INSERT *
        """
        upserter = Upserter(query, "workout_bpm_delta")

        # Ensure qualified users table
        df_users = spark.read.table(self._qt("users"))

        df_completed = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("completed_workouts"))
            .join(df_users, "user_id")
            .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
            .withWatermark("end_time", "30 seconds")
        )

        df_bpm = (
            spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table(self._qt("heart_rate"))
            .filter("valid = TRUE")
            .selectExpr("device_id", "time", "heartrate")
            .withWatermark("time", "30 seconds")
        )

        join_cond = [
            df_completed.device_id == df_bpm.device_id,
            df_bpm.time > df_completed.start_time,
            df_bpm.time <= df_completed.end_time,
            df_completed.end_time < df_bpm.time + F.expr("INTERVAL 3 HOURS"),
        ]

        df_delta = (
            df_bpm.join(df_completed, join_cond)
            .select("user_id", "workout_id", "session_id", "start_time", "end_time", "time", "heartrate")
        )

        writer = (
            df_delta.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("append")
            .option("checkpointLocation", self._ckpt("workout_bpm"))
            .queryName("workout_bpm_upsert_stream")
        )

        self._set_pool("silver_p2")
        return writer.trigger(availableNow=True).start() if once else writer.trigger(processingTime=processing_time).start()

    # --------------------------
    # Orchestration + validation
    # --------------------------
    @staticmethod
    def _await_queries(once: bool) -> None:
        """Wait for availableNow queries to finish (no-op for continuous mode)."""
        if once:
            for q in spark.streams.active:
                q.awaitTermination()

    def upsert(self, once: bool = True, processing_time: str = "5 seconds") -> None:
        """
        Launch Silver streams in dependency-friendly waves and optionally await completion.

        Waves
        -----
        1) users, gym_logs, user_profile, workouts, heart_rate
        2) user_bins, completed_workouts
        3) workout_bpm
        """
        import time
        start = int(time.time())
        print("\nExecuting silver layer upsert ...")

        # Wave 1: base curated tables
        self.upsert_users(once, processing_time)
        self.upsert_gym_logs(once, processing_time)
        self.upsert_user_profile(once, processing_time)
        self.upsert_workouts(once, processing_time)
        self.upsert_heart_rate(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer wave 1 in {int(time.time()) - start} seconds")

        # Wave 2: dimensions & sessionization depend on wave 1
        self.upsert_user_bins(once, processing_time)
        self.upsert_completed_workouts(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer wave 2 in {int(time.time()) - start} seconds")

        # Wave 3: BPM joined to completed sessions
        self.upsert_workout_bpm(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer wave 3 in {int(time.time()) - start} seconds")

    def assert_count(self, table_name: str, expected_count: int, filter_expr: str = "true") -> None:
        """
        Assert that a table has exactly `expected_count` rows after applying `filter_expr`.
        """
        print(f"Validating record counts in {table_name}...", end="")
        actual = spark.read.table(self._qt(table_name)).where(filter_expr).count()
        assert actual == expected_count, (
            f"Expected {expected_count:,} records, found {actual:,} in {table_name} WHERE {filter_expr}"
        )
        print(f"Found {actual:,} / Expected {expected_count:,} WHERE {filter_expr}: Success")

    def validate(self, sets: int) -> None:
        """
        Validate Silver counts against deterministic test inputs (set-scaled).

        Parameters
        ----------
        sets : int
            Number of produced input sets (1 or 2 in your tests).
        """
        import time
        start = int(time.time())
        print("\nValidating silver layer records...")

        self.assert_count("users", 5 if sets == 1 else 10)
        self.assert_count("gym_logs", 8 if sets == 1 else 16)
        self.assert_count("user_profile", 5 if sets == 1 else 10)
        self.assert_count("workouts", 16 if sets == 1 else 32)
        self.assert_count("heart_rate", sets * 253_801)
        self.assert_count("user_bins", 5 if sets == 1 else 10)
        self.assert_count("completed_workouts", 8 if sets == 1 else 16)
        self.assert_count("workout_bpm", 3_968 if sets == 1 else 8_192)

        print(f"Silver layer validation completed in {int(time.time()) - start} seconds")



# class Upserter:
#     def __init__(self, merge_query, temp_view_name):
#         self.merge_query = merge_query
#         self.temp_view_name = temp_view_name 
        
#     def upsert(self, df_micro_batch, batch_id):
#         df_micro_batch.createOrReplaceTempView(self.temp_view_name)
#         df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# # COMMAND ----------

# class CDCUpserter:
#     def __init__(self, merge_query, temp_view_name, id_column, sort_by):
#         self.merge_query = merge_query
#         self.temp_view_name = temp_view_name 
#         self.id_column = id_column
#         self.sort_by = sort_by 
        
#     def upsert(self, df_micro_batch, batch_id):
#         from pyspark.sql.window import Window
#         from pyspark.sql import functions as F
        
#         window = Window.partitionBy(self.id_column).orderBy(F.col(self.sort_by).desc())
        
#         df_micro_batch.filter(F.col("update_type").isin(["new", "update"])) \
#                 .withColumn("rank", F.rank().over(window)).filter("rank == 1").drop("rank") \
#                 .createOrReplaceTempView(self.temp_view_name)
#         df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# # COMMAND ----------

# class Silver():
#     def __init__(self, env):
#         self.Conf = Config() 
#         self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
#         self.catalog = env
#         self.db_name = self.Conf.db_name
#         self.maxFilesPerTrigger = self.Conf.maxFilesPerTrigger
#         spark.sql(f"USE {self.catalog}.{self.db_name}")
        
#     def upsert_users(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
        
#         #Idempotent - User cannot register again so ignore the duplicates and insert the new records
#         query = f"""
#             MERGE INTO {self.catalog}.{self.db_name}.users a
#             USING users_delta b
#             ON a.user_id=b.user_id
#             WHEN NOT MATCHED THEN INSERT *
#             """
        
#         data_upserter=Upserter(query, "users_delta")
        
#         # Spark Structured Streaming accepts append only sources. 
#         #      - This is not a problem for silver layer streams because bronze layer is insert only
#         #      - However, you may want to allow bronze layer deletes due to regulatory compliance 
#         # Spark Structured Streaming throws an exception if any modifications occur on the table being used as a source
#         #      - This is a problem for silver layer streaming jobs.
#         #      - ignoreDeletes allows to delete records on partition column in the bronze layer without exception on silver layer streams 
#         # Starting version is to allow you to restart your stream from a given version just in case you need it
#         #      - startingVersion is only applied for an empty checkpoint
#         # Limiting your input stream size is critical for running on limited capacity
#         #      - maxFilesPerTrigger/maxBytesPerTrigger can be used with the readStream
#         #      - Default value is 1000 for maxFilesPerTrigger and maxBytesPerTrigger has no default value
#         #      - The recomended maxFilesPerTrigger is equal to #executors assuming auto optimize file size to 128 MB
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.registered_users_bz")
#                          .selectExpr("user_id", "device_id", "mac_address", "cast(registration_timestamp as timestamp)")
#                          .withWatermark("registration_timestamp", "30 seconds")
#                          .dropDuplicates(["user_id", "device_id"])
#                    )
        
#         # We read new records in bronze layer and insert them to silver. So, the silver layer is insert only in a typical case. 
#         # However, we want to ignoreDeletes, remove duplicates and also merge with an update statement depending upon the scenarios
#         # Hence, it is recomended to se the update mode
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("update")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/users")
#                                  .queryName("users_upsert_stream")
#                         )

#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
        
          
#     def upsert_gym_logs(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
        
#         #Idempotent - Insert new login records 
#         #           - Update logout time when 
#         #                   1. It is greater than login time
#         #                   2. It is greater than earlier logout
#         #                   3. It is not NULL (This is also satisfied by above conditions)
#         query = f"""
#             MERGE INTO {self.catalog}.{self.db_name}.gym_logs a
#             USING gym_logs_delta b
#             ON a.mac_address=b.mac_address AND a.gym=b.gym AND a.login=b.login
#             WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
#               THEN UPDATE SET logout = b.logout
#             WHEN NOT MATCHED THEN INSERT *
#             """
        
#         data_upserter=Upserter(query, "gym_logs_delta")
        
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.gym_logins_bz")
#                          .selectExpr("mac_address", "gym", "cast(login as timestamp)", "cast(logout as timestamp)")
#                          .withWatermark("login", "30 seconds")
#                          .dropDuplicates(["mac_address", "gym", "login"])
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("update")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/gym_logs")
#                                  .queryName("gym_logs_upsert_stream")
#                         )

#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
        
    
#     def upsert_user_profile(self, once=False, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F

#         #Idempotent - Insert new record
#         #           - Ignore deletes
#         #           - Update user details when
#         #               1. update_type in ("new", "append")
#         #               2. current update is newer than the earlier
#         schema = """
#             user_id bigint, update_type STRING, timestamp FLOAT, 
#             dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, 
#             address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
#             """
        
#         query = f"""
#             MERGE INTO {self.catalog}.{self.db_name}.user_profile a
#             USING user_profile_cdc b
#             ON a.user_id=b.user_id
#             WHEN MATCHED AND a.updated < b.updated
#               THEN UPDATE SET *
#             WHEN NOT MATCHED
#               THEN INSERT *
#             """
        
#         data_upserter = CDCUpserter(query, "user_profile_cdc", "user_id", "updated")
        
#         df_cdc = (spark.readStream
#                        .option("startingVersion", startingVersion)
#                        .option("ignoreDeletes", True)
#                        #.option("withEventTimeOrder", "true")
#                        #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                        .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
#                        .filter("topic = 'user_info'")
#                        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
#                        .select("v.*")
#                        .select("user_id", F.to_date('dob','MM/dd/yyyy').alias('dob'),
#                                'sex', 'gender','first_name','last_name', 'address.*',
#                                F.col('timestamp').cast("timestamp").alias("updated"),
#                                "update_type")
#                        .withWatermark("updated", "30 seconds")
#                        .dropDuplicates(["user_id", "updated"])
#                  )
    
#         stream_writer = (df_cdc.writeStream
#                                .foreachBatch(data_upserter.upsert) 
#                                .outputMode("update") 
#                                .option("checkpointLocation", f"{self.checkpoint_base}/user_profile") 
#                                .queryName("user_profile_stream")
#                         )
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
        
    
#     def upsert_workouts(self, once=False, processing_time="10 seconds", startingVersion=0):
#         from pyspark.sql import functions as F        
#         schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"
        
#         #Idempotent - User cannot have two workout sessions at the same time. So ignore the duplicates and insert the new records
#         query = f"""
#             MERGE INTO {self.catalog}.{self.db_name}.workouts a
#             USING workouts_delta b
#             ON a.user_id=b.user_id AND a.time=b.time
#             WHEN NOT MATCHED THEN INSERT *
#             """

#         data_upserter=Upserter(query, "workouts_delta")
        
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
#                          .filter("topic = 'workout'")
#                          .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
#                          .select("v.*")
#                          .select("user_id", "workout_id", 
#                                  F.col("timestamp").cast("timestamp").alias("time"), 
#                                  "action", "session_id")
#                          .withWatermark("time", "30 seconds")
#                          .dropDuplicates(["user_id", "time"])
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("update")
#                                  .option("checkpointLocation",f"{self.checkpoint_base}/workouts")
#                                  .queryName("workouts_upsert_stream")
#                         )
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
        
        
#     def upsert_heart_rate(self, once=False, processing_time="10 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
        
#         schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
        
#         #Idempotent - Only one BPM signal is allowed at a timestamp. So ignore the duplicates and insert the new records
#         query = f"""
#         MERGE INTO {self.catalog}.{self.db_name}.heart_rate a
#         USING heart_rate_delta b
#         ON a.device_id=b.device_id AND a.time=b.time
#         WHEN NOT MATCHED THEN INSERT *
#         """
        
#         data_upserter=Upserter(query, "heart_rate_delta")
    
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
#                          .filter("topic = 'bpm'")
#                          .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
#                          .select("v.*", F.when(F.col("v.heartrate") <= 0, False).otherwise(True).alias("valid"))
#                          .withWatermark("time", "30 seconds")
#                          .dropDuplicates(["device_id", "time"])
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("update")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/heart_rate")
#                                  .queryName("heart_rate_upsert_stream")
#                         )

#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
          
    
#     def age_bins(self, dob_col):
#         from pyspark.sql import functions as F
#         age_col = F.floor(F.months_between(F.current_date(), dob_col)/12).alias("age")
#         return (F.when((age_col < 18), "under 18")
#                 .when((age_col >= 18) & (age_col < 25), "18-25")
#                 .when((age_col >= 25) & (age_col < 35), "25-35")
#                 .when((age_col >= 35) & (age_col < 45), "35-45")
#                 .when((age_col >= 45) & (age_col < 55), "45-55")
#                 .when((age_col >= 55) & (age_col < 65), "55-65")
#                 .when((age_col >= 65) & (age_col < 75), "65-75")
#                 .when((age_col >= 75) & (age_col < 85), "75-85")
#                 .when((age_col >= 85) & (age_col < 95), "85-95")
#                 .when((age_col >= 95), "95+")
#                 .otherwise("invalid age").alias("age"))
        
    
#     def upsert_user_bins(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
        
#         # Idempotent - This table is maintained as SCD Type 1 dimension
#         #            - Insert new user_id records 
#         #            - Update old records using the user_id

#         query = f"""
#             MERGE INTO {self.catalog}.{self.db_name}.user_bins a
#             USING user_bins_delta b
#             ON a.user_id=b.user_id
#             WHEN MATCHED 
#               THEN UPDATE SET *
#             WHEN NOT MATCHED THEN INSERT *
#             """
        
#         data_upserter=Upserter(query, "user_bins_delta")
        
#         df_user = spark.table(f"{self.catalog}.{self.db_name}.users").select("user_id")
        
#         # Running stream on silver table requires ignoreChanges
#         # No watermark required - Stream to staic join is stateless
#         df_delta = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreChanges", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.user_profile")
#                          .join(df_user, ["user_id"], "left")
#                          .select("user_id", self.age_bins(F.col("dob")),"gender", "city", "state")
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("update")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/user_bins")
#                                  .queryName("user_bins_upsert_stream")
#                         )
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p3")

#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
        
                  
            
#     def upsert_completed_workouts(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
              
#         #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
#         query = f"""
#         MERGE INTO {self.catalog}.{self.db_name}.completed_workouts a
#         USING completed_workouts_delta b
#         ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
#         WHEN NOT MATCHED THEN INSERT *
#         """
        
#         data_upserter=Upserter(query, "completed_workouts_delta")
    
#         df_start = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.workouts")
#                          .filter("action = 'start'")                         
#                          .selectExpr("user_id", "workout_id", "session_id", "time as start_time")
#                          .withWatermark("start_time", "30 seconds")
#                          #.dropDuplicates(["user_id", "workout_id", "session_id", "start_time"])
#                    )
        
#         df_stop = (spark.readStream
#                          .option("startingVersion", startingVersion)
#                          .option("ignoreDeletes", True)
#                          #.option("withEventTimeOrder", "true")
#                          #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                          .table(f"{self.catalog}.{self.db_name}.workouts")
#                          .filter("action = 'stop'")                         
#                          .selectExpr("user_id", "workout_id", "session_id", "time as end_time")
#                          .withWatermark("end_time", "30 seconds")
#                          #.dropDuplicates(["user_id", "workout_id", "session_id", "end_time"])
#                    )
        
#         # State cleanup - Define a condition to clean the state
#         #               - stop must occur within 3 hours of start 
#         #               - stop < start + 3 hours
#         join_condition = [df_start.user_id == df_stop.user_id, df_start.workout_id==df_stop.workout_id, df_start.session_id==df_stop.session_id, 
#                           df_stop.end_time < df_start.start_time + F.expr('interval 3 hour')]         
        
#         df_delta = (df_start.join(df_stop, join_condition)
#                             .select(df_start.user_id, df_start.workout_id, df_start.session_id, df_start.start_time, df_stop.end_time)
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("append")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/completed_workouts")
#                                  .queryName("completed_workouts_upsert_stream")
#                         )

#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p1")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
    
    
    
#     def upsert_workout_bpm(self, once=True, processing_time="15 seconds", startingVersion=0):
#         from pyspark.sql import functions as F
              
#         #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
#         query = f"""
#         MERGE INTO {self.catalog}.{self.db_name}.workout_bpm a
#         USING workout_bpm_delta b
#         ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id AND a.time=b.time
#         WHEN NOT MATCHED THEN INSERT *
#         """
        
#         data_upserter=Upserter(query, "workout_bpm_delta")        
        
#         df_users = spark.read.table("users")
        
#         df_completed_workouts = (spark.readStream
#                                       .option("startingVersion", startingVersion)
#                                       .option("ignoreDeletes", True)
#                                       #.option("withEventTimeOrder", "true")
#                                       #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                                       .table(f"{self.catalog}.{self.db_name}.completed_workouts")
#                                       .join(df_users, "user_id")
#                                       .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
#                                       .withWatermark("end_time", "30 seconds")
#                                  )
        
#         df_bpm = (spark.readStream
#                        .option("startingVersion", startingVersion)
#                        .option("ignoreDeletes", True)
#                        #.option("withEventTimeOrder", "true")
#                        #.option("maxFilesPerTrigger", self.maxFilesPerTrigger)
#                        .table(f"{self.catalog}.{self.db_name}.heart_rate")
#                        .filter("valid = True")                         
#                        .selectExpr("device_id", "time", "heartrate")
#                        .withWatermark("time", "30 seconds")
#                    )
        
#         # State cleanup - Define a condition to clean the state
#         #               - Workout could be a maximum of three hours
#         #               - workout must end within 3 hours of bpm 
#         #               - workout.end < bpm.time + 3 hours
#         join_condition = [df_completed_workouts.device_id == df_bpm.device_id, 
#                           df_bpm.time > df_completed_workouts.start_time, df_bpm.time <= df_completed_workouts.end_time,
#                           df_completed_workouts.end_time < df_bpm.time + F.expr('interval 3 hour')] 
        
#         df_delta = (df_bpm.join(df_completed_workouts, join_condition)
#                           .select("user_id", "workout_id","session_id", "start_time", "end_time", "time", "heartrate")
#                    )
        
#         stream_writer = (df_delta.writeStream
#                                  .foreachBatch(data_upserter.upsert)
#                                  .outputMode("append")
#                                  .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm")
#                                  .queryName("workout_bpm_upsert_stream")
#                         )

#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).start()
#         else:
#             return stream_writer.trigger(processingTime=processing_time).start()
       
#     def _await_queries(self, once):
#         if once:
#             for stream in spark.streams.active:
#                 stream.awaitTermination()
                
#     def upsert(self, once=True, processing_time="5 seconds"):
#         import time
#         start = int(time.time())
#         print(f"\nExecuting silver layer upsert ...")
#         self.upsert_users(once, processing_time)
#         self.upsert_gym_logs(once, processing_time)
#         self.upsert_user_profile(once, processing_time)
#         self.upsert_workouts(once, processing_time)
#         self.upsert_heart_rate(once, processing_time)        
#         self._await_queries(once)
#         print(f"Completed silver layer 1 upsert {int(time.time()) - start} seconds")
#         self.upsert_user_bins(once, processing_time)
#         self.upsert_completed_workouts(once, processing_time)        
#         self._await_queries(once)
#         print(f"Completed silver layer 2 upsert {int(time.time()) - start} seconds")
#         self.upsert_workout_bpm(once, processing_time)
#         self._await_queries(once)
#         print(f"Completed silver layer 3 upsert {int(time.time()) - start} seconds")
        
        
#     def assert_count(self, table_name, expected_count, filter="true"):
#         print(f"Validating record counts in {table_name}...", end='')
#         actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
#         assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
#         print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
#     def validate(self, sets):
#         import time
#         start = int(time.time())
#         print(f"\nValidating silver layer records...")
#         self.assert_count("users", 5 if sets == 1 else 10)
#         self.assert_count("gym_logs", 8 if sets == 1 else 16)
#         self.assert_count("user_profile", 5 if sets == 1 else 10)
#         self.assert_count("workouts", 16 if sets == 1 else 32)
#         self.assert_count("heart_rate", sets * 253801)
#         self.assert_count("user_bins", 5 if sets == 1 else 10)
#         self.assert_count("completed_workouts", 8 if sets == 1 else 16)
#         self.assert_count("workout_bpm", 3968 if sets == 1 else 8192)
#         print(f"Silver layer validation completed in {int(time.time()) - start} seconds")     
