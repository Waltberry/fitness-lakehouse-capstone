# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

from typing import Optional


class Bronze:
    """
    Bronze (raw ingest) layer.

    Responsibilities
    ----------------
    • Ingest raw files from the landing zone using Databricks Auto Loader (cloudFiles).
    • Append to Delta bronze tables with minimal transformations.
    • Add ingestion metadata (load_time, source_file).
    • Maintain checkpoints for exactly-once semantics.
    • Provide simple validation helpers for test harnesses.

    Tables written
    --------------
    • registered_users_bz     (CSV)
    • gym_logins_bz           (CSV)
    • kafka_multiplex_bz      (JSON; partitioned later in DDL; enriched with date/week_part)

    Typical usage
    -------------
    bz = Bronze(env="dev")
    bz.consume(once=True)     # batch catch-up using availableNow
    # or
    bz.consume(once=False, processing_time="5 seconds")  # continuous micro-batches
    """

    # --------------------------
    # Init & utilities
    # --------------------------
    def __init__(self, env: str, max_files_per_trigger: Optional[int] = None) -> None:
        """
        Initialize I/O paths and database context.

        Parameters
        ----------
        env : str
            Unity Catalog catalog name ("dev", "test", "prod", ...).
        max_files_per_trigger : Optional[int]
            Optional override for Auto Loader's max files per trigger. If None,
            falls back to Config().maxFilesPerTrigger.
        """
        self.Conf = Config()
        self.landing_zone: str = f"{self.Conf.base_dir_data}/raw"
        self.checkpoint_base: str = f"{self.Conf.base_dir_checkpoint}/checkpoints"
        self.catalog: str = env
        self.db_name: str = self.Conf.db_name
        self.max_files_per_trigger: int = (
            int(max_files_per_trigger)
            if max_files_per_trigger is not None
            else int(self.Conf.maxFilesPerTrigger)
        )

        # Ensure the default DB context is set for this session
        spark.sql(f"USE {self.catalog}.{self.db_name}")

    def _qt(self, table: str) -> str:
        """Fully-qualified table name."""
        return f"{self.catalog}.{self.db_name}.{table}"

    def _checkpoint(self, leaf: str) -> str:
        """Checkpoint directory per stream."""
        return f"{self.checkpoint_base}/{leaf}"

    # --------------------------
    # User registration (CSV → bronze)
    # --------------------------
    def consume_user_registration(self, once: bool = True, processing_time: str = "5 seconds"):
        """
        Ingest CSV user-registration files into bronze.registered_users_bz.

        Notes
        -----
        • Auto Loader (cloudFiles) watches `<landing>/registered_users_bz/`.
        • Schema is provided (no schema inference).
        • Append-only write (bronze principle).
        • Checkpointing enables exactly-once guarantees across restarts.
        • availableNow (once=True) runs a bounded catch-up batch until there is no backlog.
          Otherwise, use processingTime to run continuous micro-batches.
        """
        from pyspark.sql import functions as F

        # Keep schema minimal: raw types from the CSV source
        schema = "user_id LONG, device_id LONG, mac_address STRING, registration_timestamp DOUBLE"

        df_stream = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("maxFilesPerTrigger", 1)  # test-friendly; you can bump to self.max_files_per_trigger
            .load(f"{self.landing_zone}/registered_users_bz")
            .withColumn("load_time", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        # Bronze is append-only: no upserts here
        writer = (
            df_stream.writeStream.format("delta")
            .option("checkpointLocation", self._checkpoint("registered_users_bz"))
            .outputMode("append")
            .queryName("registered_users_bz_ingestion_stream")
        )

        # Optional: place this stream in a scheduler pool (for multi-stream QoS)
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")

        if once:
            return writer.trigger(availableNow=True).toTable(self._qt("registered_users_bz"))
        else:
            return writer.trigger(processingTime=processing_time).toTable(self._qt("registered_users_bz"))

    # --------------------------
    # Gym logins (CSV → bronze)
    # --------------------------
    def consume_gym_logins(self, once: bool = True, processing_time: str = "5 seconds"):
        """
        Ingest CSV gym login/logout files into bronze.gym_logins_bz.

        Notes
        -----
        • Source timestamps arrive as DOUBLE epoch seconds; bronze stores raw values.
        • Minimal metadata added; type fixes happen in SILVER.
        """
        from pyspark.sql import functions as F

        schema = "mac_address STRING, gym BIGINT, login DOUBLE, logout DOUBLE"

        df_stream = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("maxFilesPerTrigger", 1)
            .load(f"{self.landing_zone}/gym_logins_bz")
            .withColumn("load_time", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        writer = (
            df_stream.writeStream.format("delta")
            .option("checkpointLocation", self._checkpoint("gym_logins_bz"))
            .outputMode("append")
            .queryName("gym_logins_bz_ingestion_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")

        if once:
            return writer.trigger(availableNow=True).toTable(self._qt("gym_logins_bz"))
        else:
            return writer.trigger(processingTime=processing_time).toTable(self._qt("gym_logins_bz"))

    # --------------------------
    # Kafka multiplex (JSON → bronze)
    # --------------------------
    def consume_kafka_multiplex(self, once: bool = True, processing_time: str = "5 seconds"):
        """
        Ingest JSON events (user_info, workout, bpm) into bronze.kafka_multiplex_bz and
        enrich with calendar fields (date, week_part) via a broadcast join to `date_lookup`.

        Notes
        -----
        • Source files live under `<landing>/kafka_multiplex_bz/`.
        • We keep raw Kafka-like envelope: key, value, topic, partition, offset, timestamp.
        • `date` & `week_part` are stamped for efficient downstream partition/filtering.
        • Using a BROADCAST join ensures a small dimension doesn't cause skew.
        """
        from pyspark.sql import functions as F

        schema = "key STRING, value STRING, topic STRING, partition BIGINT, offset BIGINT, timestamp BIGINT"

        # Small dimension table → perfect for broadcast
        df_date_lookup = spark.table(self._qt("date_lookup")).select("date", "week_part")

        df_stream = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", "json")
            .option("maxFilesPerTrigger", 1)
            .load(f"{self.landing_zone}/kafka_multiplex_bz")
            .withColumn("load_time", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
            # Map event timestamp (ms) → date for joining to the dimension
            .join(
                F.broadcast(df_date_lookup),
                on=[
                    F.to_date((F.col("timestamp") / F.lit(1000)).cast("timestamp")) == F.col("date")
                ],
                how="left",
            )
        )

        writer = (
            df_stream.writeStream.format("delta")
            .option("checkpointLocation", self._checkpoint("kafka_multiplex_bz"))
            .outputMode("append")
            .queryName("kafka_multiplex_bz_ingestion_stream")
        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p1")

        if once:
            return writer.trigger(availableNow=True).toTable(self._qt("kafka_multiplex_bz"))
        else:
            return writer.trigger(processingTime=processing_time).toTable(self._qt("kafka_multiplex_bz"))

    # --------------------------
    # Orchestration
    # --------------------------
    def consume(self, once: bool = True, processing_time: str = "5 seconds") -> None:
        """
        Kick off all bronze ingesters (user_registration, gym_logins, kafka_multiplex).

        Parameters
        ----------
        once : bool
            If True, runs each stream in `availableNow` mode (bounded catch-up).
            If False, runs continuous micro-batches using `processing_time`.
        processing_time : str
            Trigger interval when `once=False` (e.g., "5 seconds").
        """
        import time

        start = int(time.time())
        print("\nStarting bronze layer consumption ...")
        self.consume_user_registration(once, processing_time)
        self.consume_gym_logins(once, processing_time)
        self.consume_kafka_multiplex(once, processing_time)

        # In availableNow mode, the three queries finish on their own.
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()

        elapsed = int(time.time()) - start
        print(f"Completed bronze layer consumption in {elapsed} seconds")

    # --------------------------
    # Validation helpers
    # --------------------------
    def assert_count(self, table_name: str, expected_count: int, filter_expr: str = "true") -> None:
        """
        Assert that `<table_name>` has `expected_count` rows subject to `filter_expr`.

        Parameters
        ----------
        table_name : str
            Unqualified table name (e.g., "kafka_multiplex_bz").
        expected_count : int
            Expected exact row count.
        filter_expr : str
            SQL boolean expression to filter rows (e.g., "topic='bpm'").
        """
        print(f"Validating record counts in {table_name}...", end="")
        actual_count = spark.read.table(self._qt(table_name)).where(filter_expr).count()
        assert actual_count == expected_count, (
            f"Expected {expected_count:,} records, found {actual_count:,} "
            f"in {table_name} WHERE {filter_expr}"
        )
        print(f"Found {actual_count:,} / Expected {expected_count:,} WHERE {filter_expr}: Success")

    def validate(self, sets: int) -> None:
        """
        Validate bronze counts against deterministic test inputs.

        Parameters
        ----------
        sets : int
            Number of produced sets (1 or 2 in your tests). Counts scale linearly
            except for topic='bpm' which is large (253,801 per set).
        """
        import time

        start = int(time.time())
        print("\nValidating bronze layer records...")

        # CSVs
        self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
        self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)

        # JSON multiplex by topic
        self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
        self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
        self.assert_count("kafka_multiplex_bz", sets * 253_801, "topic='bpm'")

        elapsed = int(time.time()) - start
        print(f"Bronze layer validation completed in {elapsed} seconds")


# class Bronze():
#     def __init__(self, env):        
#         self.Conf = Config()
#         self.landing_zone = self.Conf.base_dir_data + "/raw" 
#         self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
#         self.catalog = env
#         self.db_name = self.Conf.db_name
#         spark.sql(f"USE {self.catalog}.{self.db_name}")
        
#     def consume_user_registration(self, once=True, processing_time="5 seconds"):
#         from pyspark.sql import functions as F
#         schema = "user_id long, device_id long, mac_address string, registration_timestamp double"
        
#         df_stream = (spark.readStream
#                         .format("cloudFiles")
#                         .schema(schema)
#                         .option("maxFilesPerTrigger", 1)
#                         .option("cloudFiles.format", "csv")
#                         .option("header", "true")
#                         .load(self.landing_zone + "/registered_users_bz")
#                         .withColumn("load_time", F.current_timestamp()) 
#                         .withColumn("source_file", F.input_file_name())
#                     )
                        
#         # Use append mode because bronze layer is expected to insert only from source
#         stream_writer = df_stream.writeStream \
#                                  .format("delta") \
#                                  .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz") \
#                                  .outputMode("append") \
#                                  .queryName("registered_users_bz_ingestion_stream")
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
#         else:
#             return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
          
#     def consume_gym_logins(self, once=True, processing_time="5 seconds"):
#         from pyspark.sql import functions as F
#         schema = "mac_address string, gym bigint, login double, logout double"
        
#         df_stream = (spark.readStream 
#                         .format("cloudFiles") 
#                         .schema(schema) 
#                         .option("maxFilesPerTrigger", 1) 
#                         .option("cloudFiles.format", "csv") 
#                         .option("header", "true") 
#                         .load(self.landing_zone + "/gym_logins_bz") 
#                         .withColumn("load_time", F.current_timestamp())
#                         .withColumn("source_file", F.input_file_name())
#                     )
        
#         # Use append mode because bronze layer is expected to insert only from source
#         stream_writer = df_stream.writeStream \
#                                  .format("delta") \
#                                  .option("checkpointLocation", self.checkpoint_base + "/gym_logins_bz") \
#                                  .outputMode("append") \
#                                  .queryName("gym_logins_bz_ingestion_stream")
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
            
#         if once == True:
#             return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
#         else:
#             return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        
        
#     def consume_kafka_multiplex(self, once=True, processing_time="5 seconds"):
#         from pyspark.sql import functions as F
#         schema = "key string, value string, topic string, partition bigint, offset bigint, timestamp bigint"
#         df_date_lookup = spark.table(f"{self.catalog}.{self.db_name}.date_lookup").select("date", "week_part")
        
#         df_stream = (spark.readStream
#                         .format("cloudFiles")
#                         .schema(schema)
#                         .option("maxFilesPerTrigger", 1)
#                         .option("cloudFiles.format", "json")
#                         .load(self.landing_zone + "/kafka_multiplex_bz")                        
#                         .withColumn("load_time", F.current_timestamp())       
#                         .withColumn("source_file", F.input_file_name())
#                         .join(F.broadcast(df_date_lookup), 
#                               [F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date")], 
#                               "left")
#                     )
        
#         # Use append mode because bronze layer is expected to insert only from source
#         stream_writer = df_stream.writeStream \
#                                  .format("delta") \
#                                  .option("checkpointLocation", self.checkpoint_base + "/kafka_multiplex_bz") \
#                                  .outputMode("append") \
#                                  .queryName("kafka_multiplex_bz_ingestion_stream")
        
#         spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p1")
        
#         if once == True:
#             return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
#         else:
#             return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.kafka_multiplex_bz")
        
            
#     def consume(self, once=True, processing_time="5 seconds"):
#         import time
#         start = int(time.time())
#         print(f"\nStarting bronze layer consumption ...")
#         self.consume_user_registration(once, processing_time) 
#         self.consume_gym_logins(once, processing_time) 
#         self.consume_kafka_multiplex(once, processing_time)
#         if once:
#             for stream in spark.streams.active:
#                 stream.awaitTermination()
#         print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")
        
        
#     def assert_count(self, table_name, expected_count, filter="true"):
#         print(f"Validating record counts in {table_name}...", end='')
#         actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
#         assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
#         print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success")        
        
#     def validate(self, sets):
#         import time
#         start = int(time.time())
#         print(f"\nValidating bronz layer records...")
#         self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
#         self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)
#         self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
#         self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
#         self.assert_count("kafka_multiplex_bz", sets * 253801, "topic='bpm'")
#         print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")                
