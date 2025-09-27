# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

# from typing import Optional


class SetupHelper:
    """
    Bootstrap helper for the medallion lakehouse environment.

    Responsibilities
    ----------------
    • Create (or reuse) the target database in the given Unity Catalog catalog (env).
    • Create all BRONZE / SILVER / GOLD Delta tables and the `gym_summary` VIEW.
    • Validate that all required objects exist.
    • Cleanup the environment (DROP DATABASE CASCADE + remove landing/checkpoint dirs).

    Typical usage
    -------------
    sh = SetupHelper(env="dev")
    sh.setup()       # create DB + all tables/views
    sh.validate()    # assert everything is present
    sh.cleanup()     # (optionally) tear down DB + files
    """

    def __init__(self, env: str) -> None:
        """
        Initialize the helper with the target catalog (environment).

        Parameters
        ----------
        env : str
            The Unity Catalog catalog name to use (e.g., "dev", "test", "prod").
        """
        conf = Config()
        self.landing_zone: str = f"{conf.base_dir_data}/raw"
        self.checkpoint_base: str = f"{conf.base_dir_checkpoint}/checkpoints"
        self.catalog: str = env
        self.db_name: str = conf.db_name
        self.initialized: bool = False

    # --------------------------
    # Internal helpers
    # --------------------------
    def _ensure_initialized(self) -> None:
        """Raise a clear error if create_db() has not been called yet."""
        if not self.initialized:
            raise ReferenceError(
                "Application database is not defined. Call create_db() first."
            )

    def _exec(self, sql: str) -> None:
        """Execute a SQL statement (thin wrapper for consistency)."""
        spark.sql(sql)

    # --------------------------
    # Database
    # --------------------------
    def create_db(self) -> None:
        """Create and USE the application database inside the selected catalog."""
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{self.db_name}...", end="")
        self._exec(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        self._exec(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Done")

    # --------------------------
    # BRONZE tables (append-only)
    # --------------------------
    def create_registered_users_bz(self) -> None:
        """Create the bronze table for user registrations (raw CSV → append-only)."""
        self._ensure_initialized()
        print("Creating registered_users_bz table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                user_id BIGINT,
                device_id BIGINT,
                mac_address STRING,
                registration_timestamp DOUBLE,
                load_time TIMESTAMP,
                source_file STRING
            )
            """
        )
        print("Done")

    def create_gym_logins_bz(self) -> None:
        """Create the bronze table for gym login/logout events (raw CSV → append-only)."""
        self._ensure_initialized()
        print("Creating gym_logins_bz table...", end="")
        # Using OR REPLACE here matches your original intent to reset the schema if needed.
        self._exec(
            f"""
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                mac_address STRING,
                gym BIGINT,
                login DOUBLE,
                logout DOUBLE,
                load_time TIMESTAMP,
                source_file STRING
            )
            """
        )
        print("Done")

    def create_kafka_multiplex_bz(self) -> None:
        """
        Create the bronze multiplex table for JSON 'topics' (user_info, workout, bpm).

        Notes
        -----
        • Partitioned by (topic, week_part) because silver/gold frequently filter by these.
        • `date` and `week_part` are populated during bronze ingestion for efficient partition pruning.
        """
        self._ensure_initialized()
        print("Creating kafka_multiplex_bz table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                key STRING,
                value STRING,
                topic STRING,
                partition BIGINT,
                offset BIGINT,
                timestamp BIGINT,
                date DATE,
                week_part STRING,
                load_time TIMESTAMP,
                source_file STRING
            )
            PARTITIONED BY (topic, week_part)
            """
        )
        print("Done")

    # --------------------------
    # SILVER tables (curated)
    # --------------------------
    def create_users(self) -> None:
        """Create the silver `users` table (idempotent upsert target)."""
        self._ensure_initialized()
        print("Creating users table...", end="")
        self._exec(
            f"""
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                user_id BIGINT,
                device_id BIGINT,
                mac_address STRING,
                registration_timestamp TIMESTAMP
            )
            """
        )
        print("Done")

    def create_gym_logs(self) -> None:
        """Create the silver `gym_logs` table (deduped logins with corrected types)."""
        self._ensure_initialized()
        print("Creating gym_logs table...", end="")
        self._exec(
            f"""
            CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                mac_address STRING,
                gym BIGINT,
                login TIMESTAMP,
                logout TIMESTAMP
            )
            """
        )
        print("Done")

    def create_user_profile(self) -> None:
        """Create the silver `user_profile` table (CDC target; latest row per user)."""
        self._ensure_initialized()
        print("Creating user_profile table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                user_id BIGINT,
                dob DATE,
                sex STRING,
                gender STRING,
                first_name STRING,
                last_name STRING,
                street_address STRING,
                city STRING,
                state STRING,
                zip INT,
                updated TIMESTAMP
            )
            """
        )
        print("Done")

    def create_heart_rate(self) -> None:
        """Create the silver `heart_rate` table (validated BPM time series)."""
        self._ensure_initialized()
        print("Creating heart_rate table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                device_id BIGINT,
                time TIMESTAMP,
                heartrate DOUBLE,
                valid BOOLEAN
            )
            """
        )
        print("Done")

    def create_user_bins(self) -> None:
        """Create the silver `user_bins` table (type-1 dimension: age/gender/city/state)."""
        self._ensure_initialized()
        print("Creating user_bins table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                user_id BIGINT,
                age STRING,
                gender STRING,
                city STRING,
                state STRING
            )
            """
        )
        print("Done")

    def create_workouts(self) -> None:
        """Create the silver `workouts` table (start/stop actions with session_id)."""
        self._ensure_initialized()
        print("Creating workouts table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                user_id INT,
                workout_id INT,
                time TIMESTAMP,
                action STRING,
                session_id INT
            )
            """
        )
        print("Done")

    def create_completed_workouts(self) -> None:
        """
        Create the silver `completed_workouts` table.

        Notes
        -----
        • Derived by matching start/stop with bounded state in streaming (3-hour cap).
        """
        self._ensure_initialized()
        print("Creating completed_workouts table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP
            )
            """
        )
        print("Done")

    def create_workout_bpm(self) -> None:
        """Create the silver `workout_bpm` table (BPM readings within completed sessions)."""
        self._ensure_initialized()
        print("Creating workout_bpm table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                user_id INT,
                workout_id INT,
                session_id INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                time TIMESTAMP,
                heartrate DOUBLE
            )
            """
        )
        print("Done")

    def create_date_lookup(self) -> None:
        """Create the silver `date_lookup` table (supports partitioning and joins)."""
        self._ensure_initialized()
        print("Creating date_lookup table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                date DATE,
                week INT,
                year INT,
                month INT,
                dayofweek INT,
                dayofmonth INT,
                dayofyear INT,
                week_part STRING
            )
            """
        )
        print("Done")

    # --------------------------
    # GOLD (facts & views)
    # --------------------------
    def create_workout_bpm_summary(self) -> None:
        """Create the gold `workout_bpm_summary` table (per-session BPM aggregates)."""
        self._ensure_initialized()
        print("Creating workout_bpm_summary table...", end="")
        self._exec(
            f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                workout_id INT,
                session_id INT,
                user_id BIGINT,
                age STRING,
                gender STRING,
                city STRING,
                state STRING,
                min_bpm DOUBLE,
                avg_bpm DOUBLE,
                max_bpm DOUBLE,
                num_recordings BIGINT
            )
            """
        )
        print("Done")

    def create_gym_summary(self) -> None:
        """
        Create or replace the gold `gym_summary` VIEW.

        What it shows
        -------------
        • Date-level minutes in gym (login→logout) and minutes exercising (start→end),
          per (gym, mac_address, workout session).

        Why cast?
        ---------
        • We cast numeric epoch fields to TIMESTAMP using ANSI CAST for Spark SQL portability.
        """
        self._ensure_initialized()
        print("Creating gym_summary gold view...", end="")
        self._exec(
            f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
            SELECT
              CAST(login AS TIMESTAMP)            AS login_ts,
              CAST(logout AS TIMESTAMP)           AS logout_ts,
              TO_DATE(CAST(login AS TIMESTAMP))   AS date,
              l.gym,
              l.mac_address,
              w.workout_id,
              w.session_id,
              ROUND( (CAST(logout AS BIGINT) - CAST(login AS BIGINT)) / 60.0, 2) AS minutes_in_gym,
              ROUND( (CAST(end_time AS BIGINT) - CAST(start_time AS BIGINT)) / 60.0, 2) AS minutes_exercising
            FROM {self.catalog}.{self.db_name}.gym_logs l
            JOIN (
              SELECT
                u.mac_address,
                w.workout_id,
                w.session_id,
                w.start_time,
                w.end_time
              FROM {self.catalog}.{self.db_name}.completed_workouts w
              INNER JOIN {self.catalog}.{self.db_name}.users u
                ON w.user_id = u.user_id
            ) w
              ON l.mac_address = w.mac_address
             AND w.start_time BETWEEN l.login AND l.logout
            ORDER BY date, l.gym, l.mac_address, w.session_id
            """
        )
        print("Done")

    # --------------------------
    # Orchestration / Validation
    # --------------------------
    def setup(self) -> None:
        """Create the database and all required tables/views (idempotent)."""
        import time

        start = int(time.time())
        print("\nStarting setup ...")

        # DB first, then tables layer-by-layer
        self.create_db()

        # Bronze
        self.create_registered_users_bz()
        self.create_gym_logins_bz()
        self.create_kafka_multiplex_bz()

        # Silver
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()

        # Gold
        self.create_workout_bpm_summary()
        self.create_gym_summary()

        print(f"Setup completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name: str) -> None:
        """Assert a table or view exists in the target database."""
        exists = (
            spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}")
            .filter(f"isTemporary == false and tableName == '{table_name}'")
            .count()
            == 1
        )
        assert exists, f"The table/view {table_name} is missing"
        print(f"Found {table_name} in {self.catalog}.{self.db_name}: Success")

    def validate(self) -> None:
        """Verify the database and all required tables/views exist."""
        import time

        start = int(time.time())
        print("\nStarting setup validation ...")

        # Database exists?
        db_exists = (
            spark.sql(f"SHOW DATABASES IN {self.catalog}")
            .filter(f"databaseName == '{self.db_name}'")
            .count()
            == 1
        )
        assert db_exists, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")

        # Required objects
        self.assert_table("registered_users_bz")
        self.assert_table("gym_logins_bz")
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary")
        self.assert_table("gym_summary")

        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def cleanup(self) -> None:
        """
        Tear down the environment: drop the database (CASCADE) and
        remove the landing and checkpoint directories.

        Warning
        -------
        • This deletes ALL tables in the DB and removes files under the configured
          landing/checkpoint roots. Use carefully in shared environments.
        """
        # Drop DB if present
        if (
            spark.sql(f"SHOW DATABASES IN {self.catalog}")
            .filter(f"databaseName == '{self.db_name}'")
            .count()
            == 1
        ):
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end="")
            self._exec(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")

        # Delete landing and checkpoint directories
        print(f"Deleting {self.landing_zone}...", end="")
        dbutils.fs.rm(self.landing_zone, recurse=True)
        print("Done")

        print(f"Deleting {self.checkpoint_base}...", end="")
        dbutils.fs.rm(self.checkpoint_base, recurse=True)
        print("Done")





# class SetupHelper():   
#     def __init__(self, env):
#         Conf = Config()
#         self.landing_zone = Conf.base_dir_data + "/raw"
#         self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"        
#         self.catalog = env
#         self.db_name = Conf.db_name
#         self.initialized = False
        
#     def create_db(self):
#         spark.catalog.clearCache()
#         print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
#         spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
#         spark.sql(f"USE {self.catalog}.{self.db_name}")
#         self.initialized = True
#         print("Done")
        
#     def create_registered_users_bz(self):
#         if(self.initialized):
#             print(f"Creating registered_users_bz table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
#                     user_id long,
#                     device_id long, 
#                     mac_address string, 
#                     registration_timestamp double,
#                     load_time timestamp,
#                     source_file string                    
#                     )
#                   """) 
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
#     def create_gym_logins_bz(self):
#         if(self.initialized):
#             print(f"Creating gym_logins_bz table...", end='')
#             spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
#                     mac_address string,
#                     gym bigint,
#                     login double,                      
#                     logout double,                    
#                     load_time timestamp,
#                     source_file string
#                     )
#                   """) 
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
#     def create_kafka_multiplex_bz(self):
#         if(self.initialized):
#             print(f"Creating kafka_multiplex_bz table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
#                   key string, 
#                   value string, 
#                   topic string, 
#                   partition bigint, 
#                   offset bigint, 
#                   timestamp bigint,                  
#                   date date, 
#                   week_part string,                  
#                   load_time timestamp,
#                   source_file string)
#                   PARTITIONED BY (topic, week_part)
#                   """) 
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
#     def create_users(self):
#         if(self.initialized):
#             print(f"Creating users table...", end='')
#             spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
#                     user_id bigint, 
#                     device_id bigint, 
#                     mac_address string,
#                     registration_timestamp timestamp
#                     )
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
#     def create_gym_logs(self):
#         if(self.initialized):
#             print(f"Creating gym_logs table...", end='')
#             spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
#                     mac_address string,
#                     gym bigint,
#                     login timestamp,                      
#                     logout timestamp
#                     )
#                   """) 
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
#     def create_user_profile(self):
#         if(self.initialized):
#             print(f"Creating user_profile table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
#                     user_id bigint, 
#                     dob DATE, 
#                     sex STRING, 
#                     gender STRING, 
#                     first_name STRING, 
#                     last_name STRING, 
#                     street_address STRING, 
#                     city STRING, 
#                     state STRING, 
#                     zip INT, 
#                     updated TIMESTAMP)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")

#     def create_heart_rate(self):
#         if(self.initialized):
#             print(f"Creating heart_rate table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
#                     device_id LONG, 
#                     time TIMESTAMP, 
#                     heartrate DOUBLE, 
#                     valid BOOLEAN)
#                   """)
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
#     def create_user_bins(self):
#         if(self.initialized):
#             print(f"Creating user_bins table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
#                     user_id BIGINT, 
#                     age STRING, 
#                     gender STRING, 
#                     city STRING, 
#                     state STRING)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
#     def create_workouts(self):
#         if(self.initialized):
#             print(f"Creating workouts table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
#                     user_id INT, 
#                     workout_id INT, 
#                     time TIMESTAMP, 
#                     action STRING, 
#                     session_id INT)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
#     def create_completed_workouts(self):
#         if(self.initialized):
#             print(f"Creating completed_workouts table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
#                     user_id INT, 
#                     workout_id INT, 
#                     session_id INT, 
#                     start_time TIMESTAMP, 
#                     end_time TIMESTAMP)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
#     def create_workout_bpm(self):
#         if(self.initialized):
#             print(f"Creating workout_bpm table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
#                     user_id INT, 
#                     workout_id INT, 
#                     session_id INT,
#                     start_time TIMESTAMP, 
#                     end_time TIMESTAMP,
#                     time TIMESTAMP, 
#                     heartrate DOUBLE)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
#     def create_date_lookup(self):
#         if(self.initialized):
#             print(f"Creating date_lookup table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
#                     date date, 
#                     week int, 
#                     year int, 
#                     month int, 
#                     dayofweek int, 
#                     dayofmonth int, 
#                     dayofyear int, 
#                     week_part string)
#                   """)  
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
#     def create_workout_bpm_summary(self):
#         if(self.initialized):
#             print(f"Creating workout_bpm_summary table...", end='')
#             spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
#                     workout_id INT, 
#                     session_id INT, 
#                     user_id BIGINT, 
#                     age STRING, 
#                     gender STRING, 
#                     city STRING, 
#                     state STRING, 
#                     min_bpm DOUBLE, 
#                     avg_bpm DOUBLE, 
#                     max_bpm DOUBLE, 
#                     num_recordings BIGINT)
#                   """)
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
#     def create_gym_summary(self):
#         if(self.initialized):
#             print(f"Creating gym_summar gold view...", end='')
#             spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
#                             SELECT to_date(login::timestamp) date,
#                             gym, l.mac_address, workout_id, session_id, 
#                             round((logout::long - login::long)/60,2) minutes_in_gym,
#                             round((end_time::long - start_time::long)/60,2) minutes_exercising
#                             FROM gym_logs l 
#                             JOIN (
#                             SELECT mac_address, workout_id, session_id, start_time, end_time
#                             FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
#                             ON l.mac_address = w.mac_address 
#                             AND w. start_time BETWEEN l.login AND l.logout
#                             order by date, gym, l.mac_address, session_id
#                         """)
#             print("Done")
#         else:
#             raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
#     def setup(self):
#         import time
#         start = int(time.time())
#         print(f"\nStarting setup ...")
#         self.create_db()       
#         self.create_registered_users_bz()
#         self.create_gym_logins_bz() 
#         self.create_kafka_multiplex_bz()        
#         self.create_users()
#         self.create_gym_logs()
#         self.create_user_profile()
#         self.create_heart_rate()
#         self.create_workouts()
#         self.create_completed_workouts()
#         self.create_workout_bpm()
#         self.create_user_bins()
#         self.create_date_lookup()
#         self.create_workout_bpm_summary()  
#         self.create_gym_summary()
#         print(f"Setup completed in {int(time.time()) - start} seconds")
        
#     def assert_table(self, table_name):
#         assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
#                    .filter(f"isTemporary == false and tableName == '{table_name}'") \
#                    .count() == 1, f"The table {table_name} is missing"
#         print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
        
#     def validate(self):
#         import time
#         start = int(time.time())
#         print(f"\nStarting setup validation ...")
#         assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
#                     .filter(f"databaseName == '{self.db_name}'") \
#                     .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
#         print(f"Found database {self.catalog}.{self.db_name}: Success")
#         self.assert_table("registered_users_bz")   
#         self.assert_table("gym_logins_bz")        
#         self.assert_table("kafka_multiplex_bz")
#         self.assert_table("users")
#         self.assert_table("gym_logs")
#         self.assert_table("user_profile")
#         self.assert_table("heart_rate")
#         self.assert_table("workouts")
#         self.assert_table("completed_workouts")
#         self.assert_table("workout_bpm")
#         self.assert_table("user_bins")
#         self.assert_table("date_lookup")
#         self.assert_table("workout_bpm_summary") 
#         self.assert_table("gym_summary") 
#         print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
#     def cleanup(self): 
#         if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
#             print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
#             spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
#             print("Done")
#         print(f"Deleting {self.landing_zone}...", end='')
#         dbutils.fs.rm(self.landing_zone, True)
#         print("Done")
#         print(f"Deleting {self.checkpoint_base}...", end='')
#         dbutils.fs.rm(self.checkpoint_base, True)
#         print("Done")    
