# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

from typing import Optional


class Producer:
    """
    Deterministic test-data generator for the landing zone.

    Responsibilities
    ----------------
    • Copy fixture files from `<data_zone>/test_data` into `<data_zone>/raw/...`
      so that Bronze Auto Loader streams will pick them up.
    • Provide basic validations of landed raw record counts, per test 'set'.

    Feeds
    -----
    1) registered_users_bz (CSV)         -> 1-registered_users_{set}.csv
    2) kafka_multiplex_bz (user_info)    -> 2-user_info_{set}.json
    3) kafka_multiplex_bz (bpm)          -> 3-bpm_{set}.json
    4) kafka_multiplex_bz (workout)      -> 4-workout_{set}.json
    5) gym_logins_bz (CSV)               -> 5-gym_logins_{set}.csv

    Typical usage
    -------------
    pr = Producer()
    pr.produce(1)     # copy 'set 1' files into the landing zone
    pr.validate(1)    # assert expected raw counts are present
    """

    def __init__(self) -> None:
        conf = Config()
        self.landing_zone: str = f"{conf.base_dir_data}/raw"
        self.test_data_dir: str = f"{conf.base_dir_data}/test_data"

    # --------------------------
    # Internal path/copy helpers
    # --------------------------
    def _src(self, filename: str) -> str:
        """Build absolute source path inside `test_data`."""
        return f"{self.test_data_dir}/{filename}"

    def _dst(self, subdir: str, filename: str) -> str:
        """Build absolute destination path inside the landing zone."""
        return f"{self.landing_zone}/{subdir}/{filename}"

    @staticmethod
    def _cp(src: str, dst: str) -> None:
        """
        Copy a file in DBFS/cloud storage, with simple logging.
        Raises on failure so calling code can stop early.
        """
        print(f"Producing {src} -> {dst} ...", end="")
        ok = dbutils.fs.cp(src, dst, recurse=False)
        # dbutils.fs.cp returns True/False; throw if it failed.
        if not ok:
            raise IOError(f"Copy failed: {src} -> {dst}")
        print("Done")

    # --------------------------
    # Per-feed producers
    # --------------------------
    def user_registration(self, set_num: int) -> None:
        """Emit the registered_users CSV for a given set into its landing folder."""
        fn = f"1-registered_users_{set_num}.csv"
        self._cp(self._src(fn), self._dst("registered_users_bz", fn))

    def profile_cdc(self, set_num: int) -> None:
        """Emit the user_info CDC JSON for a given set into kafka_multiplex landing."""
        fn = f"2-user_info_{set_num}.json"
        self._cp(self._src(fn), self._dst("kafka_multiplex_bz", fn))

    def workout(self, set_num: int) -> None:
        """Emit the workout JSON for a given set into kafka_multiplex landing."""
        fn = f"4-workout_{set_num}.json"
        self._cp(self._src(fn), self._dst("kafka_multiplex_bz", fn))

    def bpm(self, set_num: int) -> None:
        """Emit the large bpm JSON for a given set into kafka_multiplex landing."""
        fn = f"3-bpm_{set_num}.json"
        self._cp(self._src(fn), self._dst("kafka_multiplex_bz", fn))

    def gym_logins(self, set_num: int) -> None:
        """Emit the gym_logins CSV for a given set into its landing folder."""
        fn = f"5-gym_logins_{set_num}.csv"
        self._cp(self._src(fn), self._dst("gym_logins_bz", fn))

    # --------------------------
    # Batch producer / orchestrator
    # --------------------------
    def produce(self, set_num: int) -> None:
        """
        Produce a single test 'set' into the landing zone.

        Behavior
        --------
        • For set_num <= 2  : emit all small feeds (users, user_info, workout, gym_logins)
        • For set_num <= 10 : also emit the large bpm feed
        """
        import time

        start = int(time.time())
        print(f"\nProducing test data set {set_num} ...")

        # small feeds (1..2)
        if set_num <= 2:
            self.user_registration(set_num)
            self.profile_cdc(set_num)
            self.workout(set_num)
            self.gym_logins(set_num)

        # large bpm feed (1..10)
        if set_num <= 10:
            self.bpm(set_num)

        print(f"Test data set {set_num} produced in {int(time.time()) - start} seconds")

    # --------------------------
    # Validation helpers
    # --------------------------
    def _validate_count(self, fmt: str, location_prefix: str, expected_count: int) -> None:
        """
        Validate record count for files matching a glob in the landing zone.

        Parameters
        ----------
        fmt : str
            Spark format ('csv' or 'json').
        location_prefix : str
            Sub-path under the landing zone without extension/glob; a '*' wildcard
            is appended by this function (e.g., 'kafka_multiplex_bz/2-user_info').
        expected_count : int
            Expected number of rows across all matched files.
        """
        target_glob = f"{self.landing_zone}/{location_prefix}_*.{fmt}"
        print(f"Validating {location_prefix} ({fmt}) ...", end="")
        df = spark.read.format(fmt)
        if fmt.lower() == "csv":
            df = df.option("header", "true")
        actual = df.load(target_glob).count()
        assert actual == expected_count, (
            f"Expected {expected_count:,} records, found {actual:,} in {location_prefix}"
        )
        print(f"Found {actual:,} / Expected {expected_count:,}: Success")

    def validate(self, sets: int) -> None:
        """
        Validate that N 'sets' of raw files exist with the expected row counts.

        Parameters
        ----------
        sets : int
            Number of produced sets (1 or 2 for small feeds; BPM scales linearly).

        Notes
        -----
        • Expected counts are derived from your deterministic fixtures:
          - registered_users : 5 rows per set
          - user_info        : 7 rows (set 1) -> 13 rows (set 2)
          - bpm              : 253,801 rows per set
          - workout          : 16 rows (set 1) -> 32 rows (set 2)
          - gym_logins       : 8 rows (set 1) -> 16 rows (set 2)
        """
        import time

        start = int(time.time())
        print(f"\nValidating test data across {sets} set(s)...")

        # CSV feeds
        self._validate_count("csv",  "registered_users_bz/1-registered_users", 5 if sets == 1 else 10)
        self._validate_count("csv",  "gym_logins_bz/5-gym_logins",             8 if sets == 1 else 16)

        # JSON feeds (kafka_multiplex)
        self._validate_count("json", "kafka_multiplex_bz/2-user_info",         7 if sets == 1 else 13)
        self._validate_count("json", "kafka_multiplex_bz/4-workout",          16 if sets == 1 else 32)
        self._validate_count("json", "kafka_multiplex_bz/3-bpm",               sets * 253_801)

        # Optional: print timing if you like
        # print(f"Test data validation completed in {int(time.time()) - start} seconds")




# # Databricks notebook source
# # MAGIC %run ./01-config

# # COMMAND ----------

# class Producer():
#     def __init__(self):
#         self.Conf = Config()
#         self.landing_zone = self.Conf.base_dir_data + "/raw"      
#         self.test_data_dir = self.Conf.base_dir_data + "/test_data"
               
#     def user_registration(self, set_num):
#         source = f"{self.test_data_dir}/1-registered_users_{set_num}.csv"
#         target = f"{self.landing_zone}/registered_users_bz/1-registered_users_{set_num}.csv" 
#         print(f"Producing {source}...", end='')
#         dbutils.fs.cp(source, target)
#         print("Done")
        
#     def profile_cdc(self, set_num):
#         source = f"{self.test_data_dir}/2-user_info_{set_num}.json"
#         target = f"{self.landing_zone}/kafka_multiplex_bz/2-user_info_{set_num}.json"
#         print(f"Producing {source}...", end='')
#         dbutils.fs.cp(source, target)
#         print("Done")        
        
#     def workout(self, set_num):
#         source = f"{self.test_data_dir}/4-workout_{set_num}.json"
#         target = f"{self.landing_zone}/kafka_multiplex_bz/4-workout_{set_num}.json"
#         print(f"Producing {source}...", end='')
#         dbutils.fs.cp(source, target)
#         print("Done")
        
#     def bpm(self, set_num):
#         source = f"{self.test_data_dir}/3-bpm_{set_num}.json"
#         target = f"{self.landing_zone}/kafka_multiplex_bz/3-bpm_{set_num}.json"
#         print(f"Producing {source}...", end='')
#         dbutils.fs.cp(source, target)
#         print("Done")
        
#     def gym_logins(self, set_num):
#         source = f"{self.test_data_dir}/5-gym_logins_{set_num}.csv"
#         target = f"{self.landing_zone}/gym_logins_bz/5-gym_logins_{set_num}.csv" 
#         print(f"Producing {source}...", end='')
#         dbutils.fs.cp(source, target)
#         print("Done")
        
#     def produce(self, set_num):
#         import time
#         start = int(time.time())
#         print(f"\nProducing test data set {set_num} ...")
#         if set_num <=2:
#             self.user_registration(set_num)
#             self.profile_cdc(set_num)        
#             self.workout(set_num)
#             self.gym_logins(set_num)
#         if set_num <=10:
#             self.bpm(set_num)
#         print(f"Test data set {set_num} produced in {int(time.time()) - start} seconds")
    
#     def _validate_count(self, format, location, expected_count):
#         print(f"Validating {location}...", end='')
#         target = f"{self.landing_zone}/{location}_*.{format}"
#         actual_count = (spark.read
#                              .format(format)
#                              .option("header","true")
#                              .load(target).count())
#         assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {location}"
#         print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")
          
#     def validate(self, sets):
#         import time
#         start = int(time.time())
#         print(f"\nValidating test data {sets} sets...")       
#         self._validate_count("csv", "registered_users_bz/1-registered_users", 5 if sets == 1 else 10)
#         self._validate_count("json","kafka_multiplex_bz/2-user_info", 7 if sets == 1 else 13)
#         self._validate_count("json","kafka_multiplex_bz/3-bpm", sets * 253801)
#         self._validate_count("json","kafka_multiplex_bz/4-workout", 16 if sets == 1 else 32)  
#         self._validate_count("csv", "gym_logins_bz/5-gym_logins", 8 if sets == 1 else 16)
#         #print(f"Test data validation completed in {int(time.time()) - start} seconds")
