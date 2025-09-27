# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

# from typing import Optional


class HistoryLoader:
    """
    Seeder/Loader for small, mostly-static historical datasets (lookup dimensions).

    Current responsibilities
    ------------------------
    • Load the `date_lookup` table from JSON fixtures under `<data_zone>/test_data`.
    • Validate the record counts after load.

    Typical usage
    -------------
    hl = HistoryLoader(env="dev")
    hl.load_history()   # loads/overwrites lookup data (idempotent)
    hl.validate()       # asserts expected row counts (e.g., 365 rows for dates)
    """

    def __init__(self, env: str) -> None:
        """
        Initialize the loader with paths & database context.

        Parameters
        ----------
        env : str
            Unity Catalog catalog name (e.g., "dev", "test", "prod").
        """
        conf = Config()
        # Base directories (resolved from UC external locations)
        self.landing_zone: str = f"{conf.base_dir_data}/raw"
        self.test_data_dir: str = f"{conf.base_dir_data}/test_data"

        # UC catalog & database (database name comes from shared Config)
        self.catalog: str = env
        self.db_name: str = conf.db_name

    # --------------------------
    # Internal helpers
    # --------------------------
    def _qdb(self) -> str:
        """Qualified database name: '<catalog>.<db_name>'."""
        return f"{self.catalog}.{self.db_name}"

    def _qt(self, table: str) -> str:
        """Fully qualified table name: '<catalog>.<db_name>.<table>'."""
        return f"{self._qdb()}.{table}"

    def _exec(self, sql: str) -> None:
        """Execute a SQL statement (thin wrapper for consistent style)."""
        spark.sql(sql)

    # --------------------------
    # Loaders
    # --------------------------
    def load_date_lookup(self) -> None:
        """
        Load/overwrite the `date_lookup` table from JSON fixture data.

        Notes
        -----
        • Uses INSERT OVERWRITE to make the load idempotent.
        • Expects file: `<test_data>/6-date-lookup.json/` containing fields:
          (date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part).
        """
        # Ensure database is present; using it avoids fully qualifying in the SELECT.
        self._exec(f"USE {self._qdb()}")

        print("Loading date_lookup table...", end="")
        self._exec(
            f"""
            INSERT OVERWRITE TABLE {self._qt("date_lookup")}
            SELECT
              date,
              week,
              year,
              month,
              dayofweek,
              dayofmonth,
              dayofyear,
              week_part
            FROM json.`{self.test_data_dir}/6-date-lookup.json/`
            """
        )
        print("Done")

    def load_history(self) -> None:
        """
        Orchestrate all historical data loads.

        Currently this only loads `date_lookup`, but this is the single entry point
        to add other lookup/historical loaders later (e.g., postal codes, gyms, etc.).
        """
        import time

        start = int(time.time())
        print("\nStarting historical data load ...")

        # Add additional loaders here as your lookups grow.
        self.load_date_lookup()

        print(f"Historical data load completed in {int(time.time()) - start} seconds")

    # --------------------------
    # Validation helpers
    # --------------------------
    def assert_count(self, table_name: str, expected_count: int) -> None:
        """
        Assert a table's row count equals `expected_count`.

        Parameters
        ----------
        table_name : str
            Unqualified table name (e.g., "date_lookup").
        expected_count : int
            The expected exact row count.

        Raises
        ------
        AssertionError
            If the actual count does not match `expected_count`.
        """
        print(f"Validating record counts in {table_name}...", end="")
        # Reading through the table abstraction triggers Delta's snapshot (strongly consistent).
        actual_count = spark.read.table(self._qt(table_name)).count()
        assert (
            actual_count == expected_count
        ), f"Expected {expected_count:,} records, found {actual_count:,} in {table_name}"
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")

    def validate(self) -> None:
        """
        Validate that all required historical/lookup tables are correctly loaded.

        Currently verifies:
        • `date_lookup` has 365 rows (one common-year worth of dates).
        """
        import time

        start = int(time.time())
        print("\nStarting historical data load validation...")

        # If your fixture has leap-year data, update expected_count accordingly.
        self.assert_count("date_lookup", expected_count=365)

        print(f"Historical data load validation completed in {int(time.time()) - start} seconds")




# class HistoryLoader():
#     def __init__(self, env):
#         Conf = Config()
#         self.landing_zone = Conf.base_dir_data + "/raw"      
#         self.test_data_dir = Conf.base_dir_data + "/test_data"
#         self.catalog = env
#         self.db_name = Conf.db_name
        
#     def load_date_lookup(self):        
#         print(f"Loading date_lookup table...", end='')        
#         spark.sql(f"""INSERT OVERWRITE TABLE {self.catalog}.{self.db_name}.date_lookup 
#                 SELECT date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part 
#                 FROM json.`{self.test_data_dir}/6-date-lookup.json/`""")
#         print("Done")
        
#     def load_history(self):
#         import time
#         start = int(time.time())
#         print(f"\nStarting historical data load ...")
#         self.load_date_lookup()
#         print(f"Historical data load completed in {int(time.time()) - start} seconds")
        
#     def assert_count(self, table_name, expected_count):
#         print(f"Validating record counts in {table_name}...", end='')
#         actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").count()
#         assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name}" 
#         print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")        
        
#     def validate(self):
#         import time
#         start = int(time.time())
#         print(f"\nStarting historical data load validation...")
#         self.assert_count(f"date_lookup", 365)
#         print(f"Historical data load validation completed in {int(time.time()) - start} seconds")               
