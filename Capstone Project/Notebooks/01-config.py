# Databricks notebook source
class Config():    
    def __init__(self):      
        # 1. Get the base data directory from Databricks external location "data_zone"
        self.base_dir_data = spark.sql("describe external location `data_zone`") \
                                  .select("url").collect()[0][0]

        # 2. Get the base checkpoint directory from external location "checkpoint"
        self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`") \
                                        .select("url").collect()[0][0]

        # 3. Set the database name
        self.db_name = "sbit_db"

        # 4. Control how many files Auto Loader processes per micro-batch
        self.maxFilesPerTrigger = 1000

