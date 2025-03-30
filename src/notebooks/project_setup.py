# Databricks notebook source
# Import required classes from python_package
from python_package.bronze.bronze_setup import BronzeSetup
from python_package.silver.silver_setup import SilverSetup
from python_package.gold.gold_setup import GoldSetup
from python_package.common.config import Config
from python_package.common.logger import Logger


# COMMAND ----------
# Get custom logger from Logger class
logger  = Logger().get_logger()

# COMMAND ----------
# Get spark session from Config class
config = Config()
spark = config.spark
dbutils = config.dbutils


# COMMAND ----------
# Get enviorment variable using widgets(For example cataloge value)
dbutils.widgets.text('env', 'dev')
env = dbutils.widgets.get('env') 

# COMMAND ----------
bronze_db_location = config.get_bronze_external_location()
silver_db_location = config.get_silver_external_location()
gold_db_location = config.get_gold_external_location()

# COMMAND ----------
bronze_setup = BronzeSetup(spark=spark,  db_name="bronze", bronze_db_location=bronze_db_location,  table_names=["books","orders","customers"], env=env)


# COMMAND ----------
silver_setup = SilverSetup(spark=spark, db_name="silver", silver_db_location=silver_db_location, env=env)


# COMMAND ----------
gold_setup = GoldSetup(spark=spark, db_name="gold", gold_db_location=gold_db_location, env=env)

# COMMAND ----------
# Run bronze and silver set up to create database and tables in bronze and silver layers.
try:
    bronze_setup.run_bronze_setup()
    silver_setup.run_silver_setup()
    gold_setup.run_gold_setup()
except Exception as e:
    logger.error(e)