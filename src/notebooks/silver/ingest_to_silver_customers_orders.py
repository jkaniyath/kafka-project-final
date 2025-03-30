# Databricks notebook source

"""
This code imports three components:

1. SilverCustomersOrdersIngestion: Likely responsible for ingesting customers orders data into the silver layer of the data pipeline.
2. Config: Manages configuration settings, such as database credentials or environment variables.
3. Logger: Handles logging of messages for tracking execution and debugging.

These components are essential for managing data ingestion, configuration, and monitoring within the pipeline.
"""

from python_package.silver.silver_customers_orders_ingestion import SilverCustomersOrdersIngestion
from python_package.common.config import Config
from python_package.common.logger import Logger

# COMMAND ----------
# Get spark session from Config class
conf = Config()
spark = conf.spark
dbutils = conf.dbutils

# COMMAND ----------
# Get the custom logger Logger class. 
logger = Logger().get_logger()

"""
Sets up a Databricks widget for environment selection and retrieves its value.

Steps:
1. Creates a text widget named "env" with a default value of "dev".
2. Retrieves the value of the "env" widget, which allows dynamic configuration.

This approach is commonly used in Databricks notebooks to enable parameterization, 
allowing users to specify the environment dynamically (e.g., "dev", "staging", "prod").

Attributes:
    env (str): The selected environment from the widget input.
"""

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------
# Get the external location for checkpoints.
checkpoint_location = conf.get_checkpoint_external_location()

# COMMAND ----------
"""
This code attempts to:

1. Initialize SilverCustomersOrdersIngestion: Creates an instance to handle the ingestion of customers orders data, using the `spark` session, a checkpoint path, and environment details.
2. Call silver_orders_ingestion Method: Starts the ingestion process by calling the method to process the customers orders data.
3. Exception Handling: If any error occurs, it prints and logs the exception using the `logger`.

This ensures that book data is ingested while handling potential errors gracefully.
"""

try:
    customers_orders_ingestion = SilverCustomersOrdersIngestion(spark=spark, base_checkpoint_path=checkpoint_location, env=env)
    customers_orders_writter = customers_orders_ingestion.silver_customers_orders_ingestion(processing_time="30 seconds")
except Exception as e:
    logger.error(e)