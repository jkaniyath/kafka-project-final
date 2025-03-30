# Databricks notebook source

"""
This code imports three components:

1. SilverBooksOrdersIngestion: Likely responsible for ingesting books orders data into the silver layer of the data pipeline.
2. Config: Manages configuration settings, such as database credentials or environment variables.
3. Logger: Handles logging of messages for tracking execution and debugging.

These components are essential for managing data ingestion, configuration, and monitoring within the pipeline.
"""

from python_package.silver.silver_books_orders_ingestion import SilverBooksOrdersIngestion
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

# COMMAND ----------
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

1. Initialize SilverBooksIngestion: Creates an instance to handle the ingestion of book data, using the `spark` session, a checkpoint path, and environment details.
2. Call silver_books_ingestion Method: Starts the ingestion process by calling the method to process the books data.
3. Exception Handling: If any error occurs, it prints and logs the exception using the `logger`.

This ensures that book data is ingested while handling potential errors gracefully.

"""

try:
    books_orders_ingestion = SilverBooksOrdersIngestion(spark=spark, base_checkpoint_path=checkpoint_location, env=env)
    books_writter = books_orders_ingestion.silver_books_orders_ingestion(processing_time="30 seconds")
except Exception as e:
    logger.error(e)