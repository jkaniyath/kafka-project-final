# Databricks notebook source
"""
 This code imports four components:

 1. BronzeIngestion: Likely responsible for ingesting data from Kafka into the bronze layer of the data pipeline.
 2. Config: Manages configuration settings, such as Kafka credentials or environment variables.
 3. get_kafka_secrets: Retrieves Kafka secrets (likely for authentication) used in the ingestion process.
 4. Logger: Handles logging of messages for tracking execution and debugging.

 These components are essential for managing Kafka data ingestion, retrieving secrets, and ensuring proper logging within the pipeline."
"""

from python_package.bronze.bronze_ingestion import BronzeIngestion
from python_package.common.config import Config
from python_package.common.common_functions import get_kafka_serets
from python_package.common.logger import Logger

# COMMAND ----------
# Get spark session from Config class
conf = Config()
spark = conf.spark
dbutils = conf.dbutils


# COMMAND ----------
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
# Get all serets required to integrate with kafka server from function get_kafka_serets() using databricks secrets utilty.

kafka_secrets = get_kafka_serets(spark=spark)
BOOTSTRAP_SERVER = kafka_secrets.get("bootstrap_server")
SASL_USERNAME = kafka_secrets.get("sasl_username")
SASL_PASSWORD = kafka_secrets.get("sasl_password")
# COMMAND ----------
"""
1. Initialize the `BronzeIngestion` Class

The `BronzeIngestion` class is initialized with various parameters including the Spark session, environment variables, Kafka server details, and checkpoint directory. This sets up the ingestion process to stream data from Kafka into a Bronze table.

Summary:
- Step 1 initializes the `BronzeIngestion` class to connect to Kafka and set up the environment.
- Step 2 streams data from Kafka topic Books to the Books table Bronze layer.
- Error handling ensures any issues are logged for later review.
"""

try:
    bronze_ingestion = BronzeIngestion( spark=spark,
                                    env=env,
                                    bootstrap_server=BOOTSTRAP_SERVER, 
                                    kafka_username=SASL_USERNAME, 
                                    kafka_password=SASL_PASSWORD,
                                    base_checkpoint_dir=checkpoint_location)
    
    books_writter = bronze_ingestion.ingest_to_books_table(topic="books",
                                                            outputMode="append", 
                                                            processing_time="30 seconds", 
                                                            available_now=False)
except Exception as e:
    logger.error(e)