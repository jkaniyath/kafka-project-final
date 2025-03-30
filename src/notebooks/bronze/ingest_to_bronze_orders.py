# Databricks notebook source

"""
Ingestion and Configuration Setup

This notebook is focused on setting up the ingestion pipeline by integrating various components, including configuration management, logging, and Kafka secrets retrieval.

1. Import Required Modules and Classes

We begin by importing necessary components that handle bronze data ingestion, configuration, Kafka secrets, and logging.

```python
Importing the BronzeIngestion class to handle the ingestion of raw data into the bronze layer
from python_package.bronze.bronze_ingestion import BronzeIngestion

Importing the Config class for configuration management
from python_package.common.config import Config

Importing the function to retrieve Kafka-related secrets from Databricks secret scope
from python_package.common.common_functions import get_kafka_serets

Importing the Logger class to handle logging functionalities
from python_package.common.logger import Logger
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
# Get the custom logger from Logger class
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
- Step 2 streams data from Kafka topic Orders to the Orders table Bronze layer.
- Error handling ensures any issues are logged for later review.
"""

try:
    bronze_ingestion = BronzeIngestion( spark=spark,
                                    env=env,
                                    bootstrap_server=BOOTSTRAP_SERVER, 
                                    kafka_username=SASL_USERNAME, 
                                    kafka_password=SASL_PASSWORD,
                                    base_checkpoint_dir=checkpoint_location)
    
    books_writter = bronze_ingestion.ingest_to_orders_table(topic="orders",
                                                            outputMode="append", 
                                                            processing_time="30 seconds", 
                                                            available_now=False)
except Exception as e:
    logger.error(e)