# Databricks notebook source

"""
Importing Required Modules for Countries Lookup Ingestion

This section imports the necessary modules to facilitate logging, configuration management, and country lookup data ingestion in a Databricks or Spark environment.

1. Import Configuration, Logging, and Ingestion Modules

To maintain modularity and reusability, the following imports are used:

```python
from python_package.common.config import Config
from python_package.common.logger import Logger
from python_package.bronze.countries_lookup_ingestion import CountriesIngestion
"""

from python_package.common.config import Config
from python_package.common.logger import Logger
from python_package.bronze.countries_lookup_ingestion import CountriesIngestion

# COMMAND ----------
# Get spark session from Config class
conf = Config()
spark = conf.spark
dbutils = conf.dbutils

# Get externa location for landing zone and then get countries data
landing_zone = conf.get_landing_zone()
countries_data_path = f"{landing_zone}/country_lookup"

# COMMAND ----------
# Get the custom logger from Logger class.
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
"""
Ingesting Country Lookup Data into a Delta Table

This section demonstrates how to use the `CountriesIngestion` class to ingest country-related data from an external CSV file stored in Azure Data Lake into the `bronze.country_lookup` Delta table. The ingestion process follows a structured three-layer naming convention based on the environment.

1. Initialize `CountriesIngestion` and Ingest Data

The following code initializes an instance of `CountriesIngestion` with the active Spark session and environment name, then calls the `ingest_to_country_lookup` method to read the CSV file and write it to the Delta table.

```python
try:
    countries_ingestion = CountriesIngestion(spark=spark, env=env)  # Initialize ingestion class
    countries_ingestion.ingest_to_country_lookup(path=countries_data_path)  # Ingest data from CSV file
except Exception as e:
    logger.error(e)  # Log any errors encountered during ingestion
"""

try:
    countries_ingestion = CountriesIngestion(spark=spark, env=env)
    countries_ingestion.ingest_to_country_lookup(path=countries_data_path)
except Exception as e:
    logger.error(e)