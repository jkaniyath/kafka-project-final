from typing import List
from pyspark.sql import SparkSession

# Import all required classes from python_package
from python_package.common.common_functions import create_db
from python_package.common.logger import Logger
from python_package.common.error_handler import DatabaseCreationErrorHandler, BronzeSetupError


logger = Logger().get_logger()



class BronzeSetup:

    """
    A class for setting up the bronze database and tables for Kafka data ingestion.

    This class provides methods to create a bronze database, create Kafka-based tables for ingestion,
    and configure the necessary setup for handling the data streams.

    Attributes:
        env (str): The environment in which the setup is running (e.g., 'dev', 'prod').
        db_name (str): The name of the database to be used.
        table_names (List[str]): A list of table names to be created.
        spark (SparkSession): The Spark session to interact with Spark SQL.
        bronze_db_location (str): The location where the bronze database will be stored.
        is_db_created (bool): A flag indicating if the database has been successfully created.

    Methods:
        create_bronze_db(db_name, env):
            Creates the bronze database with the specified name.

        get_bronze_kafka_schema():
            Returns the schema for the bronze tables to ingest data from Kafka topics.

        create_bronze_kafka_tables(db_name, table_names, bronze_kafka_schema, env):
            Creates the specified Kafka-based tables in the bronze database.

        create_country_lookup_table(table_name, db_name, env):
            Creates a country lookup table in the bronze database.

        run_bronze_setup():
            Runs the full setup for creating the bronze database and all required tables.
    """

    def __init__(self,spark:SparkSession, bronze_db_location:str, db_name:str, table_names:List[str], env:str=None)->None:
        """
        Initializes the BronzeSetup class with the required configurations.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            bronze_db_location (str): The location where the bronze database will be stored.
            db_name (str): The name of the database to be used.
            table_names (List[str]): A list of table names to be created in the bronze database.
            env (str, optional): The environment in which the setup is running (e.g., 'dev', 'prod').

        Returns:
            None
        """
        self.env = env
        self.db_name = db_name
        self.table_names = table_names
        self.spark = spark
        self.bronze_db_location = bronze_db_location
        self.is_db_created = False

    def create_bronze_db(self, db_name, env:str):
        """
            Creates the bronze database with the specified name.

            Args:
                db_name (str): The name of the database to create.

            Raises:
                DatabaseCreationErrorHandler: If the database creation fails.
        """
        create_db(spark=self.spark, db_name=db_name, env=env, location=self.bronze_db_location)
        self.is_db_created = True 

    def get_bronze_kafka_schema(self) -> str:
        """
            Returns the schema for the bronze tables to ingest data from kafka topics.

            Returns:
                str: The schema string for the bronze tables.
        """
        
        schema =  "key BINARY, value BINARY, topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, timestampType INT"

        return schema
    
    def create_bronze_kafka_tables(self, db_name:str, table_names:List[str], bronze_kafka_schema:str, env:str) -> None:
        """
        Creates the specified Kafka-based tables in the bronze database.

        Args:
            db_name (str): The name of the database in which the tables will be created.
            table_names (List[str]): A list of table names to be created.
            bronze_kafka_schema (str): The schema for the Kafka tables.
            env (str): The environment in which the tables are created (e.g., 'dev', 'prod').

        Raises:
            ValueError: If the list of table names is empty.
            DatabaseCreationErrorHandler: If the database has not been created yet.
        
        Returns:
            None
        """

        if not table_names:
            raise ValueError("The list of table names is empty.")
        
        for table_name in table_names:
            if self.is_db_created:
                table_naming = f"{env + '.' if env else ''}{db_name}.{table_name}"
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table_naming} ({bronze_kafka_schema})")
            else:
                raise DatabaseCreationErrorHandler(db_name)
            
    def create_country_lookup_table(self, table_name:str, db_name:str, env:str=None)->None:

        """
        Creates a country lookup table in the bronze database.

        Args:
            table_name (str): The name of the table to be created (e.g., "country_lookup").
            db_name (str): The name of the database in which the table will be created.
            env (str, optional): The environment in which the table is created (e.g., 'dev', 'prod').

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """
        
        schema = "calling_code STRING, code STRING, country STRING"

        if self.is_db_created:
            table_naming = f"{env + '.' if env else ''}{db_name}.{table_name}"
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table_naming} ({schema})")
        else:
            raise DatabaseCreationErrorHandler(db_name)
        
    def run_bronze_setup(self):
        
        """
        Runs the full setup for creating the bronze database and all required tables.

        This method performs the following steps:
        1. Creates the bronze database.
        2. Creates Kafka-based tables for data ingestion.
        3. Creates a country lookup table.

        Raises:
            BronzeSetupError: If there is any error during the setup process, including database creation failure
                              or invalid table names.
        
        Returns:
            None
        """

        import time 
        try:
            start_time = time.time()
            
            bronze_kafka_schema = self.get_bronze_kafka_schema()

            # Create databse in bronze layer
            self.create_bronze_db(db_name=self.db_name, env=self.env)

            # Create all provided tables to ingest data from kafka in bronze layer under the specified databse name.
            self.create_bronze_kafka_tables(db_name=self.db_name, table_names=self.table_names, bronze_kafka_schema= bronze_kafka_schema, env=self.env)

            self.create_country_lookup_table(table_name="country_lookup", db_name=self.db_name, env=self.env)

            end_time = time.time()
            run_time = int(end_time - start_time)
            logger.info(f"Succesfullly created bronze database {self.db_name} and tables {self.table_names} in {run_time} seconds")
            
        except DatabaseCreationErrorHandler as e:
            raise BronzeSetupError(e) 
        
        except ValueError as e:
            raise BronzeSetupError(e) 
        
        except Exception as e:
            raise BronzeSetupError(e)
    

    
