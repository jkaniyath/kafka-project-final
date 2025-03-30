import sys
import os
from pyspark.sql import SparkSession

# Get the absolute path of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Get the src directory (two levels up)
src_dir = os.path.abspath(os.path.join(current_dir, "../../"))

# Add src to sys.path
sys.path.append(src_dir)


from python_package.common.common_functions import create_db
from python_package.common.logger import Logger
from python_package.common.common_functions import get_silver_table_schemas, create_delta_table
from python_package.common.error_handler import SilverSchemaError, DatabaseCreationErrorHandler, SilverSetupError


logger = Logger().get_logger()


class SilverSetup:

    """
    A class for setting up the silver database and tables in the silver layer.

    This class provides methods to create the silver database, create silver tables for refined data,
    and configure the necessary setup for handling silver data processing.

    Attributes:
        env (str): The environment in which the setup is running (e.g., 'dev', 'prod').
        db_name (str): The name of the database to be used for the silver layer.
        spark (SparkSession): The Spark session to interact with Spark SQL.
        silver_db_location (str): The location where the silver database will be stored.
        is_db_created (bool): A flag indicating if the silver database has been successfully created.
        silver_schema_list (list): A list of schemas for the silver tables.

    Methods:
        get_silver_schema_list():
            Returns the list of schemas for the silver tables.

        create_silver_db(spark, db_location, db_name, env):
            Creates the silver database with the specified name and location.

        create_books_table(spark, db_name, table_name):
            Creates the "books" table in the silver database.

        create_customers_table(spark, db_name, table_name):
            Creates the "customers" table in the silver database.

        create_orders_table(spark, db_name, table_name):
            Creates the "orders" table in the silver database.

        customers_orders_table(spark, db_name, table_name):
            Creates the "customers_orders" table in the silver database.

        books_orders_table(spark, db_name, table_name):
            Creates the "books_orders" table in the silver database.

        run_silver_setup():
            Runs the full setup for creating the silver database and all required tables.
    """
    def __init__(self,spark:SparkSession, db_name:str, silver_db_location:str, env:str=None) -> None:
        """
            Initializes the SilvereSetup object.

            Args:
                db_name (str): The name of the database to create.
                silver_db_location (str): External location for schema
                table_names (List[str]): A list of table names to create in the database.
                env (str, optional): The environment for the setup (default is None).
        """
        self.env = env 
        self.db_name = db_name 
        self.spark = spark
        self.silver_db_location = silver_db_location
        self.is_db_created = False
        self.silver_schema_list = self.get_silver_schema_list()

    def get_silver_schema_list(self)->list[str]:
        """
        Returns the list of schemas for the silver tables.

        The schemas are fetched by calling an external function `get_silver_table_schemas()`. 
        If the number of schemas does not match the expected value, an error is raised.

        Returns:
            list[str]: A list containing the schemas for the silver tables.

        Raises:
            SilverSchemaError: If the number of schemas is not correct.
        """
                
        silver_schema_list = get_silver_table_schemas()
        if len(silver_schema_list) != 5:
            raise SilverSchemaError(silver_schema_list)
        
        return silver_schema_list
    
    def create_silver_db(self,spark:SparkSession, db_location:str, db_name:str, env:str):
        """
            Creates the silver database with the specified name.

            Args:
                db_name (str): The name of the database to create.

            Raises:
                DatabaseCreationErrorHandler: If the database creation fails.
        """
        create_db(spark=spark, db_name=db_name,location=db_location, env=env)
        self.is_db_created = True 

    def create_books_table(self,spark:SparkSession, db_name, table_name):
        """
        Creates the "books" table in the silver database.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            db_name (str): The name of the database in which the table will be created.
            table_name (str): The name of the table to create (should be "silver_books").

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """
                
        if self.is_db_created:
            books_schema = self.get_silver_schema_list()[0]
            create_delta_table(spark=spark, table_name=table_name, db_name=db_name, schema=books_schema, env=self.env)
        else:
            raise DatabaseCreationErrorHandler(db_name=db_name)
        
    def create_customers_table(self,spark:SparkSession, db_name:str, table_name:str):

        """
        Creates the "customers" table in the silver database.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            db_name (str): The name of the database in which the table will be created.
            table_name (str): The name of the table to create (should be "silver_customers").

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """

        if self.is_db_created:
            customers_schema = self.get_silver_schema_list()[1]
            create_delta_table(spark=spark, table_name=table_name, db_name=db_name, schema=customers_schema, env=self.env, is_cdf_enabled=True)
        else:
            raise DatabaseCreationErrorHandler(db_name=db_name)
        
        
    def create_orders_table(self,spark:SparkSession, db_name:str, table_name:str):
        """
        Creates the "orders" table in the silver database.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            db_name (str): The name of the database in which the table will be created.
            table_name (str): The name of the table to create (should be "silver_orders").

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """
        if self.is_db_created:
            orders_schema = self.get_silver_schema_list()[2]
            create_delta_table(spark=spark, table_name=table_name, db_name=db_name, schema=orders_schema, env=self.env, liquid_clustering_enabled=True, cluster_columns=["order_id", "order_timestamp"])
        else:
            raise DatabaseCreationErrorHandler(db_name=db_name)
        
    def customers_orders_table(self,spark:SparkSession, db_name:str, table_name:str):
        """
        Creates the "customers_orders" table in the silver database.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            db_name (str): The name of the database in which the table will be created.
            table_name (str): The name of the table to create (should be "silver_customers_orders").

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """

        if self.is_db_created:
            customers_orders_schema = self.get_silver_schema_list()[3]
            create_delta_table(spark=spark, table_name=table_name, db_name=db_name, schema=customers_orders_schema, env=self.env, liquid_clustering_enabled=True,  cluster_columns=["order_id", "customer_id", "processed_timestamp"])
        else:
            raise DatabaseCreationErrorHandler(db_name=db_name)
        
    def books_orders_table(self,spark:SparkSession, db_name:str, table_name:str):
        """
        Creates the "books_orders" table in the silver database.

        Args:
            spark (SparkSession): The Spark session to interact with Spark SQL.
            db_name (str): The name of the database in which the table will be created.
            table_name (str): The name of the table to create (should be "silver_books_orders").

        Raises:
            DatabaseCreationErrorHandler: If the database has not been created yet.

        Returns:
            None
        """
        if self.is_db_created:
            books_orders_schema = self.get_silver_schema_list()[4]
            create_delta_table(spark=spark, table_name=table_name, db_name=db_name, schema=books_orders_schema, env=self.env)
        else:
            raise DatabaseCreationErrorHandler(db_name=db_name)
        
    def run_silver_setup(self):
        """
        Runs the full setup for creating the silver database and all required tables.

        This method performs the following steps:
        1. Creates the silver database.
        2. Creates the "books", "customers", "orders", "customers_orders", and "books_orders" tables in the silver database.

        Raises:
            SilverSetupError: If there is any error during the setup process, including database creation failure,
                              schema errors, or table creation errors.

        Returns:
            None
        """
        
        import time 
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError

        try:
            start_time = time.time()
            logger.info(f"Start creating databse and tables in silver layer")
            self.create_silver_db(spark=self.spark, db_name=self.db_name, db_location=self.silver_db_location, env=self.env)
            self.create_books_table(spark=self.spark, db_name=self.db_name, table_name="silver_books")
            self.create_customers_table(spark=self.spark, db_name=self.db_name, table_name="silver_customers")
            self.create_orders_table(spark=self.spark, db_name=self.db_name, table_name="silver_orders")
            self.customers_orders_table(spark=self.spark, db_name=self.db_name, table_name="silver_customers_orders")
            self.books_orders_table(spark=self.spark, db_name=self.db_name, table_name="silver_books_orders")
            end_time = time.time()
            run_time = int(end_time - start_time)
            logger.info(f"Finished creating databse and tables in silver layer. Run time: {run_time} seconds")

        except SilverSchemaError as e:  
            raise SilverSetupError(e)
        except AnalysisException as e:
            raise SilverSetupError(e)
        except Py4JJavaError as e:
            raise SilverSetupError(e)
        except SparkConnectGrpcException as e:
            raise SilverSetupError(e)

    