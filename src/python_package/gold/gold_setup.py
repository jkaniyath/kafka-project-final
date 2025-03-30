from pyspark.sql import SparkSession


# Import all required classes and functions from python_package
from python_package.common.common_functions import create_db, create_delta_table
from python_package.common.logger import Logger
from python_package.common.error_handler import GoldSetupError


logger = Logger().get_logger()


class GoldSetup:
    """
        A class to set up the Gold layer in a data pipeline.

        This class provides methods to create a database, a view for country statistics, and a table for author statistics 
        in the Gold layer of a data processing pipeline.
    """
    def __init__(self, spark:SparkSession, db_name:str, gold_db_location:str, env:str=None)-> None:
        """
            Initializes the GoldSetup class.

            Args:
                spark (SparkSession) :  The Spark session to interact with Spark SQL.
                db_name (str): The name of the gold database.
                gold_db_location (str): External location for schema
                env (str, optional): The environment in which the database is being created (e.g., dev, prod). Defaults to None.
        """
        self.spark = spark
        self.db_name = db_name
        self.gold_db_location = gold_db_location
        self.env = env 

    def create_gold_db(self,spark:SparkSession, db_name, db_location:str, env:str):
        """
            Creates the Gold database.

            Args:
                db_name (str): The name of the database to be created.
                env (str): The environment in which the database is being created.
        """
        create_db(spark=spark, db_name=db_name, location=db_location, env=env)
        self.is_db_created = True 


    def create_countries_stats_view(self):
        """
            Creates a view named 'countries_stats_vw' that aggregates customer orders statistics by country and order date.

            The view is created from the 'silver.silver_customers_orders' table and contains the following fields:
                - country
                - order_date (truncated to the day level)
                - orders_count (number of orders per country per day)
                - total_books (total quantity of books ordered per country per day)
        """
        self.spark.sql("""CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
            SELECT country, date_trunc("DD", order_timestamp) AS order_date,
                COUNT(order_id) AS orders_count,
                SUM(quantity) AS total_books
            FROM silver.silver_customers_orders
            GROUP BY country, date_trunc("DD", order_timestamp)
            )""")
        

    def create_gold_authors_stats_table(self,spark:SparkSession, table_name:str, db_name:str):
        """
            Creates a Delta table to store author statistics.

            Args:
                table_name (str): The name of the table to be created.
                db_name (str): The name of the database in which the table will be created.

            The table schema includes:
                - time (STRUCT with start and end TIMESTAMP fields)
                - author (STRING)
                - orders_count (BIGINT)
                - avg_quantity (DOUBLE)
        """

        authors_stats_schema = "time STRUCT<start: TIMESTAMP, end: TIMESTAMP>, author STRING, orders_count BIGINT, avg_quantity DOUBLE"
        create_delta_table(spark=spark, table_name=table_name, db_name=db_name, env=self.env, schema=authors_stats_schema)


    def run_gold_setup(self):
        """
            Executes the Gold layer setup process.

            This method performs the following steps:
                1. Creates the Gold database.
                2. Creates the 'countries_stats_vw' view.
                3. Creates the 'gold_authors_stats' table.

            Logs the execution time and raises a GoldSetupError in case of failure.

            Raises:
                GoldSetupError: If any step of the setup process fails.
        """
        
        import time 
        try:
            start_time = time.time()
            logger.info(f"Started creating gold databse {self.db_name} and tables")

            # Create databse in gold layer
            self.create_gold_db(spark=self.spark, db_name=self.db_name, db_location=self.gold_db_location, env=self.env)

            # Create view countries_stats_vw
            self.create_countries_stats_view()
         
            # Create gold table authors_stats
            self.create_gold_authors_stats_table(spark=self.spark, table_name='gold_authors_stats', db_name=self.db_name)

            end_time = time.time()
            run_time = int(end_time - start_time)
            logger.info(f"Succesfullly created gold database: {self.db_name}, view: countries_stats_vw, and  table: gold_authors_stats in {run_time} seconds")
            
        except Exception as e:
            raise GoldSetupError(e) 