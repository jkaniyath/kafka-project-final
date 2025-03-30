from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession


# Import all required classes and functions from python_package
from python_package.common.logger import Logger
from python_package.common.error_handler import UpsertError, IngestionError
from python_package.common.common_functions import clean_column_values

logger = Logger().get_logger()


class SilverCustomersIngestion:

    """
        Handles the ingestion and processing of customer data into the Silver layer using a Spark streaming pipeline.
        This class reads streaming data from a Kafka-backed Bronze layer, processes it, and performs an SCD1-type upsert
        operation on the `silver_customers` table.

        Attributes:
            spark : SparkSession
                The Spark session to interact with Spark SQL.

            base_checkpoint_path (str): 
                The base directory for Spark checkpointing.

            env (str, optional): 
                The environment prefix for database and table names.
        
        Methods:
            customers_upsert(microBatchDF, batchId):
                Performs an upsert operation on the `silver_customers` table by keeping only the latest record per customer.
            
            silver_customers_ingestion(db_name, table_name, env=None, processing_time="60 seconds", available_now=False):
                Reads and processes streaming customer data from the Bronze layer, joins it with a country lookup table,
                and writes the transformed data into the Silver layer with upserts.
    """

    def __init__(self, spark:SparkSession, base_checkpoint_path:str, env:str=None)->None:
        """
            Initializes the SilverCustomersIngestion class.
            
            Args:
                spark (SparkSession): The Spark session to interact with Spark SQL.
                checkpoint_base_dir (str): The base directory for Spark checkpointing.
                env (str, optional): The environment prefix for database and table names.
        """
        self.spark = spark
        self.env = env 
        self.base_checkpoint_path = f"{base_checkpoint_path}/silver"


    def customers_upsert(self, microBatchDF:DataFrame, batchId:int):
        """
            Performs an upsert operation on the `silver_customers` table.
            The latest record per customer is determined based on `row_time`.
            
            Args:
                microBatchDF (DataFrame): The micro-batch DataFrame received from the stream.
                batchId (int): The unique identifier for the micro-batch being processed.
            
            Raises:
                UpsertError: If an error occurs during the upsert process.
        """
        try:
            from pyspark.sql.functions import col, row_number
            from pyspark.sql.window import Window
            
            silver_customers_table = f"{self.env + '.' if self.env else ''}silver.silver_customers"
            window = Window.partitionBy(col("customer_id")).orderBy(col("row_time").desc())

            source_count = microBatchDF.count()
            logger.info(f"Batch ID {batchId}: Processed microBatchDF with {source_count} records for upsert into {silver_customers_table}.")

            # The incoming DataFrame might contain multiple rows for the same customer. 
            # We need to filter out older rows and retain only the latest row for each customer using row_number.
            microBatchDF \
                    .withColumn("row_num", row_number().over(window)) \
                    .filter(col("row_num")==1) \
                    .drop("row_num") \
                    .createOrReplaceTempView("ranked_updates")

            sql_query = f""" 
                        MERGE INTO {silver_customers_table} t 
                        USING ranked_updates s
                        ON t.customer_id = s.customer_id
                        WHEN MATCHED AND s.row_time > t.row_time AND s.row_status = "update" THEN
                        UPDATE SET *
                        WHEN MATCHED AND s.row_time > t.row_time AND s.row_status = "delete" THEN
                        DELETE
                        WHEN NOT MATCHED THEN INSERT *
                    """

            microBatchDF.sparkSession.sql(sql_query)
        
        except Exception as e:
            raise UpsertError(msg=e, func_name="customers_upsert")
        
    def silver_customers_ingestion(self, processing_time:str="60 seconds", availbale_now:bool=False, query_name:str="silver_customers_writter"):

        """
            Reads streaming customer data from the Bronze layer, enriches it with country lookup information,
            and writes it into the Silver layer while handling upserts.

            Args:
                processing_time (str, optional): The interval for micro-batch processing in streaming mode. Defaults to "60 seconds".
                available_now (bool, optional): If True, processes all available data in batch mode; otherwise, uses streaming mode.
                query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.
            
            Returns:
                StreamingQuery: The Spark Streaming query handling the data ingestion.
            
            Raises:
                IngestionError: If any error occurs during data ingestion.
        """

        from pyspark.sql.functions import from_json, col, broadcast, row_number
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError
    
        try:
            # Get the data from lookup table country_lookup to get details of countries then join with customers dataframe.
            bronze_countries_table = f"{self.env + '.' if self.env else ''}bronze.country_lookup"
            countries_df = self.spark.read.table(bronze_countries_table)
            
            # Read data from bronze customers table
            bronze_customers_table = f"{self.env + '.' if self.env else ''}bronze.customers"
            json_schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

            raw_bronze_customers = self.spark.readStream.table(bronze_customers_table) \
                .select(from_json(col("value").cast("string"), json_schema).alias("v")) \
                .select("v.*") \
                .join(broadcast(countries_df), col("code")==col("country_code"), "inner") \
                .select("customer_id", "email", "first_name", "last_name","row_status" ,"gender", "street", "city", "country", "row_time") 

            # Remove corrupted rows with corrupted customer_id
            processed_customers_df = clean_column_values(df=raw_bronze_customers, column_name = "customer_id")

            w_stream = processed_customers_df.writeStream \
                        .option("checkpointLocation", f"{self.base_checkpoint_path}/silver_customers_cp") \
                        .queryName(query_name)

            # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
            # Otherwise, it writes in trigger mode with the specified `processing_time`.
            if availbale_now:
                return w_stream \
                    .foreachBatch(self.customers_upsert) \
                    .trigger(availableNow=True) \
                    .start()
            else:
                return w_stream \
                        .foreachBatch(self.customers_upsert) \
                        .trigger(processingTime=processing_time) \
                        .start()

            
        except AnalysisException as e: 
            msg = f"Error: AnalysisException occured. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silevr_customers", db_name="silver")

        except SparkConnectGrpcException as e:  # For newer versions
            msg = f"Error: parkConnectGrpcException occured. Error message is {e}"

        except Py4JJavaError as e:  
            msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_customers", db_name="silver")

        except UpsertError as e:  
            msg = f"Error: Upsert error. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_customers", db_name="silver")

        except Exception as e:  
            msg = f"Error: A Python error occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_customers", db_name="silver")
        


