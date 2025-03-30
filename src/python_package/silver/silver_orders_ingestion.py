from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession


# Import all required classes and functions from python_package
from python_package.common.logger import Logger
from python_package.common.error_handler import UpsertError, IngestionError
from python_package.common.common_functions import extract_books_subtotal, clean_column_values, extract_and_convert_price

# Get custom logger from Logger class
logger = Logger().get_logger()


class SilverOrdersIngestion:

    """
        A class for ingesting streaming data from the Bronze orders table into the Silver orders table
        using Spark Structured Streaming with a micro-batch upsert strategy.
        
        Attributes:
            spark (SparkSession):
                The Spark session to interact with Spark SQL.

            checkpoint_base_dir (str): 
                The base directory for checkpoint storage.

            env (str, optional): 
                The environment prefix (e.g., dev, prod). Defaults to None.
    """

    def __init__(self,spark:SparkSession, base_checkpoint_path:str, env:str=None)->None:
        """
            Initializes the SilverCustomersIngestion class.
            
            Args:
                spark (SparkSession): The Spark session to interact with Spark SQL.
                checkpoint_base_dir (str): The base directory for checkpoint storage.
                env (str, optional): The environment prefix. Defaults to None.
        """
        self.spark = spark
        self.env = env 
        self.base_checkpoint_path = f"{base_checkpoint_path}/silver"

    def orders_upsert(self, microBatchDF:DataFrame, batchId:int):

            """
                Performs an upsert (MERGE INTO) operation on the Silver orders table using the micro-batch data.

                Args:
                    micro_batch_df (pyspark.sql.DataFrame): The micro-batch DataFrame.
                    batch (int): The batch ID (not used in the function).
            """

            try: 
                silver_order_table = f"{self.env + '.' if self.env else ''}silver.silver_orders"

                # Extract numeric values from the 'subtotal' in the 'books' column and convert to double.
                # We call the 'extract_books_subtotal' function inside the 'upsert' method since it involves aggregation operations, 
                # which allows us to avoid creating state information.
                df_with_extracted_subtototal = extract_books_subtotal(df=microBatchDF)

                source_count = df_with_extracted_subtototal.count()
                logger.info(f"Batch ID {batchId}: Processed microBatchDF with {source_count} records for upsert into {silver_order_table}.")

                df_with_extracted_subtototal.createOrReplaceTempView("silver_orders_temp_viw")

                sql_query = f"""
                            MERGE INTO {silver_order_table} as t 
                            USING silver_orders_temp_viw as s 
                            ON t.order_id = s.order_id
                            AND t.order_timestamp = s.order_timestamp
                            WHEN NOT MATCHED THEN INSERT * 
                        """
                microBatchDF.sparkSession.sql(sql_query)

            except Exception as e:
                raise UpsertError(msg=e, func_name="orders_upsert")
            
            
    def silver_orders_ingestion(self, processing_time:str="60 seconds", availbale_now:bool=False, query_name:str="silver_orders_writter"):

            """
                Ingests streaming data from the Bronze orders table into the Silver orders table using Spark Structured Streaming.
                It applies a micro-batch upsert strategy using a MERGE INTO query.
                
                Args:
                    processing_time (str, optional): The trigger interval for processing batches. Defaults to "60 seconds".
                    availbale_now (bool, optional): If True, processes all available data at once in batch mode. Defaults to False.
                    query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.
                
                Raises:
                    IngestionError: Raised for various Spark-related errors including AnalysisException, SparkConnectGrpcException,
                                    and Py4JJavaError.
                
                Returns:
                    pyspark.sql.streaming.StreamingQuery: The streaming query handle.
            """

            from pyspark.sql.functions import col, from_json
            from pyspark.sql.utils import AnalysisException
            from pyspark.errors.exceptions.connect import SparkConnectGrpcException
            from py4j.protocol import Py4JJavaError

            try:

                json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total STRING, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal STRING>>"

                bronze_orders_table = f"{self.env + '.' if self.env else ''}bronze.orders"

        

                raw_orders_df = self.spark.readStream.table(bronze_orders_table) \
                        .withColumn("V", from_json(col("value").cast("string"), json_schema)) \
                        .select("V.*") \
                        .filter(col("quantity") > 0) \
                        .dropDuplicates(["order_id", "order_timestamp"])

                # Remove rows with corrupted order_id and customer_id.
                orders_df_with_cleaned_order_id = clean_column_values(df=raw_orders_df, column_name = "order_id")
                orders_df_with_cleaned_customer_id = clean_column_values(df=orders_df_with_cleaned_order_id, column_name = "customer_id")

                # Extract the price from the string (e.g., "$55"), remove rows with null values, and convert the total to double format
                books_df_with_extracted_total_price = extract_and_convert_price(df = orders_df_with_cleaned_customer_id, price_column = "total")

                w_stream = books_df_with_extracted_total_price.writeStream \
                        .option("checkpointLocation", f"{self.base_checkpoint_path}/silver_orders_cp") \
                        .queryName(query_name)
                
                # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
                # Otherwise, it writes in trigger mode with the specified `processing_time`.
                if availbale_now:
                    return w_stream \
                        .foreachBatch(self.orders_upsert) \
                        .trigger(availableNow=True) \
                        .start()
                else:
                    return w_stream \
                            .foreachBatch(self.orders_upsert) \
                            .trigger(processingTime=processing_time) \
                            .start()

            except AnalysisException as e: 
                msg = f"Error: AnalysisException occured. Error message is {e}"
                raise IngestionError(msg=msg, table_name="silevr_orders", db_name="silver")
            
            except SparkConnectGrpcException as e:  # For newer versions
                msg = f"Error: parkConnectGrpcException occured. Error message is {e}"

            except Py4JJavaError as e:  
                msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
                raise IngestionError(msg=msg, table_name="silver_orders", db_name="silver")

            except UpsertError as e:  
                msg = f"Error: Upsert error. Error message is {e}"
                raise IngestionError(msg=msg, table_name="silver_orders", db_name="silver")

            except Exception as e:  
                msg = f"Error: A Python error occurred. Error message is {e}"
                raise IngestionError(msg=msg, table_name="silver_orders", db_name="silver")
