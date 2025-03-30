from pyspark.sql import SparkSession


# Import all required classes from python_package
from python_package.common.error_handler import UpsertError, IngestionError
from python_package.common.logger import Logger


# Get custome logger from Logger class
logger = Logger().get_logger()

class GoldAuthorsStatsIngestion:
    """
        GoldIngestion is responsible for processing and ingesting author statistics from a silver table
        into a gold table using structured streaming and batch upserts.

        Attributes:
            spark (SparkSession):  The Spark session to interact with Spark SQL.
            base_checkpoint_dir (str): Base directory for checkpointing streaming queries.
            env (str, optional): The environment name (e.g., 'dev', 'prod') used for namespacing tables.
    """
    def __init__(self, spark:SparkSession, base_checkpoint_dir:str, env:str=None) -> None:
        self.spark = spark
        self.base_checkpoint_dir = f"{base_checkpoint_dir}/gold"
        self.env = env 


    def authors_stats_upsert(self, microBatchDF, batchId):
        """
            Performs an upsert operation on the gold authors statistics table using a micro-batch DataFrame.

            Args:
                microBatchDF (DataFrame): The incoming micro-batch DataFrame from the streaming source.
                batchId (int): The unique batch ID associated with this micro-batch.
        """
        try:
            # Dynamically constructs the table name based on the environment value.
            gold_authors_stats_table = f"{self.env + '.' if self.env else ''}gold.gold_authors_stats"
            microBatchDF.createOrReplaceTempView("silver_books_orders_temp_vw")

            # Count the number of records in the current micro-batch.  
            # This helps in tracking the number of records being processed for the upsert operation. 
            source_count = microBatchDF.count() 
            logger.info(f"Batch ID {batchId}: Processed microBatchDF with {source_count} records for upsert into {gold_authors_stats_table}.")

            # Upsert operation:
            # - If late-arriving rows are detected, they will update the existing records and adjust aggregate values.
            # - Otherwise, new records are inserted into the target table.
            query = f"""
                    MERGE INTO {gold_authors_stats_table} t 
                    USING silver_books_orders_temp_vw s
                    ON t.time = s.time AND t.author = s.author
                    WHEN MATCHED  THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """
            microBatchDF.sparkSession.sql(query)

        except Exception as e:
            raise UpsertError(msg=e, func_name="orders_upsert")
        

    def ingest_to_authors_stats(self, books_orders_watermark:str="10 minutes", window_time:str="5 minutes", processing_time:str="30 seconds", availbale_now:bool=False, query_name="gold_authors_stats_writter"):
        """
            Initiates a structured streaming query to compute and ingest author statistics into the gold table.

            Args:
                books_orders_watermark (str, optional): Watermark duration for late data handling (default: "10 minutes").
                window_time (str, optional): Time window for aggregation (default: "5 minutes").
                processing_time (str, optional): Trigger interval for micro-batch processing (default: "30 seconds").
                availbale_now (bool, optional): Whether to process available data in batch mode and exit (default: False).
                query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.
            Returns:
                StreamingQuery: The started streaming query object.
        """
        
        from pyspark.sql.functions import col, window, count, avg
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError

        try:

            silver_books_table = f"{self.env + '.' if self.env else ''}silver.silver_books_orders"
            
            # Read book orders data from the silver table and aggregate sales metrics per author.
            authors_stats_df = self.spark.readStream.table(silver_books_table) \
                .withWatermark("order_timestamp", "10 minutes") \
                .groupBy(window("order_timestamp", window_time).alias("time"), "author") \
                .agg(count("order_id").alias("orders_count"), avg("quantity").alias("avg_quantity")) 

            w_stream = authors_stats_df.writeStream \
                        .option("checkpointLocation", f"{self.base_checkpoint_dir}/gold_authors_stats_cp") \
                        .queryName(query_name) \
                        .outputMode("update") \
                        .trigger(processingTime=processing_time) 

            # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
            # Otherwise, it writes in trigger mode with the specified `processing_time`.
            if availbale_now:
                return w_stream \
                    .foreachBatch(self.authors_stats_upsert) \
                    .trigger(availableNow=True) \
                    .start()
            else:
                return w_stream \
                        .foreachBatch(self.authors_stats_upsert) \
                        .trigger(processingTime=processing_time) \
                        .start()

        except AnalysisException as e: 
            msg = f"Error: AnalysisException occured. Error message is {e}"
            raise IngestionError(msg=msg, table_name="gold_authors_stats", db_name="gold")

        except SparkConnectGrpcException as e:  # For newer versions
            msg = f"Error: parkConnectGrpcException occured. Error message is {e}"

        except Py4JJavaError as e:  
            msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
            raise IngestionError(msg=msg,  table_name="gold_authors_stats", db_name="gold")

        except UpsertError as e:  
            msg = f"Error: Upsert error. Error message is {e}"
            raise IngestionError(msg=msg, table_name="gold_authors_stats", db_name="gold")

        except Exception as e:  
            msg = f"Error: A Python error occurred. Error message is {e}"
            raise IngestionError(msg=msg,  table_name="gold_authors_stats", db_name="gold")