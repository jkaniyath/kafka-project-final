from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession



# Import all required classes from python_package
from python_package.common.error_handler import  IngestionError, UpsertError
from python_package.common.logger import Logger

# Get custom logger from Logger class
logger = Logger().get_logger()


class SilverCustomersOrdersIngestion:

    """
        Handles the ingestion of customer and order data into the silver layer using Structured Streaming.
        This class reads change data from `silver_customers` and `silver_orders`, applies watermarks, 
        performs inner joins, and prepares the joined table `silver_customers_orders` for the golden layer.
        The processed data is then written to a target silver table using an upsert logic.
        
        Attributes:
            base_checkpoint_path (str): Base path for checkpoint storage.
            env (str, optional): Environment name (e.g., dev, prod) to prefix database names.
    """

    def __init__(self, spark:SparkSession, base_checkpoint_path:str, env:str=None ) -> None:

        """
            Initializes the ingestion class.
            
            Args:
                spark (SparkSession)
                    The Spark session to interact with Spark SQL.

                base_checkpoint_path (str): 
                    Path for storing checkpoint data.

                env (str, optional): 
                    Environment name for table namespace.
        """
        self.spark = spark
        self.env = env 
        self.base_checkpoint_path = f"{base_checkpoint_path}/silver"

    
    def customers_orders_upsert(self, microBatchDF:DataFrame, batchId:int):

        """
            Performs an upsert operation on the silver_customers_orders table using MERGE.
            
            Args:
                microBatchDF (DataFrame): Incoming micro-batch DataFrame.
                batchId (int): Batch identifier.
        """
        from pyspark.sql.window import Window
        from pyspark.sql.functions import col, rank

        try:
            silver_customers_orders_table = f"{self.env + '.' if self.env else ''}silver.silver_customers_orders"

            # Retrieve the latest customer information.
            # Since the `silver_customers` table contains multiple records due to CDF-enabled data, 
            # we need to select only the most recent customer details based on `_commit_timestamp`.
            window = Window.partitionBy("order_id", "customer_id").orderBy(col("_commit_timestamp").desc())
        

            microBatchDF.filter(col("_change_type").isin(["insert", "update_postimage", "delete"])) \
                    .withColumn("rank", rank().over(window)) \
                    .filter("rank = 1") \
                    .drop("rank", "_commit_version") \
                    .withColumnRenamed("_commit_timestamp", "processed_timestamp") \
                    .createOrReplaceTempView("silver_customers_orders_temp_view")
                    
            # Count the number of records in the current micro-batch.  
            # This helps in tracking the number of records being processed for the upsert operation.  
            source_count = microBatchDF.count()
            logger.info(f"Batch ID {batchId}: Processed microBatchDF with {source_count} records for upsert into silver_customers_orders.")

            # Upsert operation:  
            # - If an update occurs in the `silver_customers` table, the corresponding record in the target table is updated.  
            # - If a delete operation is detected, the corresponding row is removed from the target table.  
            # - Otherwise, new records are inserted into the target table.
                
            query = f""" MERGE INTO {silver_customers_orders_table} t
                            USING silver_customers_orders_temp_view s
                            ON t.order_id=s.order_id AND t.customer_id=s.customer_id
                            WHEN MATCHED AND s.processed_timestamp > t.processed_timestamp AND s._change_type == "update_postimage" THEN
                            UPDATE SET *
                            WHEN MATCHED AND s.processed_timestamp > t.processed_timestamp AND s._change_type == "delete" THEN
                            DELETE
                            WHEN NOT MATCHED THEN 
                            INSERT *
                        """
            microBatchDF.sparkSession.sql(query)
        
        except Exception as e:
            raise UpsertError(msg=e, func_name="customers_orders_upsert")
        

    
    def silver_customers_orders_ingestion(self, processing_time:str="60 seconds", 
                                          availbale_now:bool=False, 
                                          customers_watermark:str = "70 seconds", 
                                          orders_watermark:str="40 seconds",
                                          query_name:str="silver_customers_orders_writter")->DataStreamWriter:

        """
            Starts a streaming job that ingests data from silver customers and orders tables.
            It applies watermarking, performs an inner join, and upserts records into the target table.

            The function loads data from a CDF-enabled table `silver_customers`, joins it with the `silver_orders` 
            stream using a stream-stream join, retains only the latest customer information, and writes the results 
            to the `silver_customers_orders` table through an upsert operation.
            
            Args:
                processing_time (str, optional): Processing time interval for trigger execution.
                availbale_now (bool, optional): Whether to process all available data once.
                customers_watermark (str, optional): Watermark for customer stream (default: "70 seconds").
                orders_watermark (str, optional): Watermark for order stream (default: "40 seconds").
                query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.
            
            Returns:
                DataStreamWriter: Streaming query writer that processes data in micro-batches.
            
            Raises:
                IngestionError: If an error occurs during ingestion.
        """
        
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError

       
        try:
    
            silver_customers_table = f"{self.env + '.' if self.env else ''}silver.silver_customers"
            silver_orders_table = f"{self.env + '.' if self.env else ''}silver.silver_orders"

            # Reads data from the CDF-enabled `silver_customers` table in the silver layer.
            # Since no specific table version is provided, it reads only the latest available changes
            # (i.e., inserts, updates, and deletes) from the change feed. The system automatically 
            # determines the starting version based on available change data.
            customers_df = self.spark.readStream \
                            .option("readChangeFeed", "true") \
                            .table(silver_customers_table) \
                            .withWatermark("row_time", customers_watermark)
                         

            orders_df = self.spark.readStream.table(silver_orders_table) \
                            .withWatermark("order_timestamp", orders_watermark) 

            joined_df = customers_df.join(orders_df, ["customer_id"], "inner")

            w_stream = joined_df.writeStream \
                        .option("checkpointLocation", f"{self.base_checkpoint_path}/silver_customers_orders_cp") \
                        .queryName(query_name)
            
            # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
            # Otherwise, it writes in trigger mode with the specified `processing_time`.
            if availbale_now:
                return w_stream \
                    .foreachBatch(self.customers_orders_upsert) \
                    .trigger(availableNow=True) \
                    .start()
            else:
                return w_stream \
                        .foreachBatch(self.customers_orders_upsert) \
                        .trigger(processingTime=processing_time) \
                        .start()
        
        except AnalysisException as e: 
            msg = f"Error: AnalysisException occured. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silevr_customers_orders", db_name="silver")

        except SparkConnectGrpcException as e:  # For newer versions
            msg = f"Error: parkConnectGrpcException occured. Error message is {e}"
        
        except Py4JJavaError as e: 
            msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_customers_orders", db_name="silver")

        except Exception as e:  
            msg = f"Error: A Python error occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_customers_orders", db_name="silver")