from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession



# Import all required classes from python_package
from python_package.common.error_handler import  IngestionError


class SilverBooksOrdersIngestion:
    """
        A class to ingest data into the `silver_books_orders` table in the silver layer
        by performing a static-stream join between `silver_books` and `silver_orders` tables.

        Attributes:
            base_checkpoint_path (str): Base path for storing streaming checkpoint information.
            env (str, optional): Environment prefix for database and table names (e.g., `dev`, `prod`).
    """
    def __init__(self, spark:SparkSession, base_checkpoint_path:str, env:str=None)->None:
        """
            Initializes the SilverBooksOrdersIngestion class.

            Args:
                spark (SparkSession)
                    The Spark session to interact with Spark SQL.

                base_checkpoint_path (str): 
                    Path for checkpoint storage.

                env (str, optional): 
                    Environment prefix for tables. Defaults to None.
        """
        self.spark = spark
        self.env = env 
        self.base_checkpoint_path = f"{base_checkpoint_path}/silver"


    def silver_books_orders_ingestion(self, processing_time:str="60 seconds", availbale_now:bool=False, outputMode:str="append", query_name:str="silver_books_orders_writter"):
        """
            Performs a streaming ingestion of book order data into the `silver_books_orders` table.

            Args:
                processing_time (str, optional): Processing interval for micro-batch streaming mode. Defaults to "60 seconds".
                availbale_now (bool, optional): If True, processes data in batch mode using `availableNow`. Defaults to False.
                outputMode (str, optional): The output mode for streaming write operation. Defaults to "append".
                query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.

            Returns:
                DataStreamWriter: A streaming writer to process and ingest data into the `silver_books_orders` table.
        """

        from pyspark.sql.functions import col, explode 
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError

        try:

            # Adjusts table names dynamically based on the environment prefix if provided.
            silver_books_table = f"{self.env + '.' if self.env else ''}silver.silver_books"
            silver_orders_table = f"{self.env + '.' if self.env else ''}silver.silver_orders"
            silver_books_orders_table = f"{self.env + '.' if self.env else ''}silver.silver_books_orders"

            books_df = self.spark.table(silver_books_table) \
                .filter(col("current") == True) \
                .select("book_id", "title", "author", "price")

            orders_df = self.spark.readStream.table(silver_orders_table) \
                .withColumn("books", explode(col("books"))) \

            joined_df = orders_df.join(books_df, orders_df.books.book_id == books_df.book_id ) \
                .drop("book_id")

            w_stream = joined_df.writeStream \
                            .option("checkpointLocation", f"{self.base_checkpoint_path}/silver_books_orders_cp") \
                            .queryName(query_name) \
                            .outputMode(outputMode)

            # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
            # Otherwise, it writes in trigger mode with the specified `processing_time`.
            if availbale_now:
                return w_stream \
                    .trigger(availableNow=True) \
                    .toTable(silver_books_orders_table)
            else:
                return w_stream \
                        .trigger(processingTime=processing_time) \
                        .toTable(silver_books_orders_table)

        except AnalysisException as e: 
            msg = f"Error: AnalysisException occured. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silevr_books_orders", db_name="silver")
        
        except SparkConnectGrpcException as e:  # For newer versions
            msg = f"Error: parkConnectGrpcException occured. Error message is {e}"

        except Py4JJavaError as e:  
            msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_books_orders", db_name="silver")

        except Exception as e:  
            msg = f"Error: A Python error occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_books_orders", db_name="silver")