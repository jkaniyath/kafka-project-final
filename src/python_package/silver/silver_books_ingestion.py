from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession


# Import all required classes and function from python_package
from python_package.common.common_functions import remove_duplicate_rows, clean_column_values, extract_and_convert_price
from python_package.common.logger import Logger
from python_package.common.error_handler import UpsertError, IngestionError


logger = Logger().get_logger()


class SilverBooksIngestion:

    """
        SilverBooksIngestion handles the streaming ingestion and upsert process of books data
        from the bronze layer into the silver layer while maintaining Slowly Changing Dimension Type 2 (SCD2).

        Parameters:
        -----------
        spark : SparkSession
            The Spark session to interact with Spark SQL.

        base_checkpoint_path : str
            The base path for checkpoint storage used in streaming queries.
        
        env : str, optional
            The environment context (e.g., `dev`, `prod`). Default is `None`.
        
        Methods:
        --------
        _create_source_df_with_merge_keys(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
            Prepares the source DataFrame with metadata and merge keys for SCD2 upsert operations.
        
        silver_books_upsert(microBatchDF: DataFrame, batch: int) -> None:
            Performs an upsert operation using a merge statement into the `silver_books` table.
        
        silver_books_ingestion(db_name: str, table_name: str, env: str = None, processing_time: str = "60 seconds", availbale_now: bool = False) -> DataStreamWriter:
            Ingests data from the bronze books table to the silver_books table in a streaming manner, applying upsert logic.
    """

    def __init__(self, spark:SparkSession, base_checkpoint_path:str, env:str=None) -> None:
        self.spark = spark
        self.env = env 
        self.base_checkpoint_path = f"{base_checkpoint_path}/silver"

    def _create_source_df_with_merge_keys(self, source_df:DataFrame, target_df:DataFrame) -> DataFrame:

        """
            Prepares the source DataFrame with metadata and merge keys for SCD2 upsert operation.

            This function adds metadata columns (`current`, `end_date`, and `effective_date`) to the source DataFrame, 
            and creates a merge key for joining with the target DataFrame. It performs a left join to identify updated 
            rows in the source DataFrame and prepares the data for insertion or update in the target (e.g., `silver_books` table).

            Parameters:
            ----------
            source_df : DataFrame
                The source DataFrame containing the new or updated data (non-duplicate rows).

            target_df : DataFrame
                The target DataFrame representing the existing data (e.g., the current state of the `silver_books` table).

            Returns:
            -------
            DataFrame
                A DataFrame with metadata columns and merge keys, ready for SCD2 upsert operation.

            Notes:
            ------
            - This function adds `current` as `True` and `end_date` as `None` for the source data.
            - It creates a merge key based on `book_id` for matching rows between source and target.
            - The final DataFrame contains rows from the source with updated metadata and a merge key to perform upsert logic.

            Example Usage:
            -------------
            ```python
            final_df = _create_source_df_with_merge_keys(source_df=source_data, target_df=target_data)
            ```
            This prepares the source data for SCD2 upsert into the target.
        """

        from pyspark.sql.functions import current_date, lit, col 

        source_df_with_meta_data = source_df \
            .withColumn("current", lit(True)) \
            .withColumn("end_date", lit(None)) \
            .withColumnRenamed("updated", "effective_date")

        target_df_to_join = target_df \
                        .withColumnRenamed("book_id", "target_book_id")

        # Left join source and target DataFrames, select all columns from source, 
        # and only "book_id" from target. Updated rows will have "target_book_id" as not null, 
        # while new rows will have "target_book_id" as null. Then add "merge_key" to prepare for upsert operation.
        joined_source_df = source_df_with_meta_data \
            .join(target_df_to_join, (source_df_with_meta_data.book_id==target_df_to_join.target_book_id) & (target_df_to_join.current==True), "left") \
            .select(source_df_with_meta_data["*"], target_df_to_join["target_book_id"]) \
            .withColumn("merge_key", col("book_id"))

        # Creates dummy dataframe for updated rows and will use in upsert opeartion to ingest to target dataframe.
        dummy_df = joined_source_df.filter(col("target_book_id").isNotNull()) \
                    .withColumn("merge_key", lit(None))

        # Append both joined_source_df and dummy dataframe
        final_df = joined_source_df.union(dummy_df)

        return final_df 
    
    def silver_books_upsert(self, microBatchDF, batchId):
        """
            Performs an upsert operation on the `silver_books` table in the Silver layer.

            This function removes duplicate rows from the incoming micro-batch DataFrame (`microBatchDF`)  
            by comparing it against the target Silver table (`silver_books`). It then prepares the data  
            for upsertion by assigning merge keys and executes a `MERGE` SQL operation.

            The `MERGE` operation:
            - Updates existing records by setting `end_date` and marking them as inactive (`current=false`)  
            when a matching book ID is found.
            - Inserts new records when no match is found.

            Args:
                microBatchDF (DataFrame): The incoming micro-batch DataFrame containing new or updated book records.
                batch (int): The batch ID for tracking purposes.

            Raises:
                UpsertError: If an error occurs during the upsert operation.

            Logging:
                Logs the number of records being processed for the given batch.

            Example Usage:
                silver_books_upsert(microBatchDF, batch=123)
        """
        try:
            silver_books_table = f"{self.env + '.' if self.env else ''}silver.silver_books"

            target_df = self.spark.read.table(silver_books_table)
            meta_data_cols = ["current", "effective_date", "end_date"]
            # Remove duplicate rows from source dataframe as compared to target dataframe.
            deduplicated_df = remove_duplicate_rows(source_df=microBatchDF, target_df=target_df, meta_columns=meta_data_cols)

            source_count = deduplicated_df.count()
            logger.info(f"Batch ID {batchId}: Processed microBatchDF with {source_count} records for upsert into {silver_books_table}.")

            # Get datframe with merge keys.
            df = self._create_source_df_with_merge_keys(deduplicated_df, target_df)

            df.createOrReplaceTempView("books_temp_view")
           

            sql_query = f""" 
                            MERGE INTO {silver_books_table} AS t
                            USING books_temp_view AS s 
                            ON s.merge_key=t.book_id AND t.current=true
                            WHEN  MATCHED THEN
                            UPDATE SET end_date=s.effective_date, t.current=false
                            WHEN NOT MATCHED THEN
                            INSERT(book_id, title, author, price, current, effective_date, end_date)
                            VALUES(s.book_id, s.title, s.author, s.price, s.current, s.effective_date, s.end_date)
                        """
            microBatchDF.sparkSession.sql(sql_query)

        except Exception as e:
            raise UpsertError(msg=e, func_name="silver_books_upsert")
        
    def silver_books_ingestion(self, processing_time:str="60 seconds", availbale_now:bool=False, query_name:str="silver_books_writter") -> DataStreamWriter:

        """
            Ingests data from the bronze layer's "books" table, processes the data, and writes it to the silver layer. 

            This method reads the data from the bronze layer, extracts and processes the book data, and writes the processed data 
            into a silver table. It ensures that only distinct records based on "book_id" and "updated" columns are retained. 
            The function supports both batch and streaming ingestion, with configurable processing time or the option to process 
            data as soon as it becomes available.

            Args:
                processing_time (str, optional): The time interval for batch processing in the stream. Defaults to "60 seconds".
                availbale_now (bool, optional): If True, the ingestion process will use the `availableNow` option for stream writing. 
                                                If False, the stream will use the `processingTime` option. Defaults to False.
                query_name (str, optional): A unique identifier for the streaming query. It helps track, monitor, and manage the query execution within Spark Structured Streaming. Defaults to a predefined name if not provided.

            Returns:
                DataStreamWriter: A `DataStreamWriter` object that manages the ingestion process in Spark.

            Raises:
                IngestionError: If there is any error during the ingestion process, an IngestionError is raised with the relevant 
                                database and table name information, and the exception message.

            Example:
                silver_books_ingestion(processing_time="30 seconds", availbale_now=False)
        """

        from pyspark.sql.functions import from_json, col
        from pyspark.sql.utils import AnalysisException
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException
        from py4j.protocol import Py4JJavaError
    
        json_schema =  "book_id STRING, title STRING, author STRING, price STRING, updated TIMESTAMP"

        try:
            # Read the "books" topic from the bronze layer's books table, extract values from the topic, 
            # and remove duplicate rows based on the "book_id" and "updated" columns.
            books_table_naming = f"{self.env + '.' if self.env else ''}bronze.books"

            raw_books_df = self.spark.readStream \
                    .table(books_table_naming) \
                    .withColumn("v", from_json(col("value").cast("string"), json_schema)) \
                    .select("v.*") \
                    .dropDuplicates(["book_id", "updated"]) 

            # Extract the price from the string (e.g., "$55"), remove rows with null values, and convert the price to double format
            books_df_with_extracted_price = extract_and_convert_price(df = raw_books_df, price_column = "price")

            # It will will remove rows with corrupted book_id
            processed_books_df = clean_column_values(df=books_df_with_extracted_price, column_name="book_id") 
                                

            # it will extarct price from string values and returned only not null rows
            w_stream = processed_books_df.writeStream \
                    .option("checkpointLocation", f"{self.base_checkpoint_path}/silver_books_cp") \
                    .queryName(query_name)

            # It writes in batch mode using the `availableNow` stream writer option if `available_now` is True. 
            # Otherwise, it writes in trigger mode with the specified `processing_time`.
            if availbale_now:
                return w_stream \
                    .foreachBatch(self.silver_books_upsert) \
                    .trigger(availableNow=True) \
                    .start()
            else:
                return w_stream \
                        .foreachBatch(self.silver_books_upsert) \
                        .trigger(processingTime=processing_time) \
                        .start()

        except AnalysisException as e: 
            msg = f"Error: AnalysisException occured. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silevr_books", db_name="silver")

        except SparkConnectGrpcException as e:  # For newer versions
            msg = f"Error: parkConnectGrpcException occured. Error message is {e}"

        except Py4JJavaError as e:  
            msg = f"Error: Py4JJavaError (Spark Backend) occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_books", db_name="silver")

        except UpsertError as e:  
            msg = f"Error: Upsert error. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_books", db_name="silver")

        except Exception as e:  
            msg = f"Error: A Python error occurred. Error message is {e}"
            raise IngestionError(msg=msg, table_name="silver_books", db_name="silver")