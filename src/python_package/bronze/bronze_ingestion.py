from pyspark.sql import SparkSession

#Import all required classes from python_package
from python_package.common.common_functions import read_from_kafka, write_stream_to_bronze_table
from python_package.common.error_handler import IngestionError
from python_package.common.config import Config




class BronzeIngestion:
    """
        A class responsible for ingesting data from Kafka topics into bronze tables.
        
        Bronze tables serve as historical tables that store raw, unprocessed data from Kafka.
        This ingestion process enables streaming data from Kafka into bronze tables,
        which can later be processed and refined into silver tables.

        Attributes:
        -----------
        spark : SparkSession
            The Spark session to interact with Spark SQL.
        env : str, optional
            The environment in which the ingestion is running (e.g., 'dev', 'prod').
        bootstrap_server : str
            The Kafka bootstrap server URL used to connect to the Kafka cluster.
        kafka_username : str
            The username for authenticating with the Kafka cluster.
        kafka_password : str
            The password for authenticating with the Kafka cluster.
        base_checkpoint_dir : str
            The base directory where checkpoints will be stored for fault-tolerant streaming.
        
    """

    def __init__(self,spark:SparkSession, bootstrap_server:str, kafka_username:str, kafka_password:str, base_checkpoint_dir:str, env:str=None) -> None:
        self.spark = spark
        self.env = env 
        self.bootstrap_server = bootstrap_server
        self.kafka_username = kafka_username
        self.kafka_password = kafka_password
        self.base_checkpoint_dir = f"{base_checkpoint_dir}/bronze"

    def ingest_to_books_table(self, 
                              topic:str, 
                              outputMode:str, 
                              processing_time:str, 
                              available_now:bool ,
                              query_name:str = "bronze_books_stream_writer"):
        
        """
        Ingests data from the Kafka `books` topic into a bronze table books.

        Args:
            topic (str): The Kafka topic from which to consume data (in this case, "books").
            outputMode (str): The output mode for the streaming write operation (e.g., "append").
            processing_time (str): The time interval at which the stream will be processed (e.g., "1 minute").
            available_now (bool): Flag indicating whether the data is available immediately for processing.
            query_name (str, optional): The name of the streaming query (default is "bronze_books_stream_writer").

        Returns:
            StreamingQuery: The streaming query object that writes to the bronze table.

        Raises:
            IngestionError: If an error occurs during the ingestion process.
        """

        table_name = "books"
        db_name = "bronze"

        try:
            books_df = read_from_kafka( spark=self.spark,
                                        bootstrap_server=self.bootstrap_server, 
                                        user_name=self.kafka_username, 
                                        password=self.kafka_password, 
                                        topic=topic)
            
            
            book_stream_writer = write_stream_to_bronze_table(
                                                    df=books_df,
                                                    env=self.env,
                                                    db_name=db_name, 
                                                    table_name=table_name, 
                                                    outputMode=outputMode,
                                                    processing_time = processing_time,
                                                    available_now= available_now,
                                                    query_name = query_name,
                                                    checkpoint=f"{self.base_checkpoint_dir}/books_cp"
                                                    )
            return book_stream_writer
            
        except Exception as e:
            raise IngestionError(msg=e, table_name=table_name, db_name=db_name)

    def ingest_to_customers_table(self, 
                                  topic:str, 
                                  outputMode:str, 
                                  processing_time:str,
                                  available_now:bool ,
                                  query_name:str = "bronze_customers_stream_writer"):
        """
        Ingests data from the Kafka `customers` topic into a bronze table customers.

        Args:
            topic (str): The Kafka topic from which to consume data (in this case, "customers").
            outputMode (str): The output mode for the streaming write operation (e.g., "append").
            processing_time (str): The time interval at which the stream will be processed (e.g., "1 minute").
            available_now (bool): Flag indicating whether the data is available immediately for processing.
            query_name (str, optional): The name of the streaming query (default is "bronze_customers_stream_writer").

        Returns:
            StreamingQuery: The streaming query object that writes to the bronze table.

        Raises:
            IngestionError: If an error occurs during the ingestion process.
        """

        table_name = "customers"
        db_name = "bronze"

        try:
            customers_df = read_from_kafka( spark=self.spark,
                                            bootstrap_server=self.bootstrap_server, 
                                            user_name=self.kafka_username, 
                                            password=self.kafka_password, 
                                            topic=topic)
            
            customers_stream_writer = write_stream_to_bronze_table(
                                            df=customers_df,
                                            env=self.env,
                                            db_name=db_name, 
                                            table_name=table_name, 
                                            outputMode=outputMode,
                                            available_now=available_now,
                                            processing_time = processing_time,
                                            query_name = query_name,
                                            checkpoint=f"{self.base_checkpoint_dir}/customers_cp"
                                        )
            
            return customers_stream_writer

            
        except Exception as e:
            raise IngestionError(msg=e, table_name=table_name, db_name=db_name)

    def ingest_to_orders_table(self, topic:str, 
                               outputMode:str, 
                               processing_time:str,
                               available_now:bool,
                               query_name:str = "bronze_orders_stream_writer"
                               ):
        
        """
        Ingests data from the Kafka `orders` topic into a bronze table orders.

        Args:
            topic (str): The Kafka topic from which to consume data (in this case, "orders").
            outputMode (str): The output mode for the streaming write operation (e.g., "append").
            processing_time (str): The time interval at which the stream will be processed (e.g., "1 minute").
            available_now (bool): Flag indicating whether the data is available immediately for processing.
            query_name (str, optional): The name of the streaming query (default is "bronze_orders_stream_writer").

        Returns:
            StreamingQuery: The streaming query object that writes to the bronze table.

        Raises:
            IngestionError: If an error occurs during the ingestion process.
        """

        table_name = "orders"
        db_name = "bronze"

        try:
            orders_df = read_from_kafka(spark=self.spark,
                                        bootstrap_server=self.bootstrap_server, 
                                        user_name=self.kafka_username, 
                                        password=self.kafka_password, 
                                        topic=topic)
            
            orders_stream_writer = write_stream_to_bronze_table(
                                                df=orders_df,
                                                env=self.env,
                                                db_name=db_name, 
                                                table_name=table_name, 
                                                outputMode=outputMode,
                                                processing_time = processing_time,
                                                available_now=available_now,
                                                query_name = query_name,
                                                checkpoint=f"{self.base_checkpoint_dir}/orders_cp"
                                            )
            return orders_stream_writer
        
        except Exception as e:
            raise IngestionError(msg=e, table_name=table_name, db_name=db_name)

    