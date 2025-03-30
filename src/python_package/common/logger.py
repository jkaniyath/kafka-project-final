import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.types import StructType, StructField, StringType,TimestampType,IntegerType
from pyspark.sql import SparkSession


from python_package.common.config import Config

config = Config()
spark = config.spark
logger_db_location = config.get_logging_zone()

class CustomHandler(logging.Handler):
    """
    A custom logging handler that writes log records to a Delta table in Databricks.

    This handler creates a managed schema (if it does not exist) and appends log records 
    to a specified Delta table. The log entries include log level, line number, message, 
    and timestamp.

    Attributes:
        table_name (str): The name of the Delta table to store logs.
        db_name (str): The name of the database/schema where the table resides.
        timestamp_zone (str): The timezone for log timestamps.
        env (str): The environment name (e.g., "dev", "prod") used for schema naming.
        spark (SparkSession): The active Spark session.
        db_location (str): The location where the database/schema is stored.
        schema_name (str): The fully qualified schema name (includes environment if provided).
        schema (StructType): The schema definition for the log records.

    Methods:
        emit(record):
            Writes a log record to the Delta table.
    """

    def __init__(self,
                 table_name:str, 
                 db_name:str, 
                 timestamp_zone, 
                 env:str, 
                 spark:SparkSession,
                 db_location:str):

        super().__init__()
        self.spark = spark
        self.timestamp_zone = timestamp_zone
        self.env = env
        self.db_name = db_name
        self.db_location = db_location
        self.table_name = table_name
        self.schema_name = f"{env + '.' if env else ''}{db_name}"
        self.schema = StructType([
                    StructField("levelname", StringType(), True),
                    StructField("lineno", IntegerType(), True),
                    StructField("msg", StringType(), True),
                    StructField("created_timestamp", TimestampType(), True)
            ])
        
    def emit(self,record):
        """
        Writes a log record to the Delta table.

        This method creates the schema if it does not exist, formats the log record, 
        and appends it to the Delta table.

        Args:
            record (logging.LogRecord): The log record to be written.
        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name} MANAGED LOCATION '{self.db_location}'")
        log_table_name = f"{self.schema_name}.{self.table_name}"
        
        if record:
            # Get the local timestamp as per the given region.
            local_time = datetime.fromtimestamp(record.created, tz=ZoneInfo(self.timestamp_zone))
            logs = []
            logs.append((record.levelname,record.lineno,record.msg,local_time))
            logs_df = spark.createDataFrame(data=logs, schema=self.schema)
            logs_df.write.mode("append").saveAsTable(log_table_name)


class Logger: 
    """
    A custom logger class for logging messages to a Delta table in Databricks.

    This logger initializes a logging instance with a custom handler (`CustomHandler`) 
    that writes logs to a specified Delta table. It ensures that only one handler 
    is attached at a time.

    Attributes:
        spark (SparkSession): The active Spark session.
        logger_name (str): The name of the logger instance.
        table_name (str): The name of the Delta table where logs are stored.
        db_name (str): The name of the database/schema containing the table.
        db_location (str): The managed location of the database/schema.
        env (str): The environment name (e.g., "dev", "prod") used for schema naming.
        timestamp_zone (str): The timezone for log timestamps.

    Methods:
        get_logger():
            Initializes and returns a logger instance with a custom handler.
    """
    def __init__(self, 
                 spark:SparkSession = spark,
                 logger_name:str="KafkaBricks_Logger", 
                 table_name:str = "kafka_bricks_logger",
                 db_name:str = "logger",
                 db_location:str = logger_db_location,
                 env:str = "dev",
                 timestamp_zone:str = "Europe/Riga") -> None:
        self.spark = spark
        self.logger_name = logger_name
        self.timestamp_zone = timestamp_zone
        self.table_name = table_name
        self.db_name = db_name
        self.db_location = db_location
        self.env = env

    def get_logger(self):
        """
        Initializes and returns a logger instance with a custom handler.

        This method sets up a logger with the given name and attaches a 
        `CustomHandler` to write logs to the Delta table. If handlers already 
        exist, they are cleared before adding the custom handler.

        Returns:
            logging.Logger: The configured logger instance.
        """
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logging.DEBUG)

        customhandler = CustomHandler(spark=self.spark,
                                    table_name=self.table_name, 
                                    db_name='logger', 
                                    db_location=self.db_location,
                                    timestamp_zone = "Europe/Riga",
                                    env=self.env)
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.addHandler(customhandler)

        return logger