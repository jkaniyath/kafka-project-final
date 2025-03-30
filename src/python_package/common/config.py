from python_package.common.error_handler import ConfigError
from pyspark.dbutils import DBUtils



class Config:
    """
        A configuration class for managing Spark sessions and retrieving external storage locations.

        This class initializes a Spark session, preferring Databricks Connect if available.
        It also provides methods to fetch external storage locations (Bronze, Silver, Gold, and Checkpoints)
        from Unity Catalog.

        Attributes:
            spark (SparkSession): The active Spark session.
            dbutils (DBUtils) : Databricks utility object for interacting with the file system, secrets, widgets, and more.
    """

    def __init__(self):
        """
            Initializes the Config class by creating a Spark session.
        """
        self.spark = self.get_spark()
        self.dbutils = DBUtils(spark=self.spark)
      

    @staticmethod
    def get_spark():
        """
            Initializes and returns a Spark session.

            - If running on Databricks Connect, it initializes a `DatabricksSession`.
            - Otherwise, it initializes a standard `SparkSession`.

            Returns:
                SparkSession: The active Spark session.
        """

        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.getOrCreate()
            return spark
        except ImportError:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            return spark
        
    
    def get_bronze_external_location(self):
        """
            Retrieves the external storage location for the Bronze layer.

            Returns:
                str: The URL of the Bronze external location.

            Raises:
                ConfigError: If the location cannot be retrieved.
        """
        try:
            bronze_location = self.spark.sql("DESCRIBE EXTERNAL LOCATION bronze").select("url").collect()[0][0]
            return bronze_location
        except Exception as e:
            raise ConfigError(e)
    
    def get_silver_external_location(self):
        """
            Retrieves the external storage location for the Silver layer.

            Returns:
                str: The URL of the Silver external location.

            Raises:
                ConfigError: If the location cannot be retrieved.
        """

        try:
            silver_location = self.spark.sql("DESCRIBE EXTERNAL LOCATION silver").select("url").collect()[0][0]
            return silver_location
        except Exception as e:
            raise ConfigError(e)
    
    def get_gold_external_location(self):
        """
            Retrieves the external storage location for the Gold layer.

            Returns:
                str: The URL of the Gold external location.

            Raises:
                ConfigError: If the location cannot be retrieved.
        """

        try:
            gold_location = self.spark.sql("DESCRIBE EXTERNAL LOCATION gold").select("url").collect()[0][0]
            return gold_location
        except Exception as e:
            raise ConfigError(e)
    
    def get_checkpoint_external_location(self):
        """
            Retrieves the external storage location for the Checkpoint location.

            Returns:
                str: The URL of the Checkpoint external location.

            Raises:
                ConfigError: If the location cannot be retrieved.
        """
        try:
            checkpoint_location = self.spark.sql("DESCRIBE EXTERNAL LOCATION checkpoints").select("url").collect()[0][0]
            return checkpoint_location
        except Exception as e:
            raise ConfigError(e)
        
    def get_landing_zone(self):

        """
            Retrieves the landing zone URL, which is the external location for storing data.

            This function executes a Spark SQL query to describe the external location named 
            'landing_zone' and extracts the URL associated with it.

            Returns:
                str: The URL of the landing zone.

            Raises:
                ConfigError: If an error occurs while retrieving the external location.

        """
        try:
            landing_zone = self.spark.sql("DESCRIBE EXTERNAL LOCATION landing").select("url").collect()[0][0]
            return landing_zone
        except Exception as e:
            raise ConfigError(e)
        
    def get_logging_zone(self):

        """
            Retrieves the logging zone URL, which is the external location for storing log data.

            This function executes a Spark SQL query to describe the external location named 
            'logger' and extracts the URL associated with it.

            Returns:
                str: The URL of the landing zone.

            Raises:
                ConfigError: If an error occurs while retrieving the external location.

        """
        try:
            logging_zone = self.spark.sql("DESCRIBE EXTERNAL LOCATION logger").select("url").collect()[0][0]
            return logging_zone
        except Exception as e:
            raise ConfigError(e)