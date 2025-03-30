from pyspark.sql import SparkSession


class CountriesIngestion:

    """
        A class for ingesting country-related data from an external source into a static lookup table.

        This class reads country-related data from a CSV file stored in an Azure Data Lake 
        and writes it to a Delta table (`bronze.country_lookup`). It supports an optional 
        environment parameter (`env`) that follows a three-layer naming convention 
        (e.g., `dev.bronze.country_lookup` or `prod.bronze.country_lookup`).

        Attributes:
        ----------
        spark : SparkSession
            The active Spark session used for reading and writing data.

        env : str, optional
            The environment name (e.g., "dev", "prod"). If provided, it is used 
            to construct the table name using the three-layer naming convention.

        Methods:
        -------
        ingest_to_country_lookup(path: str, mode: str = "overwrite") -> None
            Reads country-related data from a CSV file and ingests it into 
            the `bronze.country_lookup` Delta table.
    """


    def __init__(self, spark:SparkSession, env:str=None):
        
        """
            Initializes the CountriesIngestion class.

            Parameters:
            ----------
            spark : SparkSession
                The active Spark session used for all Spark operations.

            env : str, optional
                The environment name (e.g., "dev", "prod"). It is used to construct 
                the table name for ingestion.
        """

        self.spark = spark
        self.env = env 

    def ingest_to_country_lookup(self, path:str, mode:str="overwrite")-> None:

        """
            Ingests data from a CSV file into the static country lookup table.

            This function reads country-related data from a CSV file stored in a data lake and writes it 
            to a Delta table (`bronze.country_lookup`). It follows a three-layer naming convention 
            if the `env` parameter is specified (e.g., `dev.bronze.country_lookup` or `prod.bronze.country_lookup`).

            Parameters:
            ----------
            mode : str
                Specifies the write mode for the ingestion process. Supported modes include:
                - "overwrite": Overwrites existing data.
                - "append": Appends new data to the table.
                - "error" or "errorifexists": Throws an error if the table already exists.
                - "ignore": Ignores the operation if the table exists.

            path : str
                The file path in the data lake where the CSV file is located.


            Notes:
            ------
            - The CSV file must have a header row and follow the expected schema:
                * calling_code (STRING): The international calling code of the country.
                * code (STRING): The country code (e.g., "US", "IN").
                * country (STRING): The full name of the country.
            - The function uses Spark's DataFrame API to read the CSV file and write it as a Delta table.

            Example Usage:
            -------------
            ```python
            obj.ingest_to_country_lookup(mode="overwrite", path="s3://datalake/country_data.csv")
            ```
            This will load the CSV data into `dev.bronze.country_lookup` with overwrite mode.
        """

        # If env(for example dev, or pord) then it follow 3 layer naming convention.
        country_lookup_table = f"{self.env + '.' if self.env else ''}bronze.country_lookup"
        schema = "calling_code STRING, code STRING, country STRING"

        self.spark \
            .read \
            .format("csv") \
            .schema(schema) \
            .option("header", True) \
            .load(path) \
            .write \
            .mode(mode) \
            .saveAsTable(country_lookup_table)