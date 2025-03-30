from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Dict
from typing import List 

def create_db(spark:SparkSession, db_name:str, location:str, env:str=None) -> None:
    """
        Creates a database in the specified Unity Catalog or default catalog and sets it as the active database.

        If the database does not exist, it is created with the specified managed storage location.
        
        Args:
            spark (SparkSession): The active Spark session.
            db_name (str): The name of the database to create.
            location (str): The managed storage location for the database.
            env (str, optional): The catalog/environment name. If provided, the database will be created within this catalog.

        Returns:
            None

        Example:
            ```python
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder.appName("DatabaseCreation").getOrCreate()
            create_db(spark, "bronze", "abfss://medallion@storageaccount.dfs.core.windows.net/bronze", "dev")
            ```

        Notes:
            - If `env` is provided, the database is created inside `env` (e.g., `dev.bronze`).
            - If `env` is `None`, the database is created in the default catalog.
            - The function executes `CREATE DATABASE IF NOT EXISTS` to ensure no duplicate creation.
            - The `USE` statement sets the created database as the current active database.
    """

    catalog = f"{env + '.' if env else ''}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}{db_name} MANAGED LOCATION '{location}'")
    spark.sql(f"USE {catalog}{db_name}")


from pyspark.sql import DataFrame

def read_from_kafka(spark:SparkSession, bootstrap_server:str, user_name:str, password:str, topic:str) -> DataFrame:
    """
        Reads a streaming DataFrame from a Kafka topic using PySpark Structured Streaming.

        Args:
            bootstrap_server (str): The Kafka bootstrap server address (e.g., "localhost:9092").
            user_name (str): The username for Kafka authentication.
            password (str): The password for Kafka authentication.
            topic (str): The name of the Kafka topic to subscribe to.

        Returns:
            pyspark.sql.DataFrame: A streaming DataFrame containing messages from the specified Kafka topic.

        Example:
            ```python
            df = read_from_kafka(
                bootstrap_server="broker:9092",
                user_name="my_user",
                password="my_password",
                topic="my_topic"
            )
            df.printSchema()
            ```

        Notes:
            - This function uses SASL_SSL with PLAIN authentication.
            - The `startingOffsets` option is set to "earliest" to consume all available messages from the beginning.
            - The DataFrame returned is a streaming DataFrame and must be processed using structured streaming operations.

    """
    jass_module = "org.apache.kafka.common.security.plain.PlainLoginModule"

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", f"{jass_module } required username='{user_name}' password='{password}';") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() 

    return df 
 


def write_stream_to_bronze_table(df:DataFrame, 
                                db_name:str, 
                                table_name:str, 
                                outputMode:str, 
                                checkpoint:str, 
                                query_name:str,  
                                available_now:bool,
                                processing_time:str = "30 seconds",
                                env:str=None):
    
    """
        Writes a streaming DataFrame to a Delta table in the bronze layer.

        This function writes a Spark Structured Streaming DataFrame to a Delta table with 
        the specified output mode and checkpoint location. The table is created in either 
        a two-level (`bronze.table_name`) or three-level (`env.bronze.table_name`) namespace 
        based on the `env` parameter.

        Args:
            df (DataFrame): The streaming DataFrame to be written.
            db_name (str): The database (schema) name where the table resides.
            table_name (str): The target table name.
            outputMode (str): The output mode for streaming ("append", "complete", or "update").
            checkpoint (str): The checkpoint location for managing state.
            query_name (str): The name assigned to the streaming query.
            available_now (bool, optional): If `True`, uses `availableNow=True` for micro-batch execution. Defaults to `False`.
            processing_time (str, optional): The interval at which batches are processed (e.g., "30 seconds"). Defaults to `"30 seconds"`.
            env (str, optional): If provided, the table is created in a three-level namespace (`env.db_name.table_name`). Defaults to `None`.

        Returns:
            StreamingQuery: The streaming query that writes to the Delta table.

        Example:
            ```python
            query = write_stream_to_bronze_table(
                df=streaming_df, 
                db_name="bronze", 
                table_name="customer_data", 
                outputMode="append", 
                checkpoint="dbfs:/checkpoints/customer", 
                query_name="customer_stream", 
                available_now=False,
                processing_time="15 seconds",
                env="dev"
            )
            ```

        Notes:
            - The function ensures a valid checkpoint location is used to allow fault tolerance.
            - If `available_now=True`, Spark processes all available data in one trigger.
            - If `available_now=False`, Spark processes data continuously in micro-batches.
    """

    # If env is given then create table in 3 level name space other wise 2 level name space.
    table_name = f'{env + "." if env else ""}{db_name}.{table_name}' 
    
    stream_writer = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint) \
        .queryName(query_name) \
        .outputMode(outputMode) 
    
    if available_now:
        return stream_writer \
                .trigger(availableNow=True) \
                .toTable(table_name)
    else:
        return stream_writer \
                .trigger(processingTime=processing_time) \
                .toTable(table_name)
        
def get_kafka_serets(spark:SparkSession)-> Dict[str, str]:

    """
        Retrieves Kafka-related secrets from the Databricks secret scope.

        This function uses the Databricks DBUtils to fetch Kafka connection details from 
        the specified secret scope. The secrets include the Kafka bootstrap server, 
        SASL username, and SASL password.

        Args:
            spark (SparkSession): The active Spark session used to initialize DBUtils.

        Returns:
            Dict[str, str]: A dictionary containing the Kafka secrets, where keys are 
                            the secret names (e.g., 'bootstrap_server', 'sasl_username', 
                            'sasl_password') and values are the secret values.
    
    """

    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    BOOTSTRAP_SERVER = dbutils.secrets.get(scope="kafka-secrets", key="BOOTSTRAP_SERVER")
    SASL_USERNAME = dbutils.secrets.get(scope="kafka-secrets", key="SASL_USERNAME")
    SASL_PASSWORD = dbutils.secrets.get(scope="kafka-secrets", key="SASL_PASSWORD")

    return ({"bootstrap_server":BOOTSTRAP_SERVER, "sasl_username":SASL_USERNAME, "sasl_password":SASL_PASSWORD})




def get_silver_table_schemas() -> List[str]:

    """
        Returns the schema definitions for Silver tables in a Delta Lake architecture.

        These schemas define the structure of different datasets: books, customers, and orders.

        Returns:
            List[str]: A list of schema strings formatted for Spark SQL.

        Schemas:
            - books_schema: Defines columns for book details, including price, author, and validity period.
            - customers_schema: Defines customer details like name, email, address, and timestamp.
            - orders_schema: Defines order details including a nested array of book order information.

        Example:
            >>> schemas = get_silver_table_schemas()
            >>> print(schemas[0])  # Prints books schema
    """

    books_schema = "book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP"

    customers_schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_status STRING, row_time TIMESTAMP"

    orders_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

    customers_orders_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, _change_type  STRING, processed_timestamp TIMESTAMP"

    books_sales_schema = "order_id STRING, order_timestamp TIMESTAMP, customer_id STRING, quantity BIGINT, total BIGINT, books STRUCT<book_id: STRING, quantity: BIGINT, subtotal: BIGINT>, title STRING, author STRING, price DOUBLE"

    return [books_schema, customers_schema, orders_schema, customers_orders_schema, books_sales_schema]


from typing import List 
def create_delta_table(spark:SparkSession,  table_name:str, db_name:str, schema:str, env:str=None, liquid_clustering_enabled:bool=False, cluster_columns:List[str]=[], is_cdf_enabled:bool=False):
    """
        Creates a Delta table in a specified database using Spark SQL.

        Parameters:
        -----------
        table_name : str
            The name of the table to be created.
        db_name : str
            The name of the database where the table should be created.
        schema : str
            The schema definition of the table in SQL format (e.g., "id INT, name STRING").
        env : str, optional
            An optional environment prefix (e.g., "dev", "prod") that, if provided, 
            will prepend the database name in the table creation command.

        Behavior:
        ---------
        - If `env` is provided, the table will be created under `{env}.{db_name}.{table_name}`.
        - If `env` is not provided, the table will be created under `{db_name}.{table_name}`.
        - Uses `CREATE TABLE IF NOT EXISTS` to ensure the table is only created if it does not already exist.

        Example Usage:
        --------------
        ```python
        create_delta_tables("employees", "company_db", "id INT, name STRING")
        create_delta_tables("customers", "sales_db", "id INT, email STRING", env="dev")
        ```

        Notes:
        ------
        - Ensure that SparkSession (`spark`) is available before calling this function.
        - Delta Lake must be enabled in the Spark environment for proper functionality.
    """
    table_naming = f"{env + '.' if env else ''}{db_name}.{table_name}"
    enable_cdf = f"{'TBLPROPERTIES (delta.enableChangeDataFeed = true)' if is_cdf_enabled else ''}"
 

    # You need Databricks Runtime 14.2 and above to run following command
    if liquid_clustering_enabled:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_naming} ({schema}) CLUSTER BY ({','.join(cluster_columns)}) {enable_cdf}")
    else:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_naming} ({schema}) {enable_cdf}")


def remove_duplicate_rows(source_df:DataFrame, target_df:DataFrame, meta_columns:list["str"]) -> DataFrame:

    """
        Compares the source and target DataFrames and removes duplicate rows from the source DataFrame.

        This function generates a hash of the rows (excluding specified metadata columns) from both the source 
        and target DataFrames using the MD5 hash function. It performs a **left anti join** between the source 
        and target DataFrames to remove rows in the source that already exist in the target (based on hash).

        Parameters:
        ----------
        source_df : DataFrame
            The source DataFrame containing the new or updated data that needs to be compared against the target DataFrame.

        target_df : DataFrame
            The target DataFrame that contains the existing data to which the source DataFrame will be compared.

        meta_columns : list[str]
                These columns typically contain metadata (e.g., timestamps, unique identifiers, or flags like `start_date`, `end_date`, and `is_active`) 
            that shouldn't be used to detect duplicates, particularly in Slowly Changing Dimension (SCD2) tables.

        Returns:
        -------
        DataFrame
            A DataFrame containing the rows from the source DataFrame that do not have corresponding matches 
            in the target DataFrame (i.e., rows that are considered unique based on the non-metadata columns).

        Notes:
        ------
        - The function creates a hash by concatenating the values of all columns (except the metadata columns) 
            and applying the MD5 hash function to detect duplicates.
        - The final result DataFrame contains only the rows from the source DataFrame that do not have an exact match 
            in the target DataFrame, based on the non-metadata columns.
        - This function uses the **left anti join** to filter out the duplicate rows from the source DataFrame.

        Example Usage:
        -------------
        ```python
        unique_rows = remove_duplicate_rows(source_df=source_data, target_df=target_data, meta_columns=["start_date", "end_date", "is_active"])
        ```

        This will return a DataFrame with only those rows from `source_data` that do not exist in `target_data`,
        ignoring the `start_date`, `end_date`, and `is_active` columns in target  datframe during comparison.

    """

    from pyspark.sql.functions import md5, lit, concat_ws

    target_df_columns = [x for x in target_df.columns if x not in meta_columns]
    target_df_without_meta_cols = target_df.select(target_df_columns)

    final_target_df = target_df_without_meta_cols.withColumn("hash", md5(concat_ws("|", *[c for c in target_df_columns])))
    final_source_df = source_df.withColumn("hash", md5(concat_ws("|", *[c for c in target_df_columns])))

    result = final_source_df.join(final_target_df, final_source_df.hash==final_target_df.hash, "left_anti") \
                .drop("hash")

    return result


def extract_and_convert_price(df:DataFrame, price_column:str) -> DataFrame:
    """
        Extracts the first numeric value (integer or decimal) from a string column and converts it to a double type.

        Parameters:
        -----------
        df : DataFrame
            The input PySpark DataFrame containing the price column.
        price_column : str
            The name of the column containing price information as a string.

        Returns:
        --------
        DataFrame
            A new DataFrame with the extracted numeric values converted to double,
            and rows with null or invalid prices filtered out.

        Notes:
        ------
        - The function extracts the first numeric value, including both integer and decimal numbers, from the price column.
        - If no numeric value is found, the row is removed from the output.
        - It assumes the price is embedded within a string, which can include currency symbols, text, etc.
        
        Example:
        --------
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("Example").getOrCreate()
        >>> data = [("Price: $123",), ("Cost is 45 USD",), ("$67.89",), ("No price",), ("Price: 2000",)]
        >>> df = spark.createDataFrame(data, ["price_text"])
        >>> result = extract_and_convert_price(df, "price_text")
        >>> result.show()
        +-----------+
        | price_text|
        +-----------+
        |     123.0 |
        |      45.0 |
        |      67.89|
        |     2000.0|
        +-----------+
    """
    from pyspark.sql.functions import regexp_extract, col 
    result = df.withColumn(price_column,  regexp_extract(price_column,  r"(\d+\.\d+|\d+)", 1).cast("double") ) \
                .filter(col(price_column).isNotNull())

    return result

def clean_column_values(df:DataFrame, column_name:str) -> DataFrame:

    """
        Cleans a specified column in a PySpark DataFrame by removing rows where the column value is:
        - "null" (case-insensitive)
        - "none" (case-insensitive)
        - Empty strings or values consisting only of whitespace

        Args:
            df (DataFrame): Input PySpark DataFrame.
            column_name (str): The name of the column to clean.

        Returns:
            DataFrame: A new DataFrame with the specified column filtered for valid values.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.getOrCreate()
            >>> data = [("Alice",), ("  ",), ("None",), ("null",), ("Bob",)]
            >>> df = spark.createDataFrame(data, ["name"])
            >>> df_cleaned = clean_column_values(df, "name")
            >>> df_cleaned.show()
            +-----+
            | name|
            +-----+
            |Alice|
            |  Bob|
            +-----+
    """

    from pyspark.sql.functions import col, when, trim , lower

    result = df.filter((lower(col(column_name))!="null") & (lower(col(column_name))!="none") & (trim(col(column_name)) != "") )

    return result 


def extract_books_subtotal(df:DataFrame) -> DataFrame:

    """
        Extracts and cleans the 'subtotal' values from the 'books' array column in an orders DataFrame.

        This function performs the following operations:
        1. **Filters out rows where the 'books' column is empty** (i.e., has size 0).
        2. **Explodes the 'books' column**, converting array elements into separate rows.
        3. **Extracts numeric values from the 'subtotal' field**, removing any non-numeric characters (e.g., "$50" → "50"),
        and converts it to a `double` data type.
        4. **Reconstructs the original schema**, grouping the transformed books back into an array.

        Parameters:
            df (DataFrame): The input Spark DataFrame containing order details with an array of books.

        Returns:
            DataFrame: A transformed DataFrame with cleaned 'subtotal' values while maintaining the original schema.

        Example Usage:
        --------------
        ```python
        processed_df = extract_books_subtotal(orders_df)
        processed_df.show(truncate=False)
        ```

        Expected Schema After Transformation:
        -------------------------------------
        - order_id: string
        - order_timestamp: timestamp
        - customer_id: string
        - quantity: long
        - total: double
        - books: array<struct<book_id: string, quantity: long, subtotal: double>>
    """
    

    from pyspark.sql.functions import  explode, collect_list, col , size, struct, regexp_replace

    # Step 1: Filter out rows where 'books' is empty, then explode the 'books' array for further processing
    exploded_df = df.filter(size(col("books"))!=0) \
                    .withColumn("books",  explode(col("books"))) 

    # Step 2: Extract numeric values from 'subtotal' and convert to double
    df_with_processed_subtotal = exploded_df.withColumn("books", 
                                      struct(col("books.book_id"), col("books.quantity") ,
                                             regexp_replace(col("books.subtotal"), "[^0-9]", "") \
                                            .cast("double").alias("subtotal")))
    
    # Step 3: Reconstruct the original schema format with aggregated 'books' array
    extracted_books_df = df_with_processed_subtotal \
                            .groupBy("order_id","order_timestamp", "customer_id", "quantity", "total") \
                            .agg(collect_list("books").alias("books"))

    return  extracted_books_df

  

    