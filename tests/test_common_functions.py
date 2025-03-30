import sys
import os
import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType
from chispa.dataframe_comparer import assert_df_equality



from src.python_package.common.common_functions import remove_duplicate_rows, extract_and_convert_price, clean_column_values



# Get the spark session
@pytest.fixture(scope="module")
def spark()->SparkSession:
    try:
      from databricks.connect import DatabricksSession
      return DatabricksSession.builder.serverless(True).getOrCreate() 
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def test_remove_duplicate_rows(spark):
    """Test that remove_duplicate_rows correctly filters out duplicates based on hash."""

    # Create test data for source DataFrame
    source_data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie")  # Unique row
    ]

    # Create test data for target DataFrame (Alice, Bob are a duplicate rows)
    target_data = [
        (1, "Alice", datetime.datetime.strptime('2021-01-01', "%Y-%m-%d").date(), datetime.datetime.strptime('2023-01-01', "%Y-%m-%d").date(), True),
        (2, "Bob", datetime.datetime.strptime('2022-01-01', "%Y-%m-%d").date(), datetime.datetime.strptime('2024-01-01', "%Y-%m-%d").date(), True)
    ]

    expected_data = [
         (3, "Charlie") 
    ]
    

    source_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    target_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_active", BooleanType(), True)
    ])



    # Create DataFrames
    source_df = spark.createDataFrame(source_data, schema=source_schema)
    target_df = spark.createDataFrame(target_data, schema=target_schema)
    expected_df = spark.createDataFrame(expected_data, schema=source_schema)

    # Define metadata columns
    meta_columns = ["start_date", "end_date", "is_active"]

    # Call the function
    result_df = remove_duplicate_rows(source_df=source_df, target_df=target_df, meta_columns=meta_columns)


    assert_df_equality(result_df, expected_df)


def test_extract_and_convert_price(spark):
    """ Extracts the first integer value from a string column and converts it to a double type."""

    # Define schemas
    test_schema = StructType([
        StructField("price_details", StringType(), True),
        StructField("book_id", IntegerType(), True),
        StructField("book_name", StringType(), True)
    ])

    expected_schema = StructType([
        StructField("price_details", DoubleType(), True),
        StructField("book_id", IntegerType(), True),
        StructField("book_name", StringType(), True)
    ])

    expected_data = [
        (123.0, 1, "Book A"),
        (45.0, 2, "Book B"),
        (67.89, 3, "Book C"),
        (100.0, 4, "Book D"),
        (2000.0, 5, "Book E")
    ]


    test_data = [
        ("$123", 1, "Book A"),
        ("45 USD", 2, "Book B"),
        ("$67.89", 3, "Book C"),
        ("€100", 4, "Book D"),
        ("$2000", 5, "Book E")
    ]

    # Create DataFrames
    test_df = spark.createDataFrame(test_data, schema=test_schema)
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    result_df = extract_and_convert_price(df=test_df, price_column="price_details")

    assert_df_equality(result_df, expected_df)


def test_clean_column_values(spark):

    # Test 1: Clean column with valid and invalid values
    data = [("Alice",), ("  ",), ("None",), ("null",), ("Bob",)]

    schema = StructType([
        StructField("name", StringType(), True)
    ])


    df = spark.createDataFrame(data, schema=schema)

    df_cleaned = clean_column_values(df, "name")

    expected_data = [("Alice",), ("Bob",)]
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Assert the cleaned DataFrame matches the expected result
    assert_df_equality(df_cleaned, expected_df)


    # Test 2: Column contains only invalid values
    data_invalid = [("null",), ("None",), ("  ",)]
    df_invalid = spark.createDataFrame(data_invalid, schema=schema)

    df_cleaned_invalid = clean_column_values(df_invalid, "name")
    
    # Expecting an empty DataFrame after cleaning
    assert df_cleaned_invalid.count() == 0

    # Test 3: Column contains only valid values
    data_valid = [("Alice",), ("Bob",)]
    df_valid = spark.createDataFrame(data_valid, schema=schema)

    df_cleaned_valid = clean_column_values(df_valid, "name")

    # Expecting the same DataFrame as input
    assert_df_equality(df_cleaned, df_valid)
    


    


