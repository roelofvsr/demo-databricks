"""This module contains utility functions for reading and writing data to Azure
Blob Storage and cleaning DataFrames."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import FloatType, StructType

spark = SparkSession.builder.getOrCreate()


def read_from_blob_wasbs(
    storage_account_name,
    storage_account_access_key,
    blob_container,
    file_name,
    fileformat,
):
    """Reads a file from Azure Blob Storage into a Spark DataFrame.

    storage_account_name:       str
    storage_account_access_key: str
    blob_container:             str
    file_name:                  str
    fileformat:                 str, e.g. 'csv', 'parquet', 'delta'
    """
    read_file_path = f"wasbs://{blob_container}@{storage_account_name}.blob.core.windows.net/{file_name}"
    df = spark.read.format(fileformat).load(
        read_file_path, inferSchema=True, header=True
    )
    return df


def save_dataframe_to_blob_wasbs(
    container: str,
    name: str,
    df: DataFrame,
    layer: str,
    storage_account_name: str,
    fileformat: str,
):
    """Saves a DataFrame to Azure Blob Storage in specific fileformat.

    container:              str
    name:                   str
    df:                     pySpark DataFrame
    layer:                  str
    storage_account_name:   str
    fileformat:             str, e.g. 'csv', 'parquet', 'delta'
    """
    # check that we write to the correct layer
    assert layer in container

    # Construct blob container name and file path
    blob_container = container
    name = name
    save_file_path = (
        f"wasbs://{blob_container}@{storage_account_name}.blob.core.windows.net/{name}"
    )

    # Save the DataFrame
    df.write.format(fileformat).mode("overwrite").option("header", "true").save(
        save_file_path
    )


def clean_dataframe(
    df: DataFrame, schema: StructType, decimal_places: int
) -> DataFrame:
    """Cleans the DataFrame by correcting column names, replacing 'O' with '0',
    dropping NA values, applying a schema, and rounding float columns to a
    specified number of decimal places.

    df: pySpark DataFrame
    schema: pySpark StructType
    decimal_places: int
    """
    # Correct column names
    df = df.select([spark_col(column).alias(column.lower()) for column in df.columns])

    # Replace 'O' with '0' and cast to float
    df = df.select(
        [
            regexp_replace(spark_col(column), "o", "0").cast("float").alias(column)
            for column in df.columns
        ]
    )

    # Drop NA and print dropped rows statistics
    row_count_before = df.count()
    df = df.dropna()
    row_count_after = df.count()
    print(f"Dropped rows: {row_count_before - row_count_after}")
    print(
        f"Percentage dropped: {round(((row_count_before - row_count_after) / row_count_before) * 100, 2)}%"
    )

    # Apply the provided schema
    df = df.rdd.toDF(schema)

    # Round to the specified number of decimal places
    df = df.select(
        [
            (
                spark_round(spark_col(column), decimal_places).alias(column)
                if isinstance(df.schema[column].dataType, FloatType)
                else spark_col(column)
            )
            for column in df.columns
        ]
    )

    return df
