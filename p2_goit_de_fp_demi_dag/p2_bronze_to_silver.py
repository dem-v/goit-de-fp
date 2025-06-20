import argparse
import os
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from p2_base import create_spark_session, logger, read_parquet_tables


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())


def deduplicate_dataframe(df, subset=None, keep_first=True):
    """
    Remove duplicate rows from a DataFrame

    Args:
        df: Spark DataFrame to deduplicate
        subset: Optional list of column names to consider for deduplication.
                If None, all columns are used.
        keep_first: If True, keeps the first occurrence of duplicates.
                   If False, keeps the last occurrence.

    Returns:
        Deduplicated DataFrame
    """
    # Count records before deduplication
    count_before = df.count()

    # Perform deduplication
    if subset:
        deduplicated_df = df.dropDuplicates(subset=subset)
    else:
        deduplicated_df = df.dropDuplicates()

    # Count records after deduplication
    count_after = deduplicated_df.count()
    duplicates_removed = count_before - count_after

    logger.info(f"Deduplication complete: {duplicates_removed} duplicate records removed")
    logger.info(f"Original record count: {count_before}, New record count: {count_after}")

    return deduplicated_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--prev', help='Previous result')
    args = parser.parse_args()

    prev = args.prev.split(',')

    # Use the arguments in your Spark application
    logger.info(f"Previous result: {prev}")

    spark = create_spark_session()
    tables = read_parquet_tables(spark, prev, base_path="../bronze/")

    # Process each DataFrame
    for table_name, df in tables.items():
        # Example transformation: Apply the clean_text_udf to all string columns
        # You can customize this part according to your needs
        for col_name in df.columns:
            if df.schema[col_name].dataType == StringType():
                df = df.withColumn(col_name, clean_text_udf(df[col_name]))

        # Deduplicate the DataFrame
        df = deduplicate_dataframe(df, subset=None)

        # Reassign the transformed DataFrame back to the dictionary
        tables[table_name] = df

    # Save the transformed DataFrames back to Parquet files
    for table_name, df in tables.items():
        output_dir = os.path.join('silver', table_name)

        # Write the dataframe as parquet
        logger.info(f"Writing parquet file to: {output_dir}")
        df.write.mode("overwrite").parquet(f"{output_dir}")

    spark.stop()
