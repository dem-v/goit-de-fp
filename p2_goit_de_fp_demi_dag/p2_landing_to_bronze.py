import os
import sys
import tempfile
from os.path import exists
from typing import Optional

import requests
from pyspark.sql import SparkSession

from p2_base import create_spark_session, logger


def download_csv_from_url(url: str, local_dir: str) -> Optional[str]:
    """
    Download a CSV file from a URL

    Args:
        url: Full URL to the CSV file
        local_dir: Local directory to save the downloaded file

    Returns:
        Path to the downloaded file or None if download failed
    """
    try:
        # Extract filename from URL
        filename = os.path.basename(url)
        local_path = os.path.join(local_dir, filename)

        logger.info(f"Downloading file from URL: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors

        with open(local_path, 'wb') as local_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    local_file.write(chunk)

        logger.info(f"File downloaded successfully to: {local_path}")
        return local_path

    except Exception as e:
        logger.error(f"Error downloading file from URL: {str(e)}")
        return None

def process_csv_to_parquet(spark: SparkSession, csv_path: str, table_name: str, 
                          bronze_dir: str = "bronze") -> bool:
    """
    Process a CSV file and save it as parquet in the bronze layer

    Args:
        spark: Spark session
        csv_path: Path to the CSV file
        table_name: Name of the table (used for output directory)
        bronze_dir: Base directory for bronze layer

    Returns:
        True if processing was successful, False otherwise
    """
    try:
        logger.info(f"Reading CSV file: {csv_path}")

        # Read the CSV file with Spark
        # Note: inferSchema is set to true for automatic schema detection
        # In production, you might want to define a specific schema
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ",")
              .csv(csv_path))

        # Show sample data and schema
        logger.info(f"CSV file loaded with {df.count()} rows")
        logger.info("Sample data:")
        df.show(5, truncate=False)
        logger.info("Schema:")
        df.printSchema()

        # Create output directory if it doesn't exist
        output_dir = os.path.join(bronze_dir, table_name)

        # Write the dataframe as parquet
        logger.info(f"Writing parquet file to: {output_dir}")
        df.write.mode("overwrite").parquet(output_dir)

        logger.info(f"Successfully processed CSV to parquet in {output_dir}")
        return True

    except Exception as e:
        logger.error(f"Error processing CSV to parquet: {str(e)}")
        return False

def execute_landing_to_bronze(url: str, bronze_dir: str = "bronze", table_name_arg: Optional[str] = None, app_name: str = "LandingToBronze") -> None:

    logger.info(f"Starting landing to bronze process for {url}")

    # Create a temporary directory for downloaded files
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"Created temporary directory: {temp_dir}")

        # Determine if source is URL or FTP based on prefix
        # Download CSV from URL
        logger.info(f"Downloading from URL: {url}")
        local_csv_path = download_csv_from_url(url, temp_dir)


        if not local_csv_path:
            logger.error("Failed to download CSV file. Exiting.")
            sys.exit(1)

        # Extract table name from the CSV filename if not provided
        table_name = table_name_arg if table_name_arg else os.path.splitext(os.path.basename(local_csv_path))[0]
        logger.info(f"Using table name: {table_name}")

        # Create Spark session
        spark = create_spark_session(app_name)

        try:
            # Process CSV to parquet
            success = process_csv_to_parquet(spark, local_csv_path, table_name, bronze_dir)

            if not exists('/tmp/airflow_spark_result.txt'):
                current_data = ''
            else:
                with open('/tmp/airflow_spark_result.txt', 'r') as f:
                    current_data = f.read()

            with open('/tmp/airflow_spark_result.txt', 'w') as f:
                f.write(f"{current_data}{(',' if len(current_data) > 0 else '')}{table_name_arg}")

            logger.info(f"Current data in /tmp/airflow_spark_result.txt: {current_data}{(',' if len(current_data) > 0 else '')}{table_name_arg}")
            if success:
                logger.info("Landing to bronze process completed successfully")
            else:
                logger.error("Landing to bronze process failed")
                sys.exit(1)

        finally:
            # Stop Spark session
            if spark:
                spark.stop()
                logger.info("Spark session stopped")

if __name__ == "__main__":
    execute_landing_to_bronze('https://ftp.goit.study/neoversity/athlete_bio.csv', '../bronze', 'athlete_bio', 'LandingToBronze')
    execute_landing_to_bronze('https://ftp.goit.study/neoversity/athlete_event_results.csv', '../bronze', 'athlete_event_results', 'LandingToBronze')
