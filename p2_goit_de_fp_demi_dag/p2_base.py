import logging
import os
import sys

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "LandingToBronze") -> SparkSession:
    """Create and configure a Spark session"""
    logger.info("Creating Spark session...")

    spark = (SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created successfully - Version: {spark.version}")

    return spark

def read_parquet_tables(spark, table_names, base_path=""):
    """
    Read tables from Parquet files into Spark DataFrames

    Args:
        spark: SparkSession instance
        table_names: List of table names to read
        base_path: Base path where the Parquet files are stored

    Returns:
        Dictionary mapping table names to their respective DataFrames
    """
    dataframes = {}
    for table_name in table_names:
        path = os.path.join(base_path, table_name)
        try:
            df = spark.read.parquet(path)
            dataframes[table_name] = df
            logger.info(f"Successfully loaded table: {table_name}")
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")

    return dataframes
