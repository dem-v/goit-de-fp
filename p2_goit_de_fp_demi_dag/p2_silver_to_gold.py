import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from p2_base import create_spark_session, logger, read_parquet_tables


def aggregate_athlete_stats(df: DataFrame, bio_df: DataFrame) -> DataFrame:
    """Агрегація статистики спортсменів"""
    return (df.join(bio_df, "athlete_id", "inner")
    .drop(bio_df.country_noc)  # Видаляємо country_noc з bio_df, залишаємо з основного df
    .select('sport', 'medal', 'sex', 'country_noc', 'height', 'weight')
    .withColumn("processing_timestamp", current_timestamp())
    .groupBy(['sport', 'medal', 'sex', 'country_noc'])
    .agg(
        count("*").alias("total_athletes"),
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        min("height").alias("min_height"),
        max("height").alias("max_height"),
        min("weight").alias("min_weight"),
        max("weight").alias("max_weight"),
        first("processing_timestamp").alias("timestamp")
    ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--prev', help='Previous result')
    args = parser.parse_args()

    prev = args.prev.split(',')

    # Use the arguments in your Spark application
    logger.info(f"Previous result: {prev}")

    spark = create_spark_session()
    tables = read_parquet_tables(spark, prev, base_path="silver/")

    df_a = tables['athlete_event_results']
    df_b = tables['athlete_bio']
    df_c = aggregate_athlete_stats(
        df_a.filter(col("athlete_id").isNotNull()),
        df_b.where(col("height").isNotNull())
              .where(col("weight").isNotNull()))

    output_dir = os.path.join('gold', 'demi_avg_stats')

    logger.info(f"Writing parquet file to: {output_dir}")
    # Save the transformed DataFrames back to Parquet files
    df_c.write.mode("overwrite").parquet(f"{output_dir}")

    logger.info(df_c.show(5))

    spark.stop()