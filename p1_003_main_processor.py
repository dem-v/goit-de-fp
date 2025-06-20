## humidity alert consumers

from configs import config
from p1_consumer_base import process_message
from p1_read_sql import get_bio_df
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
import pandas as pd
import os

from configs import config
from p1_consumer_base import process_message
from p1_read_sql import get_bio_df
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
import pandas as pd
import os
import time

# Глобальні змінні для SparkSession та bio_df
spark_session = None
bio_df_cached = None


import subprocess, logging

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_java_version():
    """Run 'java -version' command and log the output"""
    try:
        # java -version outputs to stderr, not stdout
        result = subprocess.run(['java', '-version'],
                                capture_output=True,
                                text=True,
                                check=True)

        # Log the output (java -version outputs to stderr)
        logger.info(f"Java version must be 11. Java check result:")
        for line in result.stderr.splitlines():
            logger.info(f"  {line}")

        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running java -version: {str(e)}")
        logger.error(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error running java -version: {str(e)}")
        return False

def consumer_processing_function(message):
    global spark_session, bio_df_cached

    # Print received message for debugging
    print(f"Received message: {message.value}")

    # Використовуємо глобальний SparkSession
    spark = spark_session

    # Читання потоку даних із Kafka
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", config['kafka.bootstrap_servers'][0])
          .option("kafka.security.protocol", config['kafka.security_protocol'])
          .option("kafka.sasl.mechanism", config['kafka.sasl_mechanism'])
          .option("kafka.sasl.jaas.config",
                  f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{config['kafka.username']}' password='{config['kafka.password']}';")
          .option("subscribe", config['kafka.in_topic'])
          .option("startingOffsets", "earliest")
          .option("maxOffsetsPerTrigger", "10")  # Збільшив для кращої продуктивності
          .option("failOnDataLoss", "false")  # Додав для стабільності
          .load())

    # Визначення схеми для JSON
    json_schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("result_id", IntegerType(), True)  # Додав result_id до схеми
    ])

    # Маніпуляції з даними
    clean_df = (df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value_json", from_json(col("value"), json_schema))
    .select(
        coalesce(col("value_json.result_id").cast("bigint"), col("key").cast("bigint")).alias("result_id"),
        col("value_json.athlete_id").alias("athlete_id"),
        col("value_json.sport").alias("sport"),
        col("value_json.medal").alias("medal"),
        col("value_json.country_noc").alias("country_noc")
    ))

    # Define a processing function to handle each batch
    def process_batch(batch_df, batch_id):
        try:
            print(f"Processing batch: {batch_id}")

            # Перевірка на пустий batch
            if batch_df.rdd.isEmpty():  # Виправлено: використовуємо rdd.isEmpty()
                print(f"Batch {batch_id} is empty, skipping.")
                return

            # Показуємо вхідні дані для дебагу
            print(f"Input batch {batch_id} data:")
            batch_df.show(5, truncate=False)

            # Join з bio_df (використовуємо кешований)
            joined_df = (batch_df.join(bio_df_cached, batch_df.athlete_id == bio_df_cached.athlete_id, 'inner')
            .drop(batch_df.athlete_id)
            .drop(bio_df_cached.country_noc)
            .select('sport', 'medal', 'sex', 'country_noc', 'height', 'weight')
            .withColumn("timestamp", current_timestamp())
            .groupBy(['sport', 'medal', 'sex', 'country_noc'])
            .agg(
                count("*").alias("total_cnt_"),
                avg("height").alias("height_avg_"),
                avg("weight").alias("weight_avg_")
            ))

            print(f"Join success for batch: {batch_id}")

            # Підрахуємо кількість записів перед записом
            record_count = joined_df.count()

            if record_count == 0:
                print(f"Batch {batch_id}: No records after join, skipping writes.")
                return

            # Show results for debugging
            print(f"Batch {batch_id} results ({record_count} records):")
            joined_df.show(10, truncate=False)

            # Prepare data for Kafka with key-value structure
            kafka_df = joined_df.withColumn(
                "key",
                concat(col("sport"), lit("_"), col("medal"), lit("_"), col("sex"), lit("_"), col("country_noc"))
            ).select(
                col("key").cast("string"),
                to_json(
                    struct("sport", "medal", "sex", "country_noc", "total_cnt_", "height_avg_", "weight_avg_")).alias(
                    "value")
            )

            # Send to Kafka з retry логікою
            def write_to_kafka():
                (kafka_df
                 .write
                 .format("kafka")
                 .option("kafka.bootstrap.servers", config['kafka.bootstrap_servers'][0])
                 .option("kafka.security.protocol", config['kafka.security_protocol'])
                 .option("kafka.sasl.mechanism", config['kafka.sasl_mechanism'])
                 .option("kafka.sasl.jaas.config",
                         f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{config['kafka.username']}' password='{config['kafka.password']}';")
                 .option("topic", config['kafka.out_topic'])
                 .save())

            try:
                write_to_kafka()
                print(f"Successfully wrote {record_count} records to Kafka topic {config['kafka.out_topic']}")
            except Exception as e:
                print(f"Error writing to Kafka after retries: {e}")

            # Also save to MySQL з retry логікою
            def write_to_mysql():
                (joined_df
                 .write
                 .format("jdbc")
                 .options(
                    url=config["jdbc.url"],
                    driver=config["jdbc.driver"],
                    dbtable=config["jdbc.output_table"],
                    user=config["jdbc.user"],
                    password=config["jdbc.password"])
                 .mode("append")
                 .save())

            try:
                write_to_mysql()
                print(f"Successfully wrote {record_count} records to MySQL table {config['jdbc.output_table']}")
            except Exception as e:
                print(f"Error writing to MySQL after retries: {e}")

            print(f"Processed batch {batch_id}: {record_count} rows")

        except Exception as e:
            print(f"❌ Exception in process_batch {batch_id}:")
            import traceback
            traceback.print_exc()
            # Не підіймаємо помилку, щоб не крешити весь стрім

    # Process the streaming data in micro-batches
    query = (clean_df.writeStream
             .foreachBatch(process_batch)
             .outputMode("update")
             .option("checkpointLocation", "/tmp/checkpoints-3")
             .trigger(processingTime="10 seconds")  # Додав тригер для стабільності
             .start())

    print(f"Streaming query started. Query ID: {query.id}")

    # Wait for the streaming query to terminate
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        query.stop()
    except Exception as e:
        print(f"Streaming query error: {e}")
        query.stop()


def initialize_spark_and_data():
    """Ініціалізація SparkSession та завантаження bio_df один раз"""
    global spark_session, bio_df_cached

    print("Initializing Spark Session...")
    spark_session = (SparkSession.builder
                     .appName("KafkaStreaming")
                     .master("local[*]")
                     .config("spark.driver.memory", "4g")
                     .config("spark.executor.memory", "4g")
                     .config("spark.sql.adaptive.enabled", "true")
                     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                     .getOrCreate())

    # Set log level to reduce verbosity
    spark_session.sparkContext.setLogLevel("WARN")

    print("Loading bio_df...")
    bio_df_cached = get_bio_df(existing_spark=spark_session)

    # Кешуємо bio_df для швидшого доступу
    bio_df_cached.cache()
    print(f"Bio_df loaded and cached. Record count: {bio_df_cached.count()}")

    return spark_session, bio_df_cached


if __name__ == "__main__":
    try:
        run_java_version()

        print(f"Starting main processor for topic: {config['kafka.in_topic']}")

        # Ініціалізуємо Spark та дані один раз
        initialize_spark_and_data()

        # Запускаємо обробку повідомлень
        process_message(config['kafka.in_topic'], consumer_processing_function)

    except KeyboardInterrupt:
        print("Main processor stopped by user")
        if spark_session:
            spark_session.stop()
        import sys

        sys.exit(0)
    except Exception as e:
        print(f"Error in main processor: {e}")
        import traceback

        traceback.print_exc()
        if spark_session:
            spark_session.stop()
        import sys

        sys.exit(1)