## humidity alert consumers

from configs import config
from p1_consumer_base import process_message
from p1_read_sql import get_bio_df
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
import pandas as pd
import os

def consumer_processing_function(message):
    # Print received message for debugging
    print(f"Received message: {message.value}")

    # Створення SparkSession
    spark = (SparkSession.builder
             .appName("KafkaStreaming")
             .master("local[*]")
             .getOrCreate())

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

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
          .option("maxOffsetsPerTrigger", "5")
          .load())

    # Визначення схеми для JSON,
    # оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
    json_schema = StructType([
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("country_noc", StringType(), True)
    ])

    # Маніпуляції з даними
    clean_df = (df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("value_json", from_json(col("value"), json_schema))
    .select(
        col("key").cast("bigint").alias("result_id"),
        col("value_json.athlete_id").alias("athlete_id"),
        col("value_json.sport").alias("sport"),
        col("value_json.medal").alias("medal"),
        col("value_json.country_noc").alias("country_noc"),
    ))

    bio_df = get_bio_df(existing_spark=spark)

    # Define a processing function to handle each batch
    def process_batch(batch_df, batch_id):
        print(f"Processing batch: {batch_id}")
        if batch_df.isEmpty():
            print(f"Batch {batch_id} is empty, skipping.")
            return

        # Join with bio_df (this works because bio_df is static)
        joined_df = (batch_df.join(bio_df, batch_df.athlete_id == bio_df.athlete_id, 'inner')
                    .drop(batch_df.athlete_id)
                    .drop(bio_df.country_noc)
                    .select('sport', 'medal', 'sex', 'country_noc', 'height', 'weight')
                    .withColumn("timestamp", current_timestamp())
                    .groupBy(['sport', 'medal', 'sex', 'country_noc'])
                    .agg(
                        count("*").alias("total_cnt_"),
                        avg("height").alias("height_avg_"),
                        avg("weight").alias("weight_avg_"),
                    ))

        print(f"Join success: {batch_id}")

        # Show results for debugging
        print(f"Batch {batch_id} results:")
        joined_df.show(10)

        # Prepare data for Kafka with key-value structure
        kafka_df = joined_df.withColumn(
            "key", 
            concat(col("sport"), lit("_"), col("medal"), lit("_"), col("sex"), lit("_"), col("country_noc"))
        ).select(
            col("key").cast("string"),
            to_json(struct("sport", "medal", "sex", "country_noc", "total_cnt_", "height_avg_", "weight_avg_")).alias("value")
        )

        try:
            # Send to Kafka
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
            print(f"Successfully wrote {kafka_df.count()} records to Kafka topic {config['kafka.out_topic']}")
        except Exception as e:
            print(f"Error writing to Kafka: {e}")

        try:
            # Also save to MySQL
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
            print(f"Successfully wrote {joined_df.count()} records to MySQL table {config['jdbc.output_table']}")
        except Exception as e:
            print(f"Error writing to MySQL: {e}")

        print(f"Processed batch {batch_id}: {joined_df.count()} rows")

    # Process the streaming data in micro-batches
    query = (clean_df.writeStream
             .foreachBatch(process_batch)
             .outputMode("update")
             .option("checkpointLocation", "/tmp/checkpoints-3")
             .start())

    # Wait for the streaming query to terminate
    query.awaitTermination()


if __name__ == "__main__":
    try:
        print(f"Starting main processor for topic: {config['kafka.in_topic']}")
        process_message(config['kafka.in_topic'], consumer_processing_function)
    except KeyboardInterrupt:
        print("Main processor stopped by user")
        import sys
        sys.exit(0)
    except Exception as e:
        print(f"Error in main processor: {e}")
        import sys
        sys.exit(1)
