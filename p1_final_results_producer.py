
from pyspark.sql import SparkSession
from configs import config
from pyspark.sql.functions import to_json, struct

# Функція для обробки кожної партії даних
def foreach_batch_function(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping.")
        return

    # Convert DataFrame to Kafka format (key-value)
    kafka_batch = batch_df.select(
        to_json(struct("sport", "medal", "sex", "country_noc", "total_cnt_", "height_avg_", "weight_avg_")).alias(
            "value")
    )

    # Show what we're sending (for debugging)
    print(f"Batch {batch_id} sample data:")
    kafka_batch.show(3, truncate=False)

    # Відправка збагачених даних до Kafka
    (kafka_batch
     .writeStream
     .format("kafka")
     .option("kafka.bootstrap_servers", config['kafka.bootstrap_servers'][0])
     .option("kafka.security.protocol", config['kafka.security_protocol'])
     .option("kafka.sasl.mechanism", config['kafka.sasl_mechanism'])
     .option("kafka.sasl.jaas.config",
             f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{config['kafka.username']}' password='{config['kafka.password']}';")
     .option("topic", config['kafka.out_topic'])
     .start())

    # Збереження збагачених даних до MySQL
    (batch_df
     .write
     .format("jdbc")
     .options(
        url=config["jdbc.url"],
        driver=config["jdbc.driver"],  # com.mysql.jdbc.Driver
        dbtable=config["jdbc.output_table"],
        user=config["jdbc.user"],
        password=config["jdbc.password"])
     .mode("append")
     .save())

    print(f"batch produced: {batch_id}, {batch_df.count()} rows")


def execute_stream(event_stream_enriched):
    # Налаштування потоку даних для обробки кожної партії за допомогою вказаної функції
    query = (event_stream_enriched.writeStream
             .foreachBatch(foreach_batch_function)
             .outputMode("update")
             .option("checkpointLocation", "/tmp/checkpoints-4")
             .start())

    query.awaitTermination()