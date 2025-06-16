## section 002 initiates producer and sends messages to main topic

from p1_read_sql import get_events_df
from pyspark.sql.functions import to_json, struct, col
from configs import config
from pyspark.sql import SparkSession


def produce_events():
    try:
        # Create a SparkSession
        spark = (SparkSession.builder
                .config("spark.jars", config["spark.jars"])
                .master("local[*]")
                .appName(config["spark.appName"])
                .getOrCreate())

        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        # Get events data
        kafka_df = get_events_df(existing_spark=spark)

        # Transform the DataFrame to have "key" and "value" columns (required for Kafka)
        kafka_df_2 = kafka_df.select(
            col("result_id").cast("string").alias("key"),
            to_json(struct("country_noc", "sport", "athlete_id", "medal", "result_id")).alias("value")
        )

        # Write to Kafka
        (kafka_df_2.write
            .format("kafka")
            .option("kafka.bootstrap.servers", config['kafka.bootstrap_servers'][0])
            .option("kafka.security.protocol", config['kafka.security_protocol'])
            .option("kafka.sasl.mechanism", config['kafka.sasl_mechanism'])
            .option("kafka.sasl.jaas.config",
                    f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{config['kafka.username']}' password='{config['kafka.password']}';")
            .option("topic", config['kafka.in_topic'])
            .save())

        print(f"Dataset produced successfully, {kafka_df_2.count()} rows written to topic {config['kafka.in_topic']}")
        return True
    except Exception as e:
        print(f"Error producing events: {e}")
        return False

# Execute the function
if __name__ == "__main__":
    produce_events()
