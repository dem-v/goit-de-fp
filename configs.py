import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.32 pyspark-shell'

user_name = "demi"

config = {
    "kafka.bootstrap_servers": [],
    "kafka.username": '',
    "kafka.password": '',
    "kafka.security_protocol": 'SASL_PLAINTEXT',
    "kafka.sasl_mechanism": 'PLAIN',
    "kafka.in_topic": f"{user_name}_athlete_event_results",
    "kafka.out_topic": f"{user_name}_averaged_results",

    'jdbc.url': "",
    'jdbc.events_table': "",
    'jdbc.output_table': "",
    'jdbc.bio_table': "",
    'jdbc.user': "",
    'jdbc.password': "",
    'jdbc.driver': 'com.mysql.cj.jdbc.Driver',

    "spark.jars": "",
    "spark.appName": "JDBCToKafka",

}
