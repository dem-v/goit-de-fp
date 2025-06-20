import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.32 pyspark-shell'

user_name = "demi"

config = {
    "kafka.bootstrap_servers": ['77.81.230.104:9092'],
    "kafka.username": 'admin',
    "kafka.password": 'VawEzo1ikLtrA8Ug8THa',
    "kafka.security_protocol": 'SASL_PLAINTEXT',
    "kafka.sasl_mechanism": 'PLAIN',
    "kafka.in_topic": f"{user_name}_athlete_event_results",
    "kafka.out_topic": f"{user_name}_averaged_results",

    'jdbc.url': "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    'jdbc.events_table': "athlete_event_results",
    'jdbc.output_table': "demi_fp_averaged_results",
    'jdbc.bio_table': "athlete_bio",
    'jdbc.user': "neo_data_admin",
    'jdbc.password': "Proyahaxuqithab9oplp",
    'jdbc.driver': 'com.mysql.cj.jdbc.Driver',

    # "spark.jars": "/home/demi/spark-projects-goit/fp/mysql-connector-j-8.0.32.jar",
    "spark.appName": "JDBCToKafka",

}