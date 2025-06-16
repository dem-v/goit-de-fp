## section 002 initiates producer and sends messages to main topic

from kafka import KafkaProducer
from configs import config
import json
import uuid
import time

def sanitize_data(data):
    return data

def produce_results(topic_name, sql_data):
    # Створення Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=config['kafka.bootstrap_servers'][0],
        security_protocol=config['kafka.security_protocol'],
        sasl_mechanism=config['kafka.sasl_mechanism'],
        sasl_plain_username=config['kafka.username'],
        sasl_plain_password=config['kafka.password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    data = json.dumps(sanitize_data(sql_data)).encode('utf-8')

    try:
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {data} sent to topic '{topic_name}' successfully.")
        time.sleep(1)
    except Exception as e:
        print(f"An error occurred: {e}")

    producer.close()  # Закриття producer

