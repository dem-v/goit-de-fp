## base class for consumers

from kafka import KafkaConsumer
from configs import config
import json

def process_message(topic_name, delegate_method=None, group_id='my_group_demi'):

    # Створення Kafka Consumer
    consumer = KafkaConsumer(
        bootstrap_servers=config['kafka.bootstrap_servers'][0],
        security_protocol=config['kafka.security_protocol'],
        sasl_mechanism=config['kafka.sasl_mechanism'],
        sasl_plain_username=config['kafka.username'],
        sasl_plain_password=config['kafka.password'],
        value_deserializer=lambda v: v, #json.loads(v.decode('utf-8')) if v else None,
        key_deserializer=lambda v: v, #json.loads(v.decode('utf-8')) if v else None,
        auto_offset_reset='earliest',  # Зчитування повідомлень з початку
        enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
        group_id=group_id   # Ідентифікатор групи споживачів
    )

    # Підписка на тему
    consumer.subscribe([topic_name])

    print(f"Subscribed to topic '{topic_name}'")

    # Обробка повідомлень з топіку
    try:
        for message in consumer:
            if delegate_method:
                delegate_method(message)
            else:
                print(f"Received alert: {message.value} with key: {message.key}, partition {message.partition}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        consumer.close()  # Закриття consumer

