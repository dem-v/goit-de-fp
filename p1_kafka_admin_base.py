from kafka.admin import KafkaAdminClient, NewTopic
from configs import config, user_name


def create_topic(topic_name, num_partitions=2, replication_factor=1):
    # Створення клієнта Kafka
    admin_client = KafkaAdminClient(
        bootstrap_servers=config['kafka.bootstrap_servers'],
        security_protocol=config['kafka.security_protocol'],
        sasl_mechanism=config['kafka.sasl_mechanism'],
        sasl_plain_username=config['kafka.username'],
        sasl_plain_password=config['kafka.password']
    )

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Перевіряємо список існуючих топіків
    print([print(topic) for topic in admin_client.list_topics() if user_name in topic])

    # Закриття зв'язку з клієнтом
    admin_client.close()