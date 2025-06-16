## step 001, administrates new topics
from p1_kafka_admin_base import create_topic
from configs import config

# Визначення нового топіку
create_topic(config["kafka.in_topic"])
create_topic(config["kafka.out_topic"])



