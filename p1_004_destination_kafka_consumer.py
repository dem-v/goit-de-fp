from configs import config
from p1_consumer_base import process_message
import sys

def display_message(message):
    """
    Custom message handler that formats and displays Kafka messages
    """
    try:
        print(f"\n--- New Message Received ---")
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Value: {message.value}")
        print(f"Timestamp: {message.timestamp}")
        print("---------------------------\n")
    except Exception as e:
        print(f"Error processing message: {e}")

if __name__ == "__main__":
    try:
        print(f"Starting consumer for topic: {config['kafka.out_topic']}")
        process_message(config['kafka.out_topic'], display_message)
    except KeyboardInterrupt:
        print("Consumer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error in consumer: {e}")
        sys.exit(1)
