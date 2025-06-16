from configs import config
from p1_consumer_base import process_message
import sys

if __name__ == "__main__":
    try:
        print(f"Starting consumer for topic: {config['kafka.out_topic']}")
        process_message(config['kafka.out_topic'])
    except KeyboardInterrupt:
        print("Consumer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error in consumer: {e}")
        sys.exit(1)
