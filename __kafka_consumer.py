
from kafka import KafkaConsumer
import sys, os


if __name__ == "__main__":
    try:
        print("Initialization...")
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                 group_id='my-group',
                                 enable_auto_commit=True,
                                 auto_commit_interval_ms=1000,
                                 auto_offset_reset='latest')
        consumer.subscribe(['twittertest'])

        for tweet in consumer:
            print(tweet.value)

        print("End")

    except KeyboardInterrupt:
        print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)



