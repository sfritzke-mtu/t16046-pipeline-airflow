from kafka import KafkaConsumer
import json

# Kafka Configuration
#KAFKA_BROKER = "localhost:29092"  # If running from the host
KAFKA_BROKER = "kafka:9092"  # If running inside Docker
TOPIC_NAME = "my_topic"

#Use consumer_group: Kafka guarantees that a message is only ever read by a single consumer in the group
#The enable_auto_commit property is set to False to prevent the consumer from automatically committing the offset processing a message.
#auto_offset_reset
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset = "earliest",
)

print("Waiting for messages...")
for message in consumer:
    myDict =  message.value
    print(myDict)
