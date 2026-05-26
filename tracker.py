# first connect to kafka broker and listen to messages from a specific topic
from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-tracker", # shared group ID for same instances
    "auto.offset.reset": "earliest" # tells consumer to start reading from first message
                                    # in topic if it forgets where it left off
}

consumer = Consumer(consumer_config)

# subscribe to some number of topics
consumer.subscribe(["orders"])

print("Consumer is running and subscribed to orders topic")

# logic to make consumer check for events from topic 'orders'
try:
    while True:
        # returns bytes of data, which confluent turns into a messsage objec
        msg = consumer.poll(1.0) # continousuly poll Kafka for new messages from last time
                                # it read for events
        
        if msg is None:
            continue
        
        if msg.error():
            print("Error:", msg.error())

        # else, message object form polling exists and has no error

        value = msg.value().decode("utf-8") # turns bytes to JSON string
        order = json.loads(value) # turns JSON to Python dictionary

        print(f"Package Received order: {order['quantity']} x {order['item']} from {order['user']}")

except KeyboardInterrupt:
    print("\n Stopping consumer")

finally:
    consumer.close()