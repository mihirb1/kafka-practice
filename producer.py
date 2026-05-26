'''
 install python library for kafka connection, interface for python
 to make requests to kafka
'''

from confluent_kafka import Producer
import uuid
import json

# producer will talk to Kafka server at port 9092
# bootstrap.servers provide initial hosts (ports) that is starting point
# for Kafka client to find full set of brokers
producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

# UUID is built in Python library to randomly generate a 128 bit number,
# which is globally unique

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode("utf-8")}")
        print(f"Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")
        # use to see all methods/attributes (.partition(), .offset(), .topic(), 
        # .value(), .timestamp()
        # are essentials) on message objects
        # print(dir(msg))

order = {
    "order_id": str(uuid.uuid4()),
    "user": "lara",
    "item": "frozen yogurt",
    "quantity": 10
}

# must convert orders to JSON (string), and then encodes string into bytes
value = json.dumps(order).encode("utf-8")

# sends value to Kafka, says to save this in a topic called "orders"
    # kafka will create new topic if it does not exist
producer.produce(
    topic="orders", 
    value=value, 
    callback=delivery_report
)

# kafka producer will buffer messages for performance 
    # instead of sending one by one, it will collect events in batches (ex. 10) and send

# flush sends unsent/buffered events to kafka
producer.flush()