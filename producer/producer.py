from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import time
import random
import uuid
import os

script_dir = os.path.dirname(os.path.realpath(__file__))
schema_path = os.path.join(script_dir, "../schema/order.avsc")

with open(schema_path, 'r') as f:
    avro_schema_str = f.read()
# ---------------------------------------------------

def order_to_dict(order, ctx):
    return {
        "orderId": order["orderId"],
        "product": order["product"],
        "price": order["price"]
    }

# Config
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str, order_to_dict)
string_serializer = StringSerializer('utf_8')

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': string_serializer,
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

# Generate random orders
products = ["Item1", "Item2", "Item3", "Item4"]

def produce_order():
    order = {
        "orderId": str(uuid.uuid4())[:8],
        "product": random.choice(products),
        "price": round(random.uniform(10.0, 1000.0), 2)
    }
    producer.produce(
        topic='orders',
        key=order["orderId"],
        value=order,
        on_delivery=delivery_report
    )
    producer.poll(0)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent: {msg.key()} -> {msg.value()}")

# Main loop
if __name__ == "__main__":
    try:
        print(f"Producer running. Using schema from: {schema_path}")
        i = 0
        while True:
            produce_order()
            time.sleep(1)  # 1 order per second
            i += 1
            if i % 10 == 0:
                producer.flush()
    except KeyboardInterrupt:
        producer.flush()