from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka import KafkaError
import time
import os

script_dir = os.path.dirname(os.path.realpath(__file__))
schema_path = os.path.join(script_dir, "../schema/order.avsc")

with open(schema_path, 'r') as f:
    avro_schema_str = f.read()

# === Schema Registry & Serializers ===
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)
avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema_str)

string_serializer = StringSerializer('utf-8')
string_deserializer = StringDeserializer('utf-8')

# === Consumer Config ===
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': 'order-group-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['orders'])

# === Shared Producer for both retry and DLQ (best practice) ===
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': string_serializer,
    'value.serializer': avro_serializer
}
producer = SerializingProducer(producer_conf)

# === State ===
total_price = 0.0
order_count = 0
MAX_RETRIES = 3

def process_order(order):
    global total_price, order_count
    # Simulate ~10% failure
    if hash(order["orderId"]) % 10 == 0:
        raise Exception("Simulated processing failure")

    total_price += order["price"]
    order_count += 1
    avg = total_price / order_count
    print(f"[SUCCESS] Order {order['orderId']} | {order['product']:>6} | ${order['price']:7.2f} | Running Avg: ${avg:7.2f}")

# Main loop
if __name__ == "__main__":
    try:
        print(f"Consumer running. Using schema from: {schema_path}")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                break

            order = msg.value()
            key = msg.key()

            # Extract retry count from headers
            headers = msg.headers() or []
            retry_count = 0
            for h_key, h_value in headers:
                if h_key == 'retry_count':
                    retry_count = int(h_value.decode('utf-8'))
                    break

            try:
                process_order(order)
                consumer.commit(asynchronous=False)

            except Exception as e:
                # We ALREADY have the correct retry_count from the top of the loop.
                # Just increment it.
                retry_count += 1
                print(f"Failed -> Retrying {key} | Attempt {retry_count}/{(MAX_RETRIES + 1)}")

                if retry_count >= MAX_RETRIES + 1:  # After 4th attempt (1 + 3 retries)
                    print(f"Max retries exceeded -> Sending to DLQ: {key}")
                    producer.produce(
                        topic='orders-dlq',
                        key=key,
                        value=order,
                        headers=[('reason', b'processing_failed')]
                    )
                    producer.flush(timeout=10)
                    consumer.commit(asynchronous=False)
                else:
                    # Retry: send back to orders topic
                    new_headers = [(k, v) for k, v in headers if k != 'retry_count']
                    new_headers.append(('retry_count', str(retry_count).encode()))
                    producer.produce(
                        topic='orders',
                        key=key,
                        value=order,
                        headers=new_headers
                    )
                    producer.flush(timeout=10)
                    consumer.commit(asynchronous=False)

            time.sleep(0.05)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        producer.flush()