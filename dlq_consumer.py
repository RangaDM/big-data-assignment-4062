# dlq_consumer.py
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from rich import print as rprint
from rich.table import Table
import sys

# Schema (same as main consumer)
schema_str = """
{
  "namespace": "com.example.orders",
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
"""

schema_registry = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_deserializer = AvroDeserializer(schema_registry, schema_str)
key_deserializer = StringDeserializer('utf-8')

consumer = DeserializingConsumer({
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': 'dlq-monitor-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
})

consumer.subscribe(['orders-dlq'])

print("DLQ Monitor Started – Listening to orders-dlq topic...")
print("Press Ctrl+C to stop\n")

# --- FIX 1: Create a helper function for the table ---
def create_table():
    """Helper function to create a new rich Table"""
    table = Table(title="[bold red]Dead Letter Queue (orders-dlq)[/bold red]", show_header=True, header_style="bold magenta")
    table.add_column("Order ID", style="cyan")
    table.add_column("Product", style="green")
    table.add_column("Price", justify="right", style="yellow")
    table.add_column("Reason", style="red")
    return table
# --- End of FIX 1 ---

table = create_table() # Create the first table
count = 0

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        order = msg.value()
        key = msg.key()

        # Extract reason from headers (if exists)
        reason = "unknown"
        if msg.headers():
            for h_key, h_value in msg.headers():
                # --- FIX 2: Check for string 'reason', not b'reason' ---
                if h_key == 'reason':
                    reason = h_value.decode('utf-8')
                # --- End of FIX 2 ---

        table.add_row(
            order['orderId'],
            order['product'],
            f"${order['price']:.2f}",
            reason
        )
        count += 1
        if count % 5 == 0:  # Refresh display every 5 messages
            rprint(table)
            # --- FIX 3: Create a new, fresh table instead of clearing ---
            table = create_table()
            # --- End of FIX 3 ---

except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    consumer.close()
    
    # --- FIX 4: Print any remaining messages ---
    # This prints the last batch if the user stops
    # (e.g., if you processed 12 messages, this prints the last 2)
    if table.rows: 
        rprint(table) 
        
    rprint(f"\n[bold]DLQ monitoring stopped.[/bold] Total failed orders processed: {count}")