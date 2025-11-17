# create_topics.py
from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({
    'bootstrap.servers': 'localhost:9092'
})

topics = [
    NewTopic("orders", num_partitions=1, replication_factor=1),
    NewTopic("orders-dlq", num_partitions=1, replication_factor=1)
]

fs = admin_client.create_topics(topics)

for topic, f in fs.items():
    try:
        f.result()  # Wait for operation to finish
        print(f"Topic '{topic}' created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")