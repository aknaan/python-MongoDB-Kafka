from confluent_kafka.admin import AdminClient, NewTopic

kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

admin_client = AdminClient(kafka_config)

topic_list = [NewTopic('purchase_topic', num_partitions=1, replication_factor=1)]

fs = admin_client.create_topics(topic_list)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
