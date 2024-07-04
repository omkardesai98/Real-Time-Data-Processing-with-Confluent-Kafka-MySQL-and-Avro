import time
import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'JAG3VEJKO4QGTLEC',
    'sasl.password': '9uYGGj4Vhx6NEoljKqUCc3lRFtaO5mZL77EOpdIrWoZM766An4jwlF8kH9435//B'
}

# Create Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info': 'AU3FN3SZ6AMXZI36:gjXznU3NALcBAvo9KCrBdDBMGrf3QYChcdrxFNkXKe52ndrSH4YhgXSlnPZf2eAW'
})

# Get the latest Avro schema
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro serializer for value
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
key_serializer = StringSerializer('utf-8')  # Keep this as IntegerSerializer for integer keys

# Define Serializing Producer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanism': kafka_config['sasl.mechanism'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for User record {msg.key().decode('utf-8')}: {err}")
        return
    print(f'User record {msg.key().decode('utf-8')} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def fetch_initial_records():
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='omkar@123',
        database='kafka'
    )

    my_cursor = conn.cursor()
    query = "SELECT * FROM product ORDER BY last_updated ASC"
    my_cursor.execute(query)
    result = my_cursor.fetchall()
    my_cursor.close()
    conn.close()
    return result

def fetch_audit_records(date):
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='omkar@123',
        database='kafka'
    )

    my_cursor = conn.cursor()
    query = f"SELECT product_id,name,category,price,last_updated FROM product_audit WHERE audit_timestamp > '{date}' ORDER BY audit_timestamp ASC"
    my_cursor.execute(query)
    result = my_cursor.fetchall()
    my_cursor.close()
    conn.close()
    return result

latest_date = '1990-01-01 00:00:00'
initial_fetch_done = False

try:
    while True:
        if not initial_fetch_done:
            rows = fetch_initial_records()
            initial_fetch_done = True
        else:
            rows = fetch_audit_records(latest_date)

        for row in rows:
            product_dict = {
                'id': row[0],
                'name': row[1],
                'category': row[2],
                'price': row[3],
                'last_updated': row[4].strftime('%Y-%m-%d %H:%M:%S')
            }
            key = row[0]
            value = product_dict
            producer.produce(topic='product_updates', key=str(key), value=value, on_delivery=delivery_report)
            latest_date = row[4].strftime('%Y-%m-%d %H:%M:%S')
            producer.flush()  # Flush after all messages have been produced
            time.sleep(5)  # Sleep between polling intervals
except KeyboardInterrupt:
    print("Shutting down producer due to keyboard interrupt")
    producer.flush()  # Ensure all outstanding messages are sent before shutting down
