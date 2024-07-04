import logging
import json
import time
import os
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'JAG3VEJKO4QGTLEC',
    'sasl.password': '9uYGGj4Vhx6NEoljKqUCc3lRFtaO5mZL77EOpdIrWoZM766An4jwlF8kH9435//B',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

# Create Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-e8vk0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info': 'AU3FN3SZ6AMXZI36:gjXznU3NALcBAvo9KCrBdDBMGrf3QYChcdrxFNkXKe52ndrSH4YhgXSlnPZf2eAW'
})

# Get the latest Avro schema
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro deserializer for value
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
key_deserializer = StringDeserializer('utf-8') 

# Define Deserializing Consumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanism': kafka_config['sasl.mechanism'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to the topic product_updates
consumer.subscribe(['product_updates'])

# Define the transformation function
def transform_record(record):
    record['category'] = record['category'].upper()
    discount_perc_list = [5, 10]
    if record['price'] >= 500 and record['price'] < 1000:
        record['price'] = record['price'] - record['price'] * (discount_perc_list[0] / 100)
    elif record['price'] >= 1000:
        record['price'] = record['price'] - record['price'] * (discount_perc_list[1] / 100)
    return record

# Function to handle file opening and closing
def open_file(filename):
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        f = open(filename, 'a')
        f.write('[')
    else:
        f = open(filename, 'r+')
        f.seek(0, os.SEEK_END)
        f.seek(f.tell() - 1, os.SEEK_SET)
        f.truncate()
        f.write(',\n')
    return f

# Open the file in append mode
filename = 'output.json'
f = open_file(filename)
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for a message
        if msg is None:
            continue

        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        key = msg.key()
        value = msg.value()

        if value:
            # Apply the transformation function
            transformed_value = transform_record(value)

            # Write the transformed data to the JSON file
            json_record = json.dumps(transformed_value)
            f.write(json_record + ',\n')
            print(f'Record written to file: {json_record}')
except KeyboardInterrupt:
    print("Shutting down consumer due to keyboard interrupt")
finally:
    f.seek(0, os.SEEK_END)
    f.seek(f.tell() - 3, os.SEEK_SET)
    f.truncate()
    f.write(']')
    f.close()
    consumer.close()
