from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to publish messages to Kafka topic
def publish_message(topic, message):
    producer.send(topic, message.encode('utf-8'))