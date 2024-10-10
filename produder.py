from kafka import KafkaProducer
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Ensure this matches your Kafka bootstrap servers
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

# Produce some messages
def produce_messages():
    for i in range(100):
        message = {'id': i, 'message': f'Test message {i}'}
        producer.send('test01', value=message)  # Send message to 'test-topic'
        print(f"Produced: {message}")
        time.sleep(1)  # Simulate some delay between messages

if __name__ == "__main__":
    produce_messages()