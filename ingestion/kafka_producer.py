from kafka import KafkaProducer
import json
import time
import requests

# 1. Setup Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    # Simulate fetching from your API
    # Replace with your actual API logic
    sample_job = {
        "job_id": "12345",
        "title": "Data Engineer",
        "skills": ["Python", "Spark", "Kafka"],
        "timestamp": time.time()
    }
    
    print(f"Sending job to Kafka: {sample_job['title']}")
    producer.send('tech_jobs_stream', sample_job)
    producer.flush()

if __name__ == "__main__":
    try:
        while True:
            fetch_and_send()
            time.sleep(5) # Send a job every 5 seconds for testing
    except KeyboardInterrupt:
        print("Stopping Producer...")
    