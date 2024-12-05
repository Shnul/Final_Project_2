from pyspark.sql import SparkSession
import requests
import json
from kafka import KafkaProducer

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Exchange Rate API to Kafka") \
    .getOrCreate()

# Function to make an HTTP GET request
def get_exchange_rate():
    url = "https://v6.exchangerate-api.com/v6/2fbf16c377a5875d9c8785c5/latest/USD"
    response = requests.get(url)
    return response.json()

# Function to send data to Kafka
def send_to_kafka(topic, data):
    producer = KafkaProducer(bootstrap_servers='course-kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(topic, data)
    producer.flush()

# Main function
if __name__ == "__main__":
    # Get exchange rate data
    exchange_rate_data = get_exchange_rate()
    print("Exchange rate data:", exchange_rate_data)
    
    # Send the exchange rate data to Kafka
    send_to_kafka('exchange_rate_topic', exchange_rate_data)
    print("Data sent to Kafka successfully")

# Stop the Spark session
spark.stop()
