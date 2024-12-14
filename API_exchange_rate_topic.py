from pyspark.sql import SparkSession
import requests
import json
from kafka import KafkaProducer

# Initialize Spark session with MinIO checkpoint configuration
spark = SparkSession.builder \
    .appName("Exchange Rate API to Kafka") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-server:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "I6wT68mGbJ0Q1DqC") \
    .config("spark.hadoop.fs.s3a.secret.key", "OOKyQYPYIm9PegMFM8mLd8wq2fT7sB7K") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.streaming.checkpointLocation", "s3a://checkpoints/API_exchange_rate_topic/API_exchange_rate_topic/") \
    .getOrCreate()
print("Spark session initialized successfully")

# Function to make an HTTP GET request
def get_exchange_rate():
    url = "https://v6.exchangerate-api.com/v6/2fbf16c377a5875d9c8785c5/latest/USD"
    response = requests.get(url)
    print("HTTP GET request successful")
    return response.json()

# Function to send data to Kafka
def send_to_kafka(topic, data):
    producer = KafkaProducer(bootstrap_servers='course-kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(topic, data)
    producer.flush()
    print("Data sent to Kafka successfully")

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
print("Spark session stopped successfully")
