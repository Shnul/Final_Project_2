from kafka import KafkaConsumer
import json
import psycopg2
import logging
import time
from datetime import datetime
from typing import Dict, Any
from minio import Minio
from minio.error import S3Error
import pandas as pd
import io
from elasticsearch import Elasticsearch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection parameters
DB_PARAMS = {
    'dbname': 'airflow',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'postgres', 
    'port': '5432'
}

# MinIO client configuration
MINIO_CLIENT = Minio(
    "minio:9000",  # Adjusted to use the correct port for S3 API
    access_key="TRmpK4uHjPPommBK",
    secret_key="MhGfxI7b1Oiiq14BNlJvJKWM9VPTBrGY",
    secure=False
)
CHECKPOINT_BUCKET = "checkpoints"
CHECKPOINT_FILE = "kafka_checkpoint.parquet"

# Elasticsearch client configuration
ES_CLIENT = Elasticsearch(["http://final_project-elasticsearch-1:9200"])

def create_table() -> None:
    """Create exchange rates table if it doesn't exist"""
    query = """
    CREATE TABLE IF NOT EXISTS exchange_rates (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        base_currency VARCHAR(3),
        currency VARCHAR(3),
        rate DECIMAL(20,10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        logging.info("Table creation successful or already exists")
        print("Table creation successful or already exists")
    except psycopg2.Error as e:
        logging.error(f"Failed to create table: {e}")
        print(f"Failed to create table: {e}")
        raise

def insert_exchange_rates(data: Dict[str, Any]) -> None:
    """Insert exchange rate data into PostgreSQL and Elasticsearch"""
    timestamp = datetime.now()
    base_currency = data['base_code']
    
    query = """
    INSERT INTO exchange_rates (timestamp, base_currency, currency, rate)
    VALUES (%s, %s, %s, %s)
    """
    
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                for currency, rate in data['conversion_rates'].items():
                    cur.execute(query, (timestamp, base_currency, currency, float(rate)))
                    # Index data into Elasticsearch
                    es_data = {
                        "timestamp": timestamp,
                        "base_currency": base_currency,
                        "currency": currency,
                        "rate": float(rate),
                        "created_at": datetime.now()
                    }
                    try:
                        ES_CLIENT.index(index="exchange_rates", body=es_data)
                        logging.info(f"Indexed data into Elasticsearch: {es_data}")
                        print(f"Indexed data into Elasticsearch: {es_data}")
                    except Exception as es_error:
                        logging.error(f"Failed to index data into Elasticsearch: {es_error}")
                        print(f"Failed to index data into Elasticsearch: {es_error}")
            conn.commit()
        logging.info(f"Inserted {len(data['conversion_rates'])} exchange rates")
        print(f"Inserted {len(data['conversion_rates'])} exchange rates")
    except psycopg2.Error as e:
        logging.error(f"Failed to insert data: {e}")
        print(f"Failed to insert data: {e}")
        raise

def test_connection(max_retries: int = 3) -> bool:
    """Test database connection with retries"""
    for attempt in range(max_retries):
        try:
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                logging.info(f"Database connection successful (attempt {attempt + 1})")
                print(f"Database connection successful (attempt {attempt + 1})")
                return True
        except psycopg2.Error as e:
            logging.warning(f"Connection attempt {attempt + 1} failed: {e}")
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return False

def create_consumer() -> KafkaConsumer:
    """Create and return Kafka consumer"""
    try:
        consumer = KafkaConsumer(
            'exchange_rate_index_topic',  # New topic for indexing data into Elasticsearch
            bootstrap_servers=['course-kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='exchange_rate_index_group',
            enable_auto_commit=False
        )
        logging.info("Kafka consumer created successfully")
        print("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logging.error(f"Failed to create Kafka consumer: {e}")
        print(f"Failed to create Kafka consumer: {e}")
        raise

def save_checkpoint(offset: int) -> None:
    """Save the current offset to MinIO as a Parquet file"""
    try:
        df = pd.DataFrame([{"offset": offset}])
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        MINIO_CLIENT.put_object(
            CHECKPOINT_BUCKET,
            CHECKPOINT_FILE,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        logging.info(f"Checkpoint saved at offset {offset}")
        print(f"Checkpoint saved at offset {offset}")
    except S3Error as e:
        logging.error(f"Failed to save checkpoint: {e}")
        print(f"Failed to save checkpoint: {e}")

def load_checkpoint() -> int:
    """Load the last saved offset from MinIO as a Parquet file"""
    try:
        response = MINIO_CLIENT.get_object(CHECKPOINT_BUCKET, CHECKPOINT_FILE)
        df = pd.read_parquet(io.BytesIO(response.read()))
        response.close()
        response.release_conn()
        offset = df["offset"].iloc[0]
        logging.info(f"Loaded checkpoint at offset {offset}")
        print(f"Loaded checkpoint at offset {offset}")
        return offset
    except S3Error as e:
        logging.warning(f"Failed to load checkpoint: {e}")
        print(f"Failed to load checkpoint: {e}")
        return 0

def main() -> None:
    """Main function to run the Kafka consumer"""
    if not test_connection():
        logging.error("Failed to connect to database after multiple attempts")
        print("Failed to connect to database after multiple attempts")
        return

    try:
        create_table()
        consumer = create_consumer()
        last_offset = load_checkpoint()
        logging.info(f"Kafka consumer started from offset {last_offset}. Waiting for messages...")
        print(f"Kafka consumer started from offset {last_offset}. Waiting for messages...")
        
        for message in consumer:
            if message.offset <= last_offset:
                continue
            try:
                data = message.value
                logging.info(f"Received message from partition {message.partition}, offset {message.offset}")
                print(f"Received message from partition {message.partition}, offset {message.offset}")
                insert_exchange_rates(data)
                save_checkpoint(message.offset)
                consumer.commit()
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                print(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
        print("Shutting down consumer...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logging.info("Consumer closed")
            print("Consumer closed")

if __name__ == "__main__":
    main()
