# kafka_to_postgres.py
from kafka import KafkaConsumer
import json
import psycopg2
import logging
import time
from datetime import datetime
from typing import Dict, Any

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
    except psycopg2.Error as e:
        logging.error(f"Failed to create table: {e}")
        raise

def insert_exchange_rates(data: Dict[str, Any]) -> None:
    """Insert exchange rate data into PostgreSQL"""
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
            conn.commit()
        logging.info(f"Inserted {len(data['conversion_rates'])} exchange rates")
    except psycopg2.Error as e:
        logging.error(f"Failed to insert data: {e}")
        raise

def test_connection(max_retries: int = 3) -> bool:
    """Test database connection with retries"""
    for attempt in range(max_retries):
        try:
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                logging.info(f"Database connection successful (attempt {attempt + 1})")
                return True
        except psycopg2.Error as e:
            logging.warning(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return False

def create_consumer() -> KafkaConsumer:
    """Create and return Kafka consumer"""
    return KafkaConsumer(
        'exchange_rate_topic',
        bootstrap_servers=['course-kafka:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='exchange_rate_group',
        enable_auto_commit=True
    )

def main() -> None:
    """Main function to run the Kafka consumer"""
    if not test_connection():
        logging.error("Failed to connect to database after multiple attempts")
        return

    try:
        create_table()
        consumer = create_consumer()
        logging.info("Kafka consumer started. Waiting for messages...")
        
        for message in consumer:
            try:
                data = message.value
                logging.info(f"Received message from partition {message.partition}, offset {message.offset}")
                insert_exchange_rates(data)
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logging.info("Consumer closed")

if __name__ == "__main__":
    main()