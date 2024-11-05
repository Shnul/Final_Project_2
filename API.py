import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Path to the downloaded PostgreSQL JDBC driver
jdbc_driver_path = r"C:\Final_Project\postgresql-42.2.20.jar"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API Data Processing") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Function to call the data API
def fetch_data_api():
    url = "https://air-cargo-schedule-and-rate.p.rapidapi.com/search"
    payload = {
        "origin": "CDG",
        "destination": "LAX",
        "departureDate": "2024-12-15",  # Updated to a future date
        "offset": 5,
        "shipment": {
            "product": "GCR",
            "pieces": 1,
            "weight": 100,
            "chargeableWeight": 167,
            "volume": 1,
            "dimensions": [
                {
                    "height": 100,
                    "length": 100,
                    "width": 100,
                    "pieces": 1,
                    "weight": 100,
                    "stackable": True,
                    "tiltable": False,
                    "toploadable": False,
                    "weightType": "PER_ITEM"
                }
            ]
        },
        "user": {
            "country": "FR",
            "cass": "0000",
            "iata": "0000000"
        },
        "filters": {
            "withRateOnly": True,
            "liveRequests": True
        },
        "timeout": 25
    }
    headers = {
        "x-rapidapi-key": "68cdb025bdmshd1666e99b86ad70p1b139ajsnd7a43a37820d",
        "x-rapidapi-host": "air-cargo-schedule-and-rate.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

# Fetch data from the data API
data = fetch_data_api()

# Convert the data to a Spark DataFrame
df = spark.createDataFrame([data])

# Explode the flights array to create a row for each flight
df = df.withColumn("flight", explode(col("flights"))).select("flight.*")

# Show the schema of the DataFrame to inspect the available columns
df.printSchema()

# Show the first few rows of the DataFrame to inspect the data
df.show(truncate=False)

# Write the DataFrame to PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", "flights") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
