# Final_Project_2

Project Overview

This repository contains a data engineering project designed to process and analyze data from an API. The project builds a data pipeline using Apache Kafka, PostgreSQL, and Elasticsearch to enable efficient data storage and querying, while Docker is used to simplify deployment. The pipeline aims to:

Ingest data from an external API.

Process and store the data in a PostgreSQL database.

Transfer data from PostgreSQL to Elasticsearch for querying and visualization.

Provide a framework for extending the pipeline to integrate AI models for predictions.

Technologies Used

Programming Language: Python

Message Broker: Apache Kafka

Database: PostgreSQL

Search Engine: Elasticsearch (ELK Stack)

Containerization: Docker

Repository Structure

The repository is organized as follows:

.
|-- Docker_files/               # Contains Docker configuration files for different services
|-- SQL/                        # SQL scripts for database schema and data manipulation
|-- API_exchange_rate_topic.py  # Script to ingest data from an external API to Kafka
|-- kafka_to_postgres.py        # Script to consume Kafka messages and store them in PostgreSQL
|-- postgres_to_elk.py          # Script to transfer data from PostgreSQL to Elasticsearch
|-- README.md                   # Documentation for the project

Getting Started

Follow these steps to set up and run the project locally.

Prerequisites

Docker and Docker Compose installed.

Python (version 3.8 or higher).

Kafka and PostgreSQL setup (or use the Dockerized versions provided).

Installation

Clone the repository:

git clone https://github.com/Shnul/Final_Project_2.git
cd Final_Project_2

Build and run the Docker containers:

docker-compose up

Install required Python libraries:

pip install -r requirements.txt

Usage

API Ingestion:
Run the API_exchange_rate_topic.py script to fetch data from the API and publish it to Kafka:

python API_exchange_rate_topic.py

Kafka to PostgreSQL:
Use kafka_to_postgres.py to consume Kafka messages and insert the data into PostgreSQL:

python kafka_to_postgres.py

PostgreSQL to Elasticsearch:
Execute postgres_to_elk.py to transfer data from PostgreSQL to Elasticsearch:

python postgres_to_elk.py

Visualization:
Access the data via Elasticsearch or visualize it using tools like Kibana.

Future Enhancements

AI Model Integration: Add machine learning models to predict trends or analyze data.

Improved Error Handling: Enhance resilience to API or database failures.

Scalability: Optimize the pipeline for larger datasets and higher throughput.

License

This project is licensed under the MIT License. See the LICENSE file for details.

Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

Contact

For questions or feedback, please contact the repository owner via GitHub
