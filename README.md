# Sales Data Pipeline Project

## Overview
This project is a data pipeline designed to process, transform, and store data efficiently using modern big data tools and frameworks. The project leverages Apache Spark, Delta Lake, Kafka, and other technologies to handle large-scale data processing and analytics.

## Features
- **Data Ingestion**: Supports data ingestion from Kafka and other sources.
- **Batch Data Transformation**: Implements ETL processes using PySpark for raw, silver, and gold data layers.
- **Near realtime Data Transformation**: Implement near realtime processes using Spark Streaming due to limit of my laptop. Can be changed to Flink in future.
- **Delta Lake Integration**: Ensures ACID transactions and schema enforcement for data storage.
- **MinIO Integration**: Provides S3-compatible object storage for data layers.
- **Debezium**: Captures change data from MariaDB databases.
- **Superset**: Enables data visualization and analytics.
- **Trino**: Provides SQL query capabilities over the data warehouse.

## Architecture
The project follows a layered architecture:
1. **Bronze Layer**: Raw data storage.
2. **Silver Layer**: Transformed and cleaned data.
3. **Gold Layer**: Aggregated and enriched data for analytics.

## Technologies Used
- **Apache Spark**: Distributed data processing.
- **Delta Lake**: ACID-compliant data lake.
- **Kafka**: Real-time data streaming.
- **MinIO**: S3-compatible object storage.
- **Debezium**: Change data capture.
- **Superset**: Data visualization.
- **Trino**: SQL query engine.
- **Docker**: Containerization of services.
- **Python**: Core programming language for ETL and near realtime processes.

## Prerequisites
- Docker and Docker Compose installed.
- Python 3.11+ installed locally for development.
- `.env` file configured with necessary environment variables.

## Setup and Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/pnquang260805/SaleDataPipeline.git
   cd SaleDataPipeline
   ```
2. Create `.env` file with variables:
+ `ACCESS_KEY=your-access-key`: S3 access key. If you use MinIO, that is username
+ `SECRET_KEY=your-secret-key`: S3 secret key. If you use MinIO, that is password
+ `ENDPOINT=S3-endpoint`: S3 endpoint. If you use MinIO, that is `http://minio:9000`
3. Build and start the services using Docker Compose:
```bash
docker-compose up --build -d
```
5. Access the Nifi service: `https://localhost:8443` with username `admin` and password `nifi1234@Abc`. Then upload 2 files from folder `bootstraps` and run these group files.
6. Access other services:
+ Jupyter Notebook: `http://localhost:8888`
+ Superset: `http://localhost:8088`
+ Trino: `http://localhost:8086`
+ MinIO WebUI: `http://localhost:9001`

## Directory Structure
```
SalesPipeline/
├── batch_app/               # Batch processing application
├── debezium_init/           # Debezium initialization scripts
├── dockerfiles/             # Dockerfiles for various services
├── etc/                     # Configuration files for Trino
├── hive/                    # Hive configuration files
├── mysql_config/            # MySQL configuration files
├── bootstraps/              # NiFi bootstrap flows
├── conf/                    # Metastore configuration files
├── log/                     # Log files
├── docker-compose.yaml      # Docker Compose configuration
├── .env                     # Include environment variable
└── README.md                # Project documentation
```

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request.

# Postscript
Future updates will include technical documentation and dashboard screenshots. Stay tuned!