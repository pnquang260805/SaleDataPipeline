import os

import requests
from dotenv import load_dotenv

load_dotenv()
db_name = os.getenv("DB_NAME")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

DB_HOST_NAME = "backend_db"
PORT = "3306"
DEBEZIUM_HOST = "debezium-connect"
DEBEZIUM_PORT = "8083"
KAFKA_HOST = "kafka"
KAFKA_PORT = "9092"

data = {
    "name": "db-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": DB_HOST_NAME,
        "database.port": PORT,
        "database.user": db_username,
        "database.password": db_password,
        "database.dbname": db_name,
        "database.server.name": "maria_server",
        # "table.include.list": "public.orders",
        "database.server.id": "5401",
        "database.history.kafka.bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
        "database.history.kafka.topic": "schema-changes",
        "topic.prefix": "sales",  # tạo topic với prefix là sales
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": "schema-changes",
        "max.batch.size": "2048",
        "max.queue.size": "8192",
    },
}

headers = {"Content-Type": "application/json"}

try:
    req = requests.post(
        f"http://{DEBEZIUM_HOST}:{DEBEZIUM_PORT}/connectors", headers=headers, json=data
    )
    if req.status_code == 200:
        print("OK")
    else:
        print(f"Code {req.text}")
except Exception as e:
    print(e)
