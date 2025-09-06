#!/bin/bash
set -e

# Thông tin MySQL
MYSQL_HOST=${MYSQL_HOST:-metastore-db}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_DB=${MYSQL_DB:-metastore}
MYSQL_USER=${MYSQL_USER:-hive}
MYSQL_PASS=${MYSQL_PASS:-hivepassword}

# Kiểm tra schema đã tồn tại chưa
TABLE_COUNT=$(mysql -h $MYSQL_HOST -P $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASS -D $MYSQL_DB -sse "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$MYSQL_DB';")

if [ "$TABLE_COUNT" -eq 0 ]; then
    echo "Init Schema"
    /opt/hive/bin/schematool -dbType mysql -initSchema
else
    echo "Skip init"
fi

# Khởi động Hive Metastore
hive --service metastore
