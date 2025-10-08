#! /bin/bash

set -e 
echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
    echo "Starting Master ..."
    /opt/spark/sbin/start-master.sh -p 7077
    echo "Master has been started"
    tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master*.out # Cần để giữ cho container chạy dưới background và ko bị shutdown
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
    echo "Starting Worker ..."
    /opt/spark/sbin/start-worker.sh spark://spark-master:7077
    echo "Worker has been started"
    tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker*.out # Cần để giữ cho container chạy dưới background và ko bị shutdown
elif [ "$SPARK_WORKLOAD" == "history" ]; then
    echo "Starting History Server ..."
    /opt/spark/sbin/start-history-server.sh
    tail -f /opt/spark/logs/spark--org.apache.spark.deploy.history.HistoryServer*.out # Cần để giữ cho container chạy dưới background và ko bị shutdown
fi