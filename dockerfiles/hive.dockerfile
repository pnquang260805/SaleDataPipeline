FROM apache/hive:3.1.3

USER root

RUN apt-get update && apt-get install -y default-mysql-client && rm -rf /var/lib/apt/lists/*

COPY ./entrypoints/hive.sh /opt/hive/hive.sh
RUN chmod +x /opt/hive/hive.sh

USER hive

ENTRYPOINT [ "/opt/hive/hive.sh" ]
# CMD [ "tail", "-f", "/dev/null" ]