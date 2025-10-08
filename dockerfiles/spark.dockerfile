# image này ko có entrypoint nên phải tự viết
FROM apache/spark:3.5.0-python3

USER root

# Install python 3.11
RUN apt-get update && \
    apt-get install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget -y && \
    apt-get clean

RUN wget https://www.python.org/ftp/python/3.11.13/Python-3.11.13.tgz && \
    tar -xf Python-3.11.13.tgz && \
    cd Python-3.11.13 && \
    ./configure --enable-optimizations && \
    make install && \
    cd .. && rm -rf Python-3.11.9 Python-3.11.9.tgz

COPY ./entrypoints/spark_entrypoint.sh /opt/spark_entrypoint.sh
RUN chmod +x /opt/spark_entrypoint.sh

ENTRYPOINT ["/opt/spark_entrypoint.sh"]