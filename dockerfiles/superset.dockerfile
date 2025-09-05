FROM apache/superset:GHA-16814370361

USER root

COPY ./entrypoints/superset.sh .
RUN chmod u+x ./superset.sh
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyhive[hive] \
    pyhive[spark] \
    thrift \
    thrift-sasl \
    sasl
ENTRYPOINT ["./superset.sh"]

USER 1001