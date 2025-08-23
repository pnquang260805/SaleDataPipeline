FROM flink:scala_2.12-java17

RUN mkdir -p /opt/flink/usrlib
RUN wget -P /opt/flink/usrlib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar