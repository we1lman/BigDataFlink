FROM flink:1.18.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q -P /opt/flink/lib \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" && \
    wget -q -P /opt/flink/lib \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" && \
    wget -q -P /opt/flink/lib \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"

RUN pip3 install --no-cache-dir apache-flink==1.18.1

COPY flink_star_schema.py /opt/flink-job/flink_star_schema.py

USER flink
