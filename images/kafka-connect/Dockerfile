FROM confluentinc/cp-kafka-connect:7.7.1

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.4

USER root

COPY kafka/plugins/oracle/*.jar /etc/kafka-connect/jars/
COPY kafka/plugins/postgres/*.jar /etc/kafka-connect/jars/

USER appuser