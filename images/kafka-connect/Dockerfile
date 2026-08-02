FROM confluentinc/cp-kafka-connect:7.6.1

# JDBC Source Connector - polls a table/query on an interval and produces
# straight to Kafka. This is what does the Oracle/Postgres polling now,
# instead of custom Python code.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

# PostgreSQL JDBC driver - open source, safe to download at build time.
RUN curl -fsSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Oracle's ojdbc11.jar is NOT downloaded here - Oracle's license doesn't
# allow redistributing it in an image. Download it yourself from
# https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
# and drop it in producer/connect-drivers/ - it's mounted into the plugin
# path by docker-compose.yml.
