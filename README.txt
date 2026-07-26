
-- after changing any dockerfile content or requirements.txt -------
docker build -t dhonuk-producer:1.0 -f images/producer/Dockerfile .
docker build -t dhonuk-airflow:3.1.6 -f images/airflow/Dockerfile .



-- after building run the following

docker compose up -d

-- git ssh push