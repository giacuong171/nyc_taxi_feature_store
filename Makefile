datalake_up:
	docker compose -f datalake/minio_docker-compose.yml up -d
datalake_down:
	docker compose -f datalake/minio_docker-compose.yml down
datalake_restart:
	docker compose -f datalake/minio_docker-compose.yml down
	docker compose -f datalake/minio_docker-compose.yml up -d
airflow_up:
	docker compose -f airflow-docker-compose.yaml up -d
airflow_down:
	docker compose -f airflow-docker-compose.yaml down
kafka_up:
	docker compose -f stream_processing/kafka/docker-compose.yml up -d
kafka_down:
	docker compose -f stream_processing/kafka/docker-compose.yml down
monitoring_up:
	docker compose -f monitoring/prom-graf-docker-compose.yaml up -d
monitoring_down:
	docker compose -f monitoring/prom-graf-docker-compose.yaml down
monitoring_restart:
	docker compose -f monitoring/prom-graf-docker-compose.yaml restart
elk_up:
	cd monitoring/elk  && docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml up -d
elk_down:
	cd monitoring/elk  && docker compose -f elk-docker-compose.yml -f extensions/filebeat/filebeat-compose.yml down
warehouse_up:
	docker compose -f postgresql-docker-compose.yaml up -d
warehouse_down:
	docker compose -f postgresql-docker-compose.yaml down
