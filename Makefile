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