# Makefile for managing the local data lake infrastructure

# Define a phony target to ensure 'make' doesn't confuse these with file names
.PHONY: infra-up infra-down infra-restart infra-logs-spark infra-logs-minio help

# The default target when you just run 'make'
help:
	@echo "Available commands:"
	@echo "  make infra-up      - Starts MinIO and Spark containers in the background"
	@echo "  make infra-down    - Stops and removes MinIO and Spark containers"
	@echo "  make infra-restart - Restarts the entire infrastructure"
	@echo "  make logs-spark    - Follow the PySpark/Jupyter logs"
	@echo "  make logs-minio    - Follow the MinIO server logs"
	@echo "  make airflow-install-services - (Linux only) Installs Airflow systemd services"
	@echo "  make airflow-up    - Starts Airflow API server and scheduler via systemd"
	@echo "  make airflow-down  - Stops Airflow services"
	@echo "  make logs-airflow  - Follow Airflow systemd logs"

# Target to start the infrastructure
# Notice that MinIO is started first because Spark depends on its network
infra-up:
	@echo "Starting MinIO Data Lake..."
	cd infra/minio && docker compose up -d
	@echo "Starting Jupyter PySpark environment..."
	cd infra/spark && docker compose up -d
	@echo "Infrastructure is up and running!"
	@echo "You can check Jupyter logs with: make logs-spark"

# Target to stop the infrastructure
infra-down:
	@echo "Stopping Jupyter PySpark environment..."
	cd infra/spark && docker compose down
	@echo "Stopping MinIO Data Lake..."
	cd infra/minio && docker compose down
	@echo "Infrastructure stopped."

# Target to restart the infrastructure safely
infra-restart: infra-down infra-up

# Target to view Spark logs (useful for getting the Jupyter token)
logs-spark:
	cd infra/spark && docker compose logs -f jupyter-spark

# Target to view MinIO initialization logs
logs-minio:
	cd infra/minio && docker compose logs -f minio-mc

# ==========================================
# Airflow Systemd Management (Linux/VPS only)
# ==========================================

airflow-install-services:
	@echo "Installing Airflow systemd services (requires sudo)..."
	sudo cp infra/airflow/airflow-api-server.service /etc/systemd/system/
	sudo cp infra/airflow/airflow-scheduler.service /etc/systemd/system/
	sudo cp infra/airflow/airflow-dag-processor.service /etc/systemd/system/
	sudo cp infra/airflow/airflow.env /etc/default/airflow
	sudo systemctl daemon-reload
	sudo systemctl enable airflow-api-server
	sudo systemctl enable airflow-scheduler
	sudo systemctl enable airflow-dag-processor
	@echo "Services installed and enabled to start on boot!"

airflow-up:
	@echo "Starting Airflow services..."
	sudo systemctl start airflow-api-server airflow-scheduler airflow-dag-processor
	@echo "Airflow is running! View logs with: make logs-airflow"

airflow-down:
	@echo "Stopping Airflow services..."
	sudo systemctl stop airflow-api-server airflow-scheduler airflow-dag-processor
	@echo "Airflow stopped."

logs-airflow:
	@echo "Following logs for Airflow Scheduler, API Server, and DAG Processor..."
	sudo journalctl -u airflow-scheduler -u airflow-api-server -u airflow-dag-processor -f
