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
