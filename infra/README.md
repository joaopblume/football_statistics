# Infrastructure Environment

This directory contains the infrastructure configuration for the data platform. 

## Structure

* **`minio/`**: Contains the Docker Compose configuration to deploy MinIO as our S3-compatible object storage (Data Lake).
* **`postgres/`**: Information regarding our PostgreSQL database.
* **`airflow/`**: Information regarding our Apache Airflow orchestrator.
* **`spark/`**: Contains the Docker Compose configuration to deploy a Spark cluster for data processing.
