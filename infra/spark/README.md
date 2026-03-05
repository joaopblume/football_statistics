# Apache Spark

This directory contains the Docker Compose configuration to spin up a standalone Apache Spark cluster for data processing.

## Configuration

The `docker-compose.yaml` file defines a Spark cluster consisting of:
* **Spark Master**: The cluster manager (UI available on port 8080).
* **Spark Worker**: A single worker node to execute tasks (UI available on port 8081).

*Note: You can scale the number of workers by duplicating the worker block inside the docker-compose file and simply changing its name and port.*

## How to Run

1. Open your terminal and navigate to this directory.
2. Run the following command to start the Spark cluster in the background:

```bash
# Start the Spark cluster in detached mode
docker-compose up -d
```

3. To check the logs, you can use:

```bash
# View and follow logs from the Spark cluster containers
docker-compose logs -f
```

4. To stop the cluster, run:

```bash
# Stop the Spark cluster and remove the containers
docker-compose down
```

## Accessing the UI

* **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
* **Spark Worker UI**: [http://localhost:8081](http://localhost:8081)
