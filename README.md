# Real-Time Data Streaming Pipeline with Kafka, Spark, and Cassandra

This project demonstrates a real-time data streaming pipeline that fetches user data from an external API, processes it with Apache Kafka and Apache Spark, and stores it in Apache Cassandra. The entire workflow is orchestrated by Apache Airflow and containerized using Docker for easy setup and deployment.

![alt text](<Blank diagram.png>)

## System Architecture

The project follows a modern data engineering architecture, integrating several key technologies to handle real-time data streams:

*   **Docker**: All components of the pipeline are containerized using Docker and managed with Docker Compose, ensuring a reproducible and isolated environment.
*   **Apache Airflow**: Serves as the workflow management platform. It orchestrates the entire pipeline, scheduling a Directed Acyclic Graph (DAG) to fetch data from an API and stream it to Kafka.
*   **Apache Kafka**: Acts as the central nervous system of the pipeline. It is a distributed streaming platform that ingests and stores real-time data feeds. The setup also includes:
    *   **Zookeeper**: Manages the Kafka cluster's state.
    *   **Schema Registry**: Ensures data schema consistency across the pipeline.
    *   **Control Center**: Provides a web-based interface for monitoring and managing the Kafka cluster.
*   **Apache Spark**: A unified analytics engine for large-scale data processing. In this pipeline, a Spark Streaming job consumes data from a Kafka topic, performs transformations, and writes the results to Cassandra.
*   **Apache Cassandra**: A highly scalable, distributed NoSQL database used as the data sink. It stores the processed user data, making it available for further analysis or application use.

## Project Structure

```
data-engineering-project1
├── dags
│   ├── kafka_producer.py
│   └── __pycache__
├── logs
├── plugins
├── config
├── spark_stream.py
├── requirements.txt
├── README.md
├── Dockerfile.spark
├── Dockerfile.airflow
├── docker-compose.yml
└── .env
```

## Core Technologies

*   **Orchestration**: Apache Airflow
*   **Streaming**: Apache Kafka
*   **Data Processing**: Apache Spark
*   **Database**: Apache Cassandra
*   **Containerization**: Docker, Docker Compose
*   **Programming Language**: Python

## Deployment Steps

To deploy and run this project, follow these steps:

### Prerequisites

*   Docker
*   Docker Compose

### 1. Set Up Environment File

Create a `.env` file in the project's root directory and add the following line. This helps in setting the user ID for Airflow to avoid permission issues.

```bash
AIRFLOW_UID=1000
```

### 2. Build and Run the Containers

Open a terminal in the project's root directory and run the following command to build and start all the services in detached mode:

```bash
docker-compose up -d --build
```

### 3. Verify the Services

To check if all the containers are running, use the command:

```bash
docker ps
```

You should see all the services defined in the `docker-compose.yaml` file (e.g., zookeeper, broker, spark-master, cassandra_db, airflow-webserver, etc.) with a status of `Up`.

### 4. Access Airflow and Start the DAG

1.  Open your web browser and navigate to `http://localhost:8080`.
2.  Log in with the default credentials:
    *   **Username**: airflow
    *   **Password**: airflow
3.  On the Airflow UI, find the `user_automation` DAG and enable it by clicking the toggle switch. The DAG will start its first run based on its schedule (`@daily`). You can also trigger it manually.

### 5. Monitor the Pipeline

*   **Kafka**: Use the Confluent Control Center at `http://localhost:9021` to monitor topics and messages.
*   **Spark**: Access the Spark Master UI at `http://localhost:9090` to see the running Spark application.
*   **Cassandra**: To verify that the data is being written to Cassandra, you can connect to the Cassandra container and query the table:

    ```bash
    # Get the container ID for Cassandra
    docker ps | grep cassandra

    # Connect to the Cassandra container
    docker exec -it <cassandra_container_id> cqlsh

    # In the CQL shell, run the following commands:
    USE spark_streams;
    SELECT * FROM created_users LIMIT 10;
    ```

## Code Explanation

### `kafka_producer.py`

This Python script defines an Apache Airflow DAG. The DAG consists of a single task that uses the `PythonOperator` to execute the `stream_data` function. This function:
*   Fetches random user data from the `https://randomuser.me/api/`.
*   Formats the JSON response into a structured dictionary.
*   Uses the `kafka-python` library to create a `KafkaProducer` and sends the formatted data to the `users_created` topic on the Kafka broker.

### `spark_stream.py`

This is a PySpark application that processes the data stream. Key functionalities include:
*   **`create_spark_connection`**: Establishes a Spark session with the necessary packages for Kafka and Cassandra integration.
*   **`connect_to_kafka`**: Connects to the Kafka broker and subscribes to the `users_created` topic to create a streaming DataFrame.
*   **`create_cassandra_connection`, `create_keyspace`, `create_table`**: Manages the connection to Cassandra and ensures that the required keyspace and table exist.
*   **`create_selection_df_from_kafka`**: Parses the JSON data from the Kafka stream, applies a schema, and selects the relevant fields.
*   The main part of the script orchestrates these functions and starts a streaming query that writes the processed data from Spark to the Cassandra table.

### `docker-compose.yaml`

This file defines and configures all the services required for the project. It sets up the network, volumes, and dependencies between the services, making it easy to launch the entire multi-container application with a single command.

### Dockerfiles

*   **`Dockerfile.airflow`**: Builds a custom Airflow image, installing the Python dependencies specified in `requirements.txt`.
*   **`Dockerfile.spark`**: Creates a custom Spark image that includes the `spark_stream.py` script and installs the `cassandra-driver`. This image is used by the `spark-submitter` service to run the Spark job.

### `requirements.txt`

This file lists the Python packages that need to be installed in the Airflow environment, specifically `kafka-python` for producing messages to Kafka and `requests` for making API calls.# Real-Time-Data-Streaming-Pipeline-with-Kafka-Spark-Airflow-and-Cassandra
