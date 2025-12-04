# Data Flow Project
[![Ask DeepWiki](https://devin.ai/assets/askdeepwiki.png)](https://deepwiki.com/ImGagn/Data_Flow_Project)

This project demonstrates a complete real-time data pipeline that fetches user data from a public API, streams it through Apache Kafka, processes it with Apache Spark, and stores it in Apache Cassandra. The entire workflow is orchestrated by Apache Airflow and containerized using Docker.

## Architecture

The data flows through the system as follows:

**RandomUser API → Airflow (Producer) → Kafka → Spark (Consumer) → Cassandra**

### Components

*   **Docker Compose**: Defines and manages the multi-container application stack, including all the services listed below.
*   **Apache Airflow**: Orchestrates the data pipeline. It schedules and runs a Python script to periodically fetch data from the source API.
*   **Apache Kafka**: Acts as a distributed, real-time message bus. It decouples the data producer (Airflow) from the data consumer (Spark).
*   **Confluent Stack**: Includes Zookeeper for Kafka coordination, Schema Registry for managing data schemas, and Control Center for monitoring the Kafka cluster.
*   **Apache Spark**: A distributed data processing engine. It consumes the data stream from Kafka in real-time, applies a schema, and prepares it for storage.
*   **Apache Cassandra**: A distributed NoSQL database used as the data sink to store the final processed user information.

## Prerequisites

*   Docker
*   Docker Compose

## Configuration

Before running the project, you need to update the service hostnames in the producer and consumer scripts to match the service names defined in `docker-compose.yml`.

1.  **Airflow Kafka Producer (`dags/kafka_streams.py`)**

    In the `stream_data` function, change the Kafka bootstrap server from `broker:29092` to the correct internal listener `broker:29892`.

    ```python
    # dags/kafka_streams.py

    # Change this line
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    # To this
    producer = KafkaProducer(bootstrap_servers=['broker:29892'], max_block_ms=5000)
    ```

2.  **Spark Stream Consumer (`spark_stream.py`)**

    Update the hostnames for Kafka and Cassandra to point to the respective Docker services.

    ```python
    # spark_stream.py

    # In create_spark_connection(), change 'localhost' to 'cassandra'
    .config('spark.cassandra.connection.host', 'cassandra') \

    # In connect_kafka(), change 'localhost:9092' to 'broker:29892'
    .option('kafka.bootstrap.servers', 'broker:29892') \

    # In create_cassandra_connection(), change 'localhost' to 'cassandra'
    cluster = Cluster(['cassandra'])
    ```

## How to Run

1.  **Clone the Repository**

    ```sh
    git clone https://github.com/ImGagn/Data_Flow_Project.git
    cd Data_Flow_Project
    ```

2.  **Start Services**

    Launch all services in the background using Docker Compose.

    ```sh
    docker-compose up -d
    ```

    This will build and start containers for Airflow, Kafka, Spark, Cassandra, and other required services.

3.  **Access Service UIs**

    *   **Airflow UI**: `http://localhost:8080` (Login with `admin`/`admin`)
    *   **Spark Master UI**: `http://localhost:9090`
    *   **Confluent Control Center**: `http://localhost:9021`

4.  **Run the Pipeline**

    a. **Start the Data Stream (Airflow)**

    *   Navigate to the Airflow UI at `http://localhost:8080`.
    *   Find the `user_automation` DAG.
    *   Unpause the DAG using the toggle on the left.
    *   Trigger the DAG manually by clicking the "play" button on the right. This will start fetching data from the API and sending it to the `user-created` Kafka topic.

    b. **Start the Spark Streaming Job**

    *   First, copy the Spark script into the Spark master container.

        ```sh
        docker cp spark_stream.py $(docker-compose ps -q spark-master):/opt/bitnami/spark/
        ```

    *   Access the Spark master container's shell.

        ```sh
        docker exec -it $(docker-compose ps -q spark-master) /bin/bash
        ```

    *   Inside the container, submit the Spark application. This command includes the necessary packages for Kafka and Cassandra integration.

        ```sh
        ./bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
        spark_stream.py
        ```

    The Spark job will connect to Kafka, start processing the stream, and write the data to Cassandra.

## Verifying the Output

You can verify that data is being stored in Cassandra by connecting to the database and querying the table.

1.  **Access Cassandra Shell (cqlsh)**

    ```sh
    docker exec -it cassandra cqlsh -u cassandra -p cassandra
    ```

2.  **Query the Data**

    Once inside `cqlsh`, you can query the `created_users` table.

    ```sql
    DESCRIBE KEYSPACES;

    USE spark_stream;

    SELECT * FROM created_users LIMIT 5;
    ```

    You should see the user data that has been processed by the pipeline.
