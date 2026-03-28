#  Data-Lakehouse-VN30-With-MinIO-Spark-Delta-Airflow

Data Lakehouse system designed to collect, process, and analyze VN30 stock data from the Vietnamese market. This project implements a modern Medallion Architecture (Bronze, Silver, Gold) using MinIO, Apache Spark, Delta Lake, and Airflow, all orchestrated via Docker.



##  Architecture: 
The architecture is designed to be scalable, modular, and robust, leveraging open-source technologies.

<img width="1050" height="676" alt="{E599B948-29DD-4415-BBBF-378BED8B41BF}" src="https://github.com/user-attachments/assets/48c7f277-2e9b-4c70-ac0b-2df0cfb8d729" />

The main components of the architecture are:

**Infrastructure (Docker):**
The entire platform is containerized using Docker and managed with Docker Compose, ensuring portability, reproducibility, and ease of deployment across different environments.

**Orchestration and Monitoring (Apache Airflow):**
Apache Airflow is used to schedule, orchestrate, and monitor end-to-end data pipelines, including data ingestion, ETL processing, and data quality validation tasks.

**Object Storage (MinIO & Delta Lake):**
MinIO provides an S3-compatible object storage solution as the foundation of the data lake.
Delta Lake is implemented on top of MinIO to provide ACID transactions, schema enforcement, time travel, improved reliability, and high-performance data processing.

The storage layer follows the **Medallion Architecture**, organized into three tiers:

* **Bronze:** Raw, unprocessed data ingested directly from source systems.
* **Silver:** Cleaned, validated, and enriched datasets ready for analytical processing.
* **Gold:** Aggregated and business-level datasets optimized for analytics and business intelligence.

**Metadata Management (Hive Metastore):**
Hive Metastore serves as the centralized metadata repository, storing table schemas, database definitions, and partition information. This enables Spark and other query engines to access a consistent and unified schema layer while the actual data resides in object storage.

**Compute & Query Engine (Apache Spark):**
Apache Spark acts as the distributed compute engine of the Lakehouse architecture. It is responsible for large-scale data processing, ETL transformations across Bronze, Silver, and Gold layers, and executing analytical queries.

Spark integrates seamlessly with Delta Lake and Hive Metastore, enabling:

* Distributed and scalable batch data processing
* SQL-based analytics via Spark SQL
* Efficient transformation pipelines from raw to curated datasets
* Optimized performance through partitioning and parallel execution



## Data Flow

1. **Ingestion:** Data is ingested from source systems into the Bronze layer stored in MinIO. This process is orchestrated by an Apache Airflow DAG.

2. **ETL Processing:** Airflow triggers Apache Spark jobs to perform transformations:

   * **Bronze to Silver:** Raw data is cleaned, validated, deduplicated, and enriched using Delta Lake.
   * **Silver to Gold:** Silver data is aggregated and modeled into analytics-ready business tables.

3. **Data Quality:** Data quality checks are executed during ETL to detect anomalies, schema changes, and data inconsistencies, ensuring reliability across all layers.

4. **Metadata Management:** Table schemas and partitions are registered in Hive Metastore, enabling centralized metadata management and SQL-based access.

5. **Analytics:** Business users and analysts can query curated Gold datasets using Spark SQL or connected BI tools to generate reports and insights.






## Tech Stack

-   **Orchestration**: Apache Airflow 2.10.4
-   **Processing**: Apache Spark 3.5 (PySpark)
-   **Storage**: MinIO (S3-Compatible Object Storage)
-   **Table Format**: Delta Lake (ACID transactions, Time Travel)
-   **Metastore**: Hive Metastore (Standalone) with PostgreSQL backend
-   **Containerization**: Docker & Docker Compose



## Project Structure

```text
lake_house_prj/
├── airflow/            # Airflow DAGs & logs
├── config/             # Centralized configuration
│   ├── hive/           # hive-site.xml (Metastore config)
│   └── spark/          # spark-defaults.conf (S3/Delta/Hive settings)
├── src/                # Source code
│   ├── ingestion/      # Data landing & API helpers
│   ├── jobs/           # Spark transformation jobs (B -> S -> G)
│   └── main.py         # Ingestion entry point
├── docker-compose.yaml # Infrastructure orchestration
├── requirements.txt    # Python dependencies
├── .gitignore          # Git ignore rules
└── README.md           # Project documentation
<<<<<<< HEAD
```
---
=======

>>>>>>> 70d57fe (Update)

## How to Run

### 1. Launch Infrastructure

Start all services (MinIO, Spark, Airflow, Hive, Postgres):

```bash
docker-compose up -d
```

* **MinIO Console:** [http://localhost:9001](http://localhost:9001) (minioadmin/minioadmin)
* **Airflow UI:** [http://localhost:8080](http://localhost:8080) (admin/admin)
* **Spark UI:** [http://localhost:9090](http://localhost:9090)

---

### 2. Run the Pipeline

The pipeline is fully automated via Airflow.

Log in to the Airflow UI and trigger the `stock_analyzer_pipeline` DAG.

#### Manual Execution (Alternative)

**Ingest to Bronze**

```bash
docker exec airflow-webserver python /opt/airflow/app/main.py
```

**Transform Bronze → Silver**

```bash
docker exec spark-master-2 spark-submit \
/opt/bitnami/spark/app/jobs/bronze_to_silver.py \
  --bronze_input_path s3a://bronze/stock_data/vn30/ \
  --silver_output_path s3a://silver/stock_data/vn30/
```

---

## Demonstration

---

### 1. Medallion ETL Pipeline

The main ETL pipeline is orchestrated by Apache Airflow.
This DAG is responsible for ingesting raw data and processing it through the Medallion architecture layers.

<p align="center">
  <img width="966" src="https://github.com/user-attachments/assets/1a36f2ae-28f6-46cc-9421-7f8513627096" />
</p>

---

1. **ingest_stock_data_to_bronze**
   Raw stock data is extracted from the source and stored in the **Bronze layer** in MinIO.

2. **transform_bronze_to_silver**
   Apache Spark cleans, validates, and enriches raw data, then writes refined datasets to the **Silver layer** using Delta Lake.

3. **transform_silver_to_gold**
   Silver data is aggregated and modeled into analytics-ready business tables in the **Gold layer**.

Tasks run sequentially to ensure reliable and consistent data transformation from raw ingestion to curated outputs.

---

### 2. Data Storage in MinIO

After the pipeline runs, the processed data is stored in MinIO, organized by Medallion layers (Bronze, Silver, Gold).

<p align="center">
  <img width="100%" src="https://github.com/user-attachments/assets/9b098899-1b23-444a-ae79-6cdf5d162eff" />
</p>

---
