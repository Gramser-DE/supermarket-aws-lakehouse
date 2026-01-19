# Supermarket Real-Time Stock Lakehouse 

![Status](https://img.shields.io/badge/status-in--development-yellow)
![Tech](https://img.shields.io/badge/stack-Data%20Engineering-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## Business Problem
Retailers face significant losses due to **stockouts** (out-of-stock events) and **overstocking**. This project solves this by implementing a real-time stock management platform. By analyzing streaming sales events, the system provides immediate visibility and predictive alerts to ensure products are replenished exactly when needed.

## Architecture Diagram
![Architecture Diagram](./docs/architecture_diagram.png) *(Work in progress)*

## Architecture (Medallion + Lakehouse)
This project follows the **Medallion Architecture** to ensure data quality and traceability:
- **Bronze Layer**: Raw sales events stored in S3 (JSON format).
- **Silver Layer**: Cleaned and partitioned data using Spark (Parquet format).
- **Gold Layer**: Business-level aggregates modeled in a **Star Schema** served via PostgreSQL for low-latency analytical queries.

## Data Journey (The Pipeline)
1. **Producer**: A Python script simulates POS transactions (sales events) and streams them to **AWS Kinesis**.
2. **Ingestion**: Data is persisted from Kinesis into the **S3 Bronze Layer** (Raw JSON) for audit purposes.
3. **Orchestration**: **Mage AI** triggers Spark jobs to process new data.
4. **Transformation**: **PySpark** cleans the data, handles schemas, and saves it to the **S3 Silver Layer** (Parquet).
5. **Analytics**: Aggregated metrics (stock levels, sales velocity) are loaded into **PostgreSQL (Gold Layer)** using a Star Schema.
6. **Alerting**: The system identifies products below the safety stock threshold.

## Tech Stack
- **Cloud Simulation**: [LocalStack](https://localstack.cloud/) (S3, Kinesis, IAM).
- **Orchestration**: [Mage AI](https://www.mage.ai/) (Modern alternative to Airflow).
- **Data Processing**: [Apache Spark](https://spark.apache.org/) (PySpark).
- **Infrastructure as Code**: [Terraform](https://www.terraform.io/).
- **Database**: [PostgreSQL](https://www.postgresql.org/) (Serving layer).
- **Containerization**: [Docker](https://www.docker.com/) & Docker Compose.

## Setup & Execution

1. **Prerequisites**: Install **Docker** and **Docker Compose**. (Project tested on Windows via **WSL2**).
2. **Clone**: `git clone https://github.com/Gramser-DE/Supermarket-Stock-Lakehouse`
3. **Environment**: Create a `.env` file in the root directory (refer to `.env.example`): `cp .env.example .env`
4. **Launch Services**: Run `docker-compose up -d` to start the backend infrastructure (LocalStack, PostgreSQL, and Mage AI).
5. **Deploy Infrastructure**: Use the containerized Terraform to provision S3 buckets and Kinesis streams:`docker-compose -f docker-compose.infra.yml run --rm terraform init` and
   `docker-compose -f docker-compose.infra.yml run --rm terraform apply`
6. **Verification**: Confirm that the resources (buckets: bronze, silver, gold) are active in LocalStack using a containerized AWS CLI: `docker run --rm -it --network supermarket_net --env-file .env amazon/aws-cli --endpoint-url=http://localstack:4566 s3 ls`

## Project Milestones (3-Week Sprint)
### Week 1: Infrastructure & Ingestion
- [x] Local environment with Docker & LocalStack.
- [ ] IaC: Deploy S3 Buckets and Kinesis Streams with Terraform.
- [ ] Build the Python Sales Producer (Synthetic Data).
- [ ] Connect Kinesis to S3 Bronze.

### Week 2: Orchestration & Processing
- [ ] Setup Mage AI pipelines.
- [ ] Implement Spark transformations (Bronze to Silver).
- [ ] Data Quality checks and schema validation.

### Week 3: Serving & Analysis
- [ ] Model the Gold Layer in PostgreSQL (Fact & Dimension tables).
- [ ] Final end-to-end integration test.
- [ ] Documentation of architectural decisions.