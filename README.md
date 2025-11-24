![ER Diagram](docs/ERD.png)
# Modern ELT Pipeline for Healthcare Data

This project shows a complete, end-to-end, and containerized ELT (Extract, Load, Transform) pipeline built with modern data engineering tools. It processes raw healthcare CSV data, loads it into PostgreSQL database(as a source) and then from PostgreSQL to a cloud data warehouse(BigQuery), transforms it into an analytics-ready format using dbt(Data Build Tool), and orchestrates the entire workflow using Apache Airflow.

## Architecture Diagram

CSV → Python Script → PostgreSQL → BigQuery (Raw → Staging → Marts) → dbt → Airflow


## Tech Stack

- Containerization:               Docker & Docker Compose
- Orchestration:                  Apache Airflow
- Data Ingestion:                 Python (Pandas, SQLAlchemy)
- Raw Data Store:                 PostgreSQL
- Cloud Data Warehouse:           Google BigQuery
- Data Transformation:            dbt (Data Build Tool)
- Data Quality:                   dbt tests, dbt-expectations
- Code Editor:                    VS Code

## Key Features

**Fully Containerized:** The entire stack runs in Docker, ensuring a reproducible and isolated development environment.
- **Layered Data Architecture:** Implements a robust Raw -> Staging -> Marts architecture in BigQuery.
- **Automated Transformations:** Uses dbt to transform raw data into clean, documented, and analytics-ready data models.
- **Data Quality Assurance:** Employs dbt tests to enforce data integrity, including uniqueness, non-null constraints, and referential integrity.
- **End-to-End Orchestration:** Uses Airflow to schedule and monitor the entire pipeline, from data ingestion to dbt transformations and testing.
- **Secure Credential Management:** Uses a `.gitignore` to prevent sensitive credentials from being committed to version control.


## How to Run

1. **Prerequisites:** Docker Desktop and Git installed.
2.  **Clone the repository:** `git clone https://github.com/mohsanbi/modern-elt-pipeline-healthcare`
3.  **GCP Setup:**
    - Create a Google Cloud Project and enable the BigQuery API.
    - Create a Service Account with `BigQuery Data Editor` and `BigQuery Job User` roles.
    - Download the JSON key file and save it as `gcp_credentials.json` in both the `script/` and `dbt/` directories.
4.  **Configuration:**
    - Replace `YOUR_GCP_PROJECT_ID` placeholders in `script/etl_pipeline.py`, `dbt/profiles.yml`, and `dbt/models/schema.yml`.
5.  **Build and Run:**
    
    docker-compose up --build -d
    
6.  **Access Services:**
    - **Airflow UI:** `http://localhost:8081` (user: admin, pass: admin)
    - **pgAdmin UI:** `http://localhost:8080` (user: admin@admin.com, pass: admin)


## Project Structure


healthcare_project/
- airflow/              # Airflow DAGs
- csv/                  # Source CSV data 
    (claims.csv , 
    payers.csv , 
    diagnoses.csv , 
    inventory.csv , 
    patients.csv , 
    departments.csv , 
    procedures.csv , 
    providers.csv , 
    encounters.csv)
- dbt/                  # dbt project for transformations and tests
- script/               # Python ETL script
- docker-compose.yml    # Defines all services
- README.md             # This file