import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import os
import time

# ----------------------------
# Configuration via Environment Variables
# ----------------------------

# PostgreSQL
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://admin:admin@postgres:5432/hospital_db")

# CSV folder
CSV_FOLDER = os.environ.get("CSV_FOLDER", "/csv")

# GCP / BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/app/gcp_credentials.json")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "healthcare-data-project-477711")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "healthcare_data")

# Retry settings for Postgres
MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 5

# CSV to table mapping
csv_files = {
    'claims.csv': 'claims',
    'payers.csv': 'payers',
    'diagnoses.csv': 'diagnoses',
    'inventory.csv': 'inventory',
    'patients.csv': 'patients',
    'departments.csv': 'departments',
    'procedures.csv': 'procedures',
    'providers.csv': 'providers',
    'encounters.csv': 'encounters'
}

# ----------------------------
# Connect to PostgreSQL with retry
# ----------------------------
postgres_engine = None
for i in range(MAX_RETRIES):
    try:
        print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{MAX_RETRIES})...")
        temp_engine = create_engine(POSTGRES_URI)
        temp_engine.connect().close()
        postgres_engine = temp_engine
        print("✅ Successfully connected to PostgreSQL!")
        break
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
        if i < MAX_RETRIES - 1:
            print(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            print("❌ Max retries reached. Could not connect to PostgreSQL.")
            raise

if postgres_engine is None:
    raise Exception("Failed to establish PostgreSQL connection after multiple retries.")

# ----------------------------
# Connect to BigQuery
# ----------------------------
bigquery_client = bigquery.Client()

# ----------------------------
# Functions
# ----------------------------
def load_csv_to_postgres():
    print("Starting CSV to PostgreSQL Load...")
    for file, table in csv_files.items():
        csv_path = os.path.join(CSV_FOLDER, file)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        df = pd.read_csv(csv_path)
        df.to_sql(table, postgres_engine, if_exists='replace', index=False)
        print(f"✅ {file} → PostgreSQL table: {table}")
    print("🎉 All CSVs uploaded to PostgreSQL successfully!")

def load_postgres_to_bigquery():
    print("\nStarting PostgreSQL to BigQuery Load...")

    dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"

    # Create dataset if it doesn't exist
    try:
        bigquery_client.get_dataset(dataset_id)
        print(f"Dataset {BIGQUERY_DATASET} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        bigquery_client.create_dataset(dataset, timeout=30)
        print(f"Dataset {BIGQUERY_DATASET} created.")

    for table_name in csv_files.values():
        print(f"Extracting data from PostgreSQL table: {table_name}...")
        df = pd.read_sql_table(table_name, postgres_engine)

        print(f"Loading data to BigQuery table: {BIGQUERY_DATASET}.{table_name}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bigquery_client.load_table_from_dataframe(
            df, f"{dataset_id}.{table_name}", job_config=job_config
        )
        job.result()
        print(f"✅ Data from PostgreSQL table '{table_name}' loaded to BigQuery table '{BIGQUERY_DATASET}.{table_name}'")

    print("🎉 All PostgreSQL tables uploaded to BigQuery successfully!")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    load_csv_to_postgres()
    load_postgres_to_bigquery()
