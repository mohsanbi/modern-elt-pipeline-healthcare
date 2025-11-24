import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import os
import time 

# --- PostgreSQL Configuration ---
# Add retry logic for PostgreSQL connection
MAX_RETRIES = 10
RETRY_DELAY_SECONDS = 5 # Wait 5 seconds before retring

postgres_engine = None
for i in range(MAX_RETRIES):
    try:
        print(f"Attempting to connect to PostgreSQL (Attempt {i+1}/{MAX_RETRIES})...")
        
        temp_engine = create_engine('postgresql://admin:admin@postgres:5432/hospital_db')
        # Test connection immediately
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
            raise # Re-raise the exception if max retries are exhausted

if postgres_engine is None:
    raise Exception("Failed to establish PostgreSQL connection after multiple retries.")


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

# --- BigQuery Configuration ---

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/app/gcp_credentials.json'
bigquery_client = bigquery.Client()
GCP_PROJECT_ID = 'healthcare-data-project-477711' # My PROJECT ID in GCP
BIGQUERY_DATASET = 'healthcare_data' # we Will create this dataset

def load_csv_to_postgres():
    print("Starting CSV to PostgreSQL Load...")
    for file, table in csv_files.items():
        df = pd.read_csv(f'/csv/{file}')
        df.to_sql(table, postgres_engine, if_exists='replace', index=False)
        print(f"✅ {file} → PostgreSQL table: {table}")
    print("🎉 All CSVs Uploaded to PostgreSQL Successfully!")

def load_postgres_to_bigquery():
    print("\nStarting PostgreSQL to BigQuery Load...")

    # Create BigQuery Dataset if it doesn't exist
    dataset_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}"
    try:
        bigquery_client.get_dataset(dataset_id)
        print(f"Dataset {BIGQUERY_DATASET} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US" # can region if needed
        bigquery_client.create_dataset(dataset, timeout=30)
        print(f"Dataset {BIGQUERY_DATASET} created.")

    for table_name in csv_files.values():
        print(f"Extracting data from PostgreSQL table: {table_name}...")
        df = pd.read_sql_table(table_name, postgres_engine)

        print(f"Loading data to BigQuery table: {BIGQUERY_DATASET}.{table_name}...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # Overwrite table if it exists
        job = bigquery_client.load_table_from_dataframe(
            df, f"{dataset_id}.{table_name}", job_config=job_config
        )
        job.result() # Wait for the job to complete
        print(f"✅ Data from PostgreSQL table '{table_name}' loaded to BigQuery table: '{BIGQUERY_DATASET}.{table_name}'")

    print("🎉 All PostgreSQL tables uploaded to BigQuery Successfully!")

if __name__ == "__main__":
    load_csv_to_postgres()
    load_postgres_to_bigquery()