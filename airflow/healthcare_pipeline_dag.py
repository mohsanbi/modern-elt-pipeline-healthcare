# D:\healthcare_project\airflow\healthcare_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Define environment variables for GCP credentials and project ID
# These are mounted into the Airflow container
GCP_CREDENTIALS_PATH = '/opt/airflow/gcp_credentials.json'

GCP_PROJECT_ID = 'healthcare-data-project-477711' 
BIGQUERY_DATASET = 'healthcare_data'


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'email': ['mrmoh78600@gmail.com']
}

with DAG(
    dag_id='healthcare_etl_dbt_pipeline',
    default_args=default_args,
    description='A DAG to run ETL from CSV to BigQuery and then dbt transformations',
    schedule_interval=None, # Set to None for manual trigger, or a cron string for scheduled runs
    catchup=False,
    tags=['healthcare', 'etl', 'dbt'],
) as dag:
    # 1. Task to run the Python ETL script (CSV -> PostgreSQL -> BigQuery)
    # The script is located in /opt/airflow/scripts inside the container
    run_etl_script = BashOperator(
        task_id='run_etl_pipeline',
        bash_command=f"""
            export GOOGLE_APPLICATION_CREDENTIALS={GCP_CREDENTIALS_PATH} &&
            export GCP_PROJECT_ID={GCP_PROJECT_ID} &&
            export BIGQUERY_DATASET={BIGQUERY_DATASET} &&
            python /opt/airflow/scripts/etl_pipeline.py
        """,
        env={ # Pass environment variables directly to the bash command as well
            'GOOGLE_APPLICATION_CREDENTIALS': GCP_CREDENTIALS_PATH,
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'BIGQUERY_DATASET': BIGQUERY_DATASET
        }
    )
  
    run_dbt_tests = BashOperator( 
        task_id='run_dbt_tests',
        bash_command=f"""
            cd /opt/airflow/dbt_project &&
            export GOOGLE_APPLICATION_CREDENTIALS={GCP_CREDENTIALS_PATH} &&
            dbt deps && # Ensure packages are installed for tests
            dbt test --profile healthcare_project --target dev
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': GCP_CREDENTIALS_PATH
        }
    )

    # 2. #This will run dbt transformation once the etl script executed successfully
    # The dbt project is located in /opt/airflow/dbt_project inside the container
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_models',
        bash_command=f"""
            cd /opt/airflow/dbt_project &&
            export GOOGLE_APPLICATION_CREDENTIALS={GCP_CREDENTIALS_PATH} &&
            dbt debug --profile healthcare_project --target dev && # Optional: for debugging connection
            dbt run --profile healthcare_project --target dev
        """,
        env={ 
            'GOOGLE_APPLICATION_CREDENTIALS': GCP_CREDENTIALS_PATH
        }
    )

    # Define task dependencies
    run_etl_script >> run_dbt_transformations >> run_dbt_tests 