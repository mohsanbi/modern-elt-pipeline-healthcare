# D:\healthcare_project\airflow\healthcare_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration

GCP_CREDENTIALS_PATH = '/opt/airflow/gcp_credentials.json'  # Mounted into container
GCP_PROJECT_ID = 'healthcare-data-project-477711'
BIGQUERY_DATASET = 'healthcare_data'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'email': ['xxxxx@gmail.com']
}


# DAG Definition

with DAG(
    dag_id='healthcare_etl_dbt_pipeline',
    default_args=default_args,
    description='ETL from CSV -> PostgreSQL -> BigQuery and dbt transformations/tests',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['healthcare', 'etl', 'dbt'],
) as dag:

    #  ETL Script: CSV -> PostgreSQL -> BigQuery
    run_etl_script = BashOperator(
        task_id='run_etl_pipeline',
        bash_command=f"""
            export GOOGLE_APPLICATION_CREDENTIALS={GCP_CREDENTIALS_PATH} &&
            export GCP_PROJECT_ID={GCP_PROJECT_ID} &&
            export BIGQUERY_DATASET={BIGQUERY_DATASET} &&
            python /opt/airflow/scripts/etl_pipeline.py
        """,
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': GCP_CREDENTIALS_PATH,
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'BIGQUERY_DATASET': BIGQUERY_DATASET
        }
    )
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_models',
        bash_command=f"""
            docker exec hospital_dbt bash -c "
                export GOOGLE_APPLICATION_CREDENTIALS=/usr/app/gcp_credentials.json &&
                cd /usr/app &&
                dbt run --profiles-dir /usr/app --target dev
            "
        """
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f"""
            docker exec hospital_dbt bash -c "
                export GOOGLE_APPLICATION_CREDENTIALS=/usr/app/gcp_credentials.json &&
                cd /usr/app &&
                dbt test --profiles-dir /usr/app --target dev
            "
        """
    )
    # Task Dependencies
 
    run_etl_script >> run_dbt_transformations >> run_dbt_tests
