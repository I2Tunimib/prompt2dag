from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import hashlib

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_claims():
    """Extract patient claims data from CSV file."""
    df = pd.read_csv('/path/to/claims.csv')
    return df.to_dict(orient='records')

def extract_providers():
    """Extract provider data from CSV file."""
    df = pd.read_csv('/path/to/providers.csv')
    return df.to_dict(orient='records')

def transform_join(**context):
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    claims_data = context['ti'].xcom_pull(task_ids='extract_claims')
    providers_data = context['ti'].xcom_pull(task_ids='extract_providers')
    
    claims_df = pd.DataFrame(claims_data)
    providers_df = pd.DataFrame(providers_data)
    
    merged_df = pd.merge(claims_df, providers_df, on='provider_id')
    
    # Anonymize patient identifiers
    merged_df['patient_id'] = merged_df['patient_id'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
    
    # Calculate risk scores
    merged_df['risk_score'] = merged_df['procedure_code'].apply(lambda x: len(x) * 10) + merged_df['amount']
    
    return merged_df.to_dict(orient='records')

def load_warehouse(**context):
    """Load transformed data to healthcare analytics warehouse."""
    transformed_data = context['ti'].xcom_pull(task_ids='transform_join')
    df = pd.DataFrame(transformed_data)
    df.to_sql('claims_fact', 'postgresql://user:password@warehouse_host:5432/warehouse_db', if_exists='append', index=False)

def refresh_bi():
    """Trigger refresh of BI dashboard tools."""
    # Placeholder for actual BI tool refresh logic
    print("Refreshing BI dashboards")

with DAG(
    'healthcare_claims_processing',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
) as dag:

    with TaskGroup('extract') as extract:
        extract_claims_task = PythonOperator(
            task_id='extract_claims',
            python_callable=extract_claims,
        )
        
        extract_providers_task = PythonOperator(
            task_id='extract_providers',
            python_callable=extract_providers,
        )
    
    transform_join_task = PythonOperator(
        task_id='transform_join',
        python_callable=transform_join,
        provide_context=True,
    )
    
    with TaskGroup('load') as load:
        load_warehouse_task = PythonOperator(
            task_id='load_warehouse',
            python_callable=load_warehouse,
            provide_context=True,
        )
        
        refresh_bi_task = PythonOperator(
            task_id='refresh_bi',
            python_callable=refresh_bi,
        )
    
    extract >> transform_join_task >> load