import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'healthcare-analytics',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def hash_pii(value: str) -> str:
    """Hash PII value using SHA-256."""
    if not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:16]

def calculate_risk_score(row: Dict[str, Any]) -> int:
    """Calculate risk score based on procedure code and amount."""
    amount = float(row.get('claim_amount', 0))
    procedure_code = str(row.get('procedure_code', ''))
    
    score = 0
    if amount > 10000:
        score += 3
    elif amount > 5000:
        score += 2
    elif amount > 1000:
        score += 1
    
    if procedure_code.startswith('99'):
        score += 3
    elif procedure_code.startswith('88'):
        score += 2
    
    return min(score, 5)

def extract_claims(**context):
    """Extract patient claims data from CSV file."""
    # Production: pd.read_csv('/path/to/claims.csv')
    claims_data = {
        'claim_id': ['C001', 'C002', 'C003'],
        'patient_id': ['P001', 'P002', 'P003'],
        'provider_id': ['PRV001', 'PRV002', 'PRV001'],
        'procedure_code': ['99213', '88720', '99215'],
        'claim_amount': [150.00, 7500.00, 250.00],
        'claim_date': ['2024-01-01', '2024-01-02', '2024-01-03']
    }
    df = pd.DataFrame(claims_data)
    return df.to_json(orient='records', date_format='iso')

def extract_providers(**context):
    """Extract provider data from CSV file."""
    # Production: pd.read_csv('/path/to/providers.csv')
    providers_data = {
        'provider_id': ['PRV001', 'PRV002'],
        'provider_name': ['Dr. Smith', 'Dr. Johnson'],
        'specialty': ['Cardiology', 'Oncology'],
        'npi': ['1234567890', '0987654321']
    }
    df = pd.DataFrame(providers_data)
    return df.to_json(orient='records', date_format='iso')

def transform_join(**context):
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    ti = context['task_instance']
    claims_json = ti.xcom_pull(task_ids='extract_stage.extract_claims')
    providers_json = ti.xcom_pull(task_ids='extract_stage.extract_providers')
    
    if not claims_json or not providers_json:
        raise ValueError("Missing required data from extract tasks")
    
    claims_df = pd.read_json(claims_json)
    providers_df = pd.read_json(providers_json)
    
    joined_df = claims_df.merge(providers_df, on='provider_id', how='inner')
    joined_df['patient_id_hashed'] = joined_df['patient_id'].apply(hash_pii)
    joined_df.drop('patient_id', axis=1, inplace=True)
    joined_df['risk_score'] = joined_df.apply(calculate_risk_score, axis=1)
    
    return joined_df.to_json(orient='records', date_format='iso')

def load_warehouse(**context):
    """Load transformed data to healthcare analytics warehouse."""
    ti = context['task_instance']
    transformed_json = ti.xcom_pull(task_ids='transform_join')
    
    if not transformed_json:
        raise ValueError("No transformed data available")
    
    df = pd.read_json(transformed_json)
    pg_hook = PostgresHook(postgres_conn_id='healthcare_warehouse_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Load claims fact table
    claims_fact_df = df[[
        'claim_id', 'provider_id', 'patient_id_hashed',
        'procedure_code', 'claim_amount', 'claim_date', 'risk_score'
    ]].copy()
    
    claims_fact_df.to_sql(
        'claims_fact',
        engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    
    # Load provider dimension table (upsert)
    providers_dim_df = df[[
        'provider_id', 'provider_name', 'specialty', 'npi'
    ]].drop_duplicates()
    
    providers_dim_df.to_sql(
        'providers_dim_temp',
        engine,
        if_exists='replace',
        index=False
    )
    
    upsert_sql = """
    INSERT INTO providers_dim (provider_id, provider_name, specialty, npi)
    SELECT provider_id, provider_name, specialty, npi
    FROM providers_dim_temp
    ON CONFLICT (provider_id) DO UPDATE SET
        provider_name = EXCLUDED.provider_name,
        specialty = EXCLUDED.specialty,
        npi = EXCLUDED.npi;
    """
    pg_hook.run(upsert_sql)
    pg_hook.run("DROP TABLE IF EXISTS providers_dim_temp;")
    
    return f"Loaded {len(claims_fact_df)} claims and {len(providers_dim_df)} providers"

def refresh_bi(**context):
    """Trigger refresh of BI dashboard tools."""
    # Production: Call Power BI and Tableau APIs here
    print("Triggering Power BI dataset refresh...")
    print("Triggering Tableau workbook refresh...")
    return "BI refresh triggered successfully"

with DAG(
    dag_id='healthcare_claims_processing_etl',
    default_args=default_args,
    description='Healthcare claims processing ETL pipeline with staged extraction, transformation, and loading',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['healthcare', 'etl', 'claims'],
    max_active_runs=1,
) as dag:
    
    with TaskGroup('extract_stage') as extract_stage:
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
    )
    
    with TaskGroup('load_stage') as load_stage:
        load_warehouse_task = PythonOperator(
            task_id='load_warehouse',
            python_callable=load_warehouse,
        )
        
        refresh_bi_task = PythonOperator(
            task_id='refresh_bi',
            python_callable=refresh_bi,
        )
    
    extract_stage >> transform_join_task >> load_stage