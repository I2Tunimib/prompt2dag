import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator

# For database connection
import psycopg2
from psycopg2.extras import execute_batch

# For reading CSV
import csv

# Default arguments
default_args = {
    'owner': 'healthcare-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'healthcare_claims_etl',
    default_args=default_args,
    description='Healthcare claims processing ETL pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['healthcare', 'claims', 'etl'],
) as dag:

    # Extract claims task
    def extract_claims(**context):
        """
        Extract patient claims data from CSV file.
        In production, this would read from a secure data source.
        """
        # Simulated CSV data - in real scenario, read from actual file path
        claims_data = [
            {
                'claim_id': 'CLM001',
                'patient_id': 'PAT001',
                'provider_id': 'PROV001',
                'procedure_code': '99213',
                'claim_amount': 150.00,
                'claim_date': '2024-01-01'
            },
            {
                'claim_id': 'CLM002',
                'patient_id': 'PAT002',
                'provider_id': 'PROV002',
                'procedure_code': '99214',
                'claim_amount': 200.00,
                'claim_date': '2024-01-01'
            }
        ]
        
        # In real scenario, you would read from a CSV file:
        # with open('/path/to/claims.csv', 'r') as f:
        #     reader = csv.DictReader(f)
        #     claims_data = [row for row in reader]
        
        return json.dumps(claims_data)

    # Extract providers task
    def extract_providers(**context):
        """
        Extract provider data from CSV file.
        In production, this would read from a secure data source.
        """
        # Simulated CSV data - in real scenario, read from actual file path
        providers_data = [
            {
                'provider_id': 'PROV001',
                'provider_name': 'Dr. Smith',
                'specialty': 'Family Medicine',
                'npi': '1234567890'
            },
            {
                'provider_id': 'PROV002',
                'provider_name': 'Dr. Jones',
                'specialty': 'Internal Medicine',
                'npi': '0987654321'
            }
        ]
        
        # In real scenario, you would read from a CSV file:
        # with open('/path/to/providers.csv', 'r') as f:
        #     reader = csv.DictReader(f)
        #     providers_data = [row for row in reader]
        
        return json.dumps(providers_data)

    # Transform task
    def transform_join(claims_data_str, providers_data_str, **context):
        """
        Join claims and provider data, anonymize PII, and calculate risk scores.
        """
        claims_data = json.loads(claims_data_str)
        providers_data = json.loads(providers_data_str)
        
        # Create provider lookup
        provider_lookup = {p['provider_id']: p for p in providers_data}
        
        # Transform and join data
        transformed_data = []
        for claim in claims_data:
            # Anonymize patient_id by hashing
            patient_id = claim['patient_id']
            hashed_patient_id = hashlib.sha256(patient_id.encode()).hexdigest()[:16]
            
            # Get provider info
            provider = provider_lookup.get(claim['provider_id'], {})
            
            # Calculate risk score (simplified: based on procedure code and amount)
            procedure_code = claim['procedure_code']
            claim_amount = float(claim['claim_amount'])
            
            # Simple risk scoring logic
            risk_score = 0.0
            if procedure_code in ['99213', '99214']:
                risk_score += 1.0
            if claim_amount > 175.00:
                risk_score += 1.5
            
            transformed_record = {
                'claim_id': claim['claim_id'],
                'hashed_patient_id': hashed_patient_id,
                'provider_id': claim['provider_id'],
                'provider_name': provider.get('provider_name', 'Unknown'),
                'specialty': provider.get('specialty', 'Unknown'),
                'procedure_code': procedure_code,
                'claim_amount': claim_amount,
                'claim_date': claim['claim_date'],
                'risk_score': risk_score
            }
            transformed_data.append(transformed_record)
        
        return json.dumps(transformed_data)

    # Load warehouse task
    def load_warehouse(transformed_data_str, **context):
        """
        Load transformed data to healthcare analytics warehouse.
        Creates tables if they don't exist and inserts data.
        """
        transformed_data = json.loads(transformed_data_str)
        
        # Database connection parameters - in production, use Airflow Connections
        conn_params = {
            'host': 'postgres-warehouse',
            'database': 'healthcare_analytics',
            'user': 'airflow',
            'password': 'airflow'
        }
        
        try:
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()
            
            # Create tables if they don't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS providers_dim (
                    provider_id VARCHAR(50) PRIMARY KEY,
                    provider_name VARCHAR(255),
                    specialty VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS claims_fact (
                    claim_id VARCHAR(50) PRIMARY KEY,
                    hashed_patient_id VARCHAR(64),
                    provider_id VARCHAR(50),
                    procedure_code VARCHAR(20),
                    claim_amount DECIMAL(10,2),
                    claim_date DATE,
                    risk_score DECIMAL(5,2),
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (provider_id) REFERENCES providers_dim(provider_id)
                )
            """)
            
            # Insert providers data (avoid duplicates)
            providers_to_insert = {}
            for record in transformed_data:
                provider_id = record['provider_id']
                if provider_id not in providers_to_insert:
                    providers_to_insert[provider_id] = (
                        provider_id,
                        record['provider_name'],
                        record['specialty']
                    )
            
            execute_batch(cur, """
                INSERT INTO providers_dim (provider_id, provider_name, specialty)
                VALUES (%s, %s, %s)
                ON CONFLICT (provider_id) DO NOTHING
            """, list(providers_to_insert.values()))
            
            # Insert claims data
            claims_to_insert = [
                (
                    record['claim_id'],
                    record['hashed_patient_id'],
                    record['provider_id'],
                    record['procedure_code'],
                    record['claim_amount'],
                    record['claim_date']
                )
                for record in transformed_data
            ]
            
            execute_batch(cur, """
                INSERT INTO claims_fact 
                (claim_id, hashed_patient_id, provider_id, procedure_code, claim_amount, claim_date, risk_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (claim_id) DO UPDATE SET
                    risk_score = EXCLUDED.risk_score,
                    loaded_at = CURRENT_TIMESTAMP
            """, [(c[0], c[1], c[2], c[3], c[4], c[5], record['risk_score']) 
                  for c, record in zip(claims_to_insert, transformed_data)])
            
            conn.commit()
            cur.close()
            conn.close()
            
            return f"Successfully loaded {len(transformed_data)} claims to warehouse"
            
        except Exception as e:
            raise Exception(f"Failed to load data to warehouse: {str(e)}")

    # Refresh BI task
    def refresh_bi(**context):
        """
        Trigger refresh of BI dashboard tools (Power BI and Tableau).
        In production, this would call actual BI tool APIs.
        """
        # Simulate BI refresh
        print("Triggering Power BI dataset refresh...")
        print("Triggering Tableau data source refresh...")
        
        # Example of what real implementation might look like:
        # power_bi_refresh_url = "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/refreshes"
        # tableau_refresh_url = "https://my-tableau-server/api/3.8/sites/{siteId}/datasources/{datasourceId}/refresh"
        
        return "BI dashboards refresh triggered successfully"

    # Define tasks
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
        op_kwargs={
            'claims_data_str': "{{ task_instance.xcom_pull(task_ids='extract_claims') }}",
            'providers_data_str': "{{ task_instance.xcom_pull(task_ids='extract_providers') }}"
        },
    )

    load_warehouse_task = PythonOperator(
        task_id='load_warehouse',
        python_callable=load_warehouse,
        op_kwargs={
            'transformed_data_str': "{{ task_instance.xcom_pull(task_ids='transform_join') }}"
        },
    )

    refresh_bi_task = PythonOperator(
        task_id='refresh_bi',
        python_callable=refresh_bi,
    )

    # Define dependencies
    # Extract tasks run in parallel
    [extract_claims_task, extract_providers_task] >> transform_join_task
    
    # Transform task must complete before load stage
    transform_join_task >> [load_warehouse_task, refresh_bi_task]