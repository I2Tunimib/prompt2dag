from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='db_replication_daily_snapshot',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['replication', 'database', 'fanout'],
    max_active_runs=1,
    max_active_tasks=3,
) as dag:
    """
    Daily database replication pipeline.
    
    Creates a CSV snapshot from production and fan-out copies to 
    Development, Staging, and QA environments.
    """
    
    dump_prod_csv = BashOperator(
        task_id='dump_prod_csv',
        bash_command="""
            SNAPSHOT_FILE="/tmp/prod_snapshot_{{ ds_nodash }}.csv"
            echo "Creating production CSV snapshot at ${SNAPSHOT_FILE}"
            
            # Example PostgreSQL command (customize connection details)
            # psql -h prod-db.example.com -U prod_user -d production -c "\\copy (SELECT * FROM source_table) TO '${SNAPSHOT_FILE}' WITH CSV HEADER"
            
            # Placeholder for demonstration
            touch "${SNAPSHOT_FILE}"
            echo "Snapshot created: ${SNAPSHOT_FILE}"
        """,
    )
    
    copy_dev = BashOperator(
        task_id='copy_dev',
        bash_command="""
            SNAPSHOT_FILE="/tmp/prod_snapshot_{{ ds_nodash }}.csv"
            echo "Loading ${SNAPSHOT_FILE} into Development database"
            
            # Example PostgreSQL command (customize connection details)
            # psql -h dev-db.example.com -U dev_user -d development -c "\\copy target_table FROM '${SNAPSHOT_FILE}' WITH CSV HEADER"
            
            # Verification placeholder
            if [ -f "${SNAPSHOT_FILE}" ]; then
                echo "Successfully loaded into Development DB"
            else
                echo "Error: Snapshot file not found at ${SNAPSHOT_FILE}"
                exit 1
            fi
        """,
    )
    
    copy_staging = BashOperator(
        task_id='copy_staging',
        bash_command="""
            SNAPSHOT_FILE="/tmp/prod_snapshot_{{ ds_nodash }}.csv"
            echo "Loading ${SNAPSHOT_FILE} into Staging database"
            
            # Example PostgreSQL command (customize connection details)
            # psql -h staging-db.example.com -U staging_user -d staging -c "\\copy target_table FROM '${SNAPSHOT_FILE}' WITH CSV HEADER"
            
            # Verification placeholder
            if [ -f "${SNAPSHOT_FILE}" ]; then
                echo "Successfully loaded into Staging DB"
            else
                echo "Error: Snapshot file not found at ${SNAPSHOT_FILE}"
                exit 1
            fi
        """,
    )
    
    copy_qa = BashOperator(
        task_id='copy_qa',
        bash_command="""
            SNAPSHOT_FILE="/tmp/prod_snapshot_{{ ds_nodash }}.csv"
            echo "Loading ${SNAPSHOT_FILE} into QA database"
            
            # Example PostgreSQL command (customize connection details)
            # psql -h qa-db.example.com -U qa_user -d qa -c "\\copy target_table FROM '${SNAPSHOT_FILE}' WITH CSV HEADER"
            
            # Verification placeholder
            if [ -f "${SNAPSHOT_FILE}" ]; then
                echo "Successfully loaded into QA DB"
            else
                echo "Error: Snapshot file not found at ${SNAPSHOT_FILE}"
                exit 1
            fi
        """,
    )
    
    # Define task dependencies: fan-out pattern
    dump_prod_csv >> [copy_dev, copy_staging, copy_qa]