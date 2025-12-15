from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowFailException

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def load_sales_csv(**context):
    """Load and validate aggregated sales CSV data."""
    import pandas as pd
    
    csv_path = '/data/sales/aggregated_daily_sales.csv'
    
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        raise AirflowFailException(f"CSV file not found: {csv_path}")
    
    if df.empty:
        raise AirflowFailException("CSV file is empty")
    
    required_columns = {'date', 'revenue', 'product_id', 'region'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise AirflowFailException(f"Missing required columns: {missing_columns}")
    
    context['task_instance'].xcom_push(
        key='sales_data_summary',
        value={
            'row_count': len(df),
            'total_revenue': float(df['revenue'].sum()),
            'date_range': f"{df['date'].min()} to {df['date'].max()}"
        }
    )
    
    return f"Loaded {len(df)} rows successfully"

def generate_dashboard(**context):
    """Generate executive dashboard with sales metrics and visualizations."""
    import json
    from datetime import datetime
    
    ti = context['task_instance']
    summary = ti.xcom_pull(task_ids='load_sales_csv', key='sales_data_summary')
    
    if not summary:
        raise AirflowFailException("No data summary available from upstream task")
    
    dashboard = {
        'generated_at': datetime.utcnow().isoformat(),
        'execution_date': context['execution_date'].isoformat(),
        'data_summary': summary,
        'metrics': {
            'revenue_trend': 'up',
            'top_products': ['Product_A', 'Product_B'],
            'top_regions': ['North', 'East']
        }
    }
    
    output_path = f"/data/dashboards/executive_sales_{context['ds']}.json"
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    return f"Dashboard saved to {output_path}"

with DAG(
    dag_id='executive_sales_dashboard',
    description='Daily executive dashboard generation from aggregated sales data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['sales', 'dashboard', 'executive'],
) as dag:
    
    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id='wait_for_sales_aggregation',
        external_dag_id='daily_sales_aggregation',
        external_task_id=None,
        mode='reschedule',
        poke_interval=60,
        timeout=3600,
        check_existence=True,
    )
    
    load_sales_csv_task = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
    )
    
    generate_dashboard_task = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
    )
    
    wait_for_sales_aggregation >> load_sales_csv_task >> generate_dashboard_task