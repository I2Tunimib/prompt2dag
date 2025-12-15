from datetime import datetime, timedelta
from typing import Any, Dict
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def load_sales_csv(**context: Dict[str, Any]) -> str:
    """Load and validate aggregated sales CSV from upstream DAG output."""
    try:
        import pandas as pd
        
        csv_path = "/data/sales/aggregated_daily_sales.csv"
        df = pd.read_csv(csv_path)
        
        if df.empty:
            raise ValueError("CSV file is empty")
        
        required_columns = ['date', 'revenue', 'product_id', 'region']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        context['task_instance'].xcom_push(key='sales_data_path', value=csv_path)
        
        return f"Successfully loaded {len(df)} records from {csv_path}"
        
    except Exception as e:
        raise Exception(f"Failed to load sales CSV: {str(e)}")

def generate_dashboard(**context: Dict[str, Any]) -> str:
    """Generate executive dashboard with revenue trends and performance metrics."""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        
        ti = context['task_instance']
        csv_path = ti.xcom_pull(task_ids='load_sales_csv', key='sales_data_path')
        
        if not csv_path:
            raise ValueError("No sales data path found in XCom")
        
        df = pd.read_csv(csv_path)
        
        revenue_by_date = df.groupby('date')['revenue'].sum()
        product_performance = df.groupby('product_id')['revenue'].sum().sort_values(ascending=False)
        regional_analysis = df.groupby('region')['revenue'].sum().sort_values(ascending=False)
        
        fig, axes = plt.subplots(3, 1, figsize=(12, 10))
        
        revenue_by_date.plot(ax=axes[0], title='Daily Revenue Trends')
        product_performance.head(10).plot(kind='bar', ax=axes[1], title='Top 10 Products by Revenue')
        regional_analysis.plot(kind='bar', ax=axes[2], title='Revenue by Region')
        
        plt.tight_layout()
        
        output_path = "/data/dashboards/executive_sales_dashboard.png"
        plt.savefig(output_path)
        
        return f"Dashboard generated successfully at {output_path}"
        
    except Exception as e:
        raise Exception(f"Failed to generate dashboard: {str(e)}")

with DAG(
    dag_id='executive_sales_dashboard',
    default_args=default_args,
    description='Daily executive sales dashboard pipeline with sensor-gated execution',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'dashboard', 'executive'],
) as dag:
    
    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id='wait_for_sales_aggregation',
        external_dag_id='daily_sales_aggregation',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed'],
        mode='reschedule',
        poke_interval=60,
        timeout=3600,
    )
    
    load_sales_csv = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
    )
    
    generate_dashboard = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
    )
    
    wait_for_sales_aggregation >> load_sales_csv >> generate_dashboard