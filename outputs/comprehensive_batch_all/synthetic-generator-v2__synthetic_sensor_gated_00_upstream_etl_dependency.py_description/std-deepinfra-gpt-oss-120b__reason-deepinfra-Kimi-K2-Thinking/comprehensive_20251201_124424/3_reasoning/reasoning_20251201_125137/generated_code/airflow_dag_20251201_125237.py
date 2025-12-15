from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

logger = logging.getLogger(__name__)

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
    """
    Load aggregated sales CSV data from upstream DAG output.
    Validates data format and completeness.
    """
    execution_date = context['ds']
    file_path = f"/data/sales/aggregated_sales_{execution_date}.csv"
    
    try:
        df = pd.read_csv(file_path)
        
        if df.empty:
            raise ValueError("Loaded CSV is empty")
        
        required_columns = ['date', 'revenue', 'product_id', 'region']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
        context['task_instance'].xcom_push(key='sales_data_rows', value=len(df))
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load sales CSV: {str(e)}")
        raise


def generate_dashboard(**context):
    """
    Generate executive dashboard with revenue trends, product performance,
    and regional analysis visualizations.
    """
    execution_date = context['ds']
    file_path = f"/data/sales/aggregated_sales_{execution_date}.csv"
    df = pd.read_csv(file_path)
    
    try:
        plt.figure(figsize=(15, 10))
        
        plt.subplot(2, 2, 1)
        daily_revenue = df.groupby('date')['revenue'].sum()
        daily_revenue.plot(kind='line')
        plt.title('Revenue Trend')
        plt.xlabel('Date')
        plt.ylabel('Revenue')
        
        plt.subplot(2, 2, 2)
        product_perf = df.groupby('product_id')['revenue'].sum().sort_values(ascending=False).head(10)
        product_perf.plot(kind='bar')
        plt.title('Top 10 Products by Revenue')
        plt.xlabel('Product ID')
        plt.ylabel('Revenue')
        plt.xticks(rotation=45)
        
        plt.subplot(2, 2, 3)
        regional_sales = df.groupby('region')['revenue'].sum().sort_values(ascending=False)
        regional_sales.plot(kind='pie', autopct='%1.1f%%')
        plt.title('Revenue by Region')
        
        plt.subplot(2, 2, 4)
        plt.axis('off')
        total_revenue = df['revenue'].sum()
        avg_revenue = df['revenue'].mean()
        total_orders = len(df)
        
        summary_text = f"""
        Executive Summary
        ----------------
        Total Revenue: ${total_revenue:,.2f}
        Average Order: ${avg_revenue:,.2f}
        Total Orders: {total_orders:,}
        """
        plt.text(0.1, 0.5, summary_text, fontsize=12, verticalalignment='center')
        
        plt.tight_layout()
        
        output_path = f"/data/dashboards/executive_dashboard_{execution_date}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Dashboard generated successfully at {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate dashboard: {str(e)}")
        raise


with DAG(
    dag_id='executive_sales_dashboard',
    default_args=default_args,
    description='Daily executive sales dashboard pipeline with sensor-gated execution',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
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
    
    load_sales_csv = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_sales_csv,
    )
    
    generate_dashboard = PythonOperator(
        task_id='generate_dashboard',
        python_callable=generate_dashboard,
    )
    
    wait_for_sales_aggregation >> load_sales_csv >> generate_dashboard