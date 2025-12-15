from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email': ['analytics-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='multi_region_ecommerce_analytics',
    default_args=default_args,
    description='Daily multi-region ecommerce analytics pipeline with fan-out/fan-in pattern',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ecommerce', 'analytics', 'multi-region'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    def ingest_region_data(region, **context):
        """Ingest sales data from CSV source for a specific region."""
        execution_date = context['ds']
        file_path = f'/data/{region.lower()}/sales_{execution_date}.csv'
        
        df = pd.read_csv(file_path)
        
        return {
            'region': region,
            'data': df.to_dict('records'),
            'currency': 'USD' if region.startswith('US-') else 'EUR' if region == 'EU' else 'JPY'
        }

    ingest_us_east = PythonOperator(
        task_id='ingest_us_east',
        python_callable=ingest_region_data,
        op_kwargs={'region': 'US-East'},
    )

    ingest_us_west = PythonOperator(
        task_id='ingest_us_west',
        python_callable=ingest_region_data,
        op_kwargs={'region': 'US-West'},
    )

    ingest_eu = PythonOperator(
        task_id='ingest_eu',
        python_callable=ingest_region_data,
        op_kwargs={'region': 'EU'},
    )

    ingest_apac = PythonOperator(
        task_id='ingest_apac',
        python_callable=ingest_region_data,
        op_kwargs={'region': 'APAC'},
    )

    def convert_currency(region, source_task_id, **context):
        """Convert regional sales data to USD currency."""
        ti = context['ti']
        data = ti.xcom_pull(task_ids=source_task_id)
        
        df = pd.DataFrame(data['data'])
        
        # Exchange rates - in production, fetch from an API
        EUR_TO_USD = 1.08
        JPY_TO_USD = 0.0067
        
        if region in ['US-East', 'US-West']:
            df['amount_usd'] = df['amount']
        elif region == 'EU':
            df['amount_usd'] = df['amount'] * EUR_TO_USD
        elif region == 'APAC':
            df['amount_usd'] = df['amount'] * JPY_TO_USD
        
        return {
            'region': region,
            'data': df.to_dict('records'),
            'total_revenue_usd': float(df['amount_usd'].sum())
        }

    convert_us_east = PythonOperator(
        task_id='convert_us_east',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-East', 'source_task_id': 'ingest_us_east'},
    )

    convert_us_west = PythonOperator(
        task_id='convert_us_west',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-West', 'source_task_id': 'ingest_us_west'},
    )

    convert_eu = PythonOperator(
        task_id='convert_eu',
        python_callable=convert_currency,
        op_kwargs={'region': 'EU', 'source_task_id': 'ingest_eu'},
    )

    convert_apac = PythonOperator(
        task_id='convert_apac',
        python_callable=convert_currency,
        op_kwargs={'region': 'APAC', 'source_task_id': 'ingest_apac'},
    )

    def aggregate_global_revenue(**context):
        """Aggregate all regional data and generate global revenue report."""
        ti = context['ti']
        execution_date = context['ds']
        
        regions = ['US-East', 'US-West', 'EU', 'APAC']
        regional_revenues = {}
        global_total = 0.0
        
        for region in regions:
            task_id = f'convert_{region.lower().replace("-", "_")}'
            data = ti.xcom_pull(task_ids=task_id)
            revenue = data['total_revenue_usd']
            regional_revenues[region] = revenue
            global_total += revenue
        
        # Create report DataFrame
        report_data = [{'region': region, 'revenue_usd': revenue} 
                      for region, revenue in regional_revenues.items()]
        report_data.append({'region': 'GLOBAL', 'revenue_usd': global_total})
        
        report_df = pd.DataFrame(report_data)
        
        # Save to CSV
        output_path = f'/data/global/global_revenue_report_{execution_date}.csv'
        report_df.to_csv(output_path, index=False)
        
        return {
            'regional_revenues': regional_revenues,
            'global_total': global_total,
            'report_path': output_path
        }

    aggregate_global_revenue_task = PythonOperator(
        task_id='aggregate_global_revenue',
        python_callable=aggregate_global_revenue,
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Task dependencies
    start_pipeline >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]
    
    ingest_us_east >> convert_us_east
    ingest_us_west >> convert_us_west
    ingest_eu >> convert_eu
    ingest_apac >> convert_apac
    
    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate_global_revenue_task >> end_pipeline