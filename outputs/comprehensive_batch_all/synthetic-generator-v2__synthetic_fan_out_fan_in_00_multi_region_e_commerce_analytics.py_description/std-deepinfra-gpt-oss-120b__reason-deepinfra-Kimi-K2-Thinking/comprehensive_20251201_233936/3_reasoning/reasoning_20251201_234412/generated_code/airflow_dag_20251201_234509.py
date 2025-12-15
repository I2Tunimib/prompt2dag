from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email': ['analytics-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

EXCHANGE_RATES = {
    'EUR': 1.1,
    'JPY': 0.0075,
}


def ingest_region_data(region: str, **context) -> Dict[str, Any]:
    """Ingest regional sales data from CSV source."""
    logger.info(f"Ingesting sales data for region: {region}")
    
    # Simulate CSV ingestion with sample data
    if region == 'US-East':
        data = {'region': region, 'currency': 'USD', 'sales': [1000, 2000, 1500]}
    elif region == 'US-West':
        data = {'region': region, 'currency': 'USD', 'sales': [1200, 1800, 1600]}
    elif region == 'EU':
        data = {'region': region, 'currency': 'EUR', 'sales': [900, 1100, 1000]}
    elif region == 'APAC':
        data = {'region': region, 'currency': 'JPY', 'sales': [150000, 200000, 180000]}
    else:
        raise ValueError(f"Unknown region: {region}")
    
    context['task_instance'].xcom_push(key='sales_data', value=data)
    return data


def convert_currency(region: str, **context) -> Dict[str, Any]:
    """Convert regional sales data to USD."""
    logger.info(f"Converting currency for region: {region}")
    
    task_instance = context['task_instance']
    ingest_task_id = f'ingest_{region.lower().replace("-", "_")}'
    data = task_instance.xcom_pull(task_ids=ingest_task_id, key='sales_data')
    
    if not data:
        raise ValueError(f"No data found for region {region}")
    
    currency = data['currency']
    sales = data['sales']
    
    if currency == 'USD':
        usd_sales = sales
        logger.info(f"Region {region} already in USD")
    elif currency == 'EUR':
        rate = EXCHANGE_RATES['EUR']
        usd_sales = [amount * rate for amount in sales]
        logger.info(f"Converted EUR to USD at rate {rate}")
    elif currency == 'JPY':
        rate = EXCHANGE_RATES['JPY']
        usd_sales = [amount * rate for amount in sales]
        logger.info(f"Converted JPY to USD at rate {rate}")
    else:
        raise ValueError(f"Unsupported currency: {currency}")
    
    regional_revenue = sum(usd_sales)
    converted_data = {
        'region': region,
        'original_currency': currency,
        'usd_sales': usd_sales,
        'regional_revenue_usd': regional_revenue
    }
    
    task_instance.xcom_push(key='converted_data', value=converted_data)
    return converted_data


def aggregate_global_revenue(**context):
    """Aggregate all regional data and generate global revenue report."""
    logger.info("Aggregating global revenue from all regions")
    
    task_instance = context['task_instance']
    regions = ['US-East', 'US-West', 'EU', 'APAC']
    
    all_data = []
    total_revenue = 0
    
    for region in regions:
        convert_task_id = f'convert_{region.lower().replace("-", "_")}'
        data = task_instance.xcom_pull(task_ids=convert_task_id, key='converted_data')
        
        if not data:
            raise ValueError(f"No converted data found for region {region}")
        
        all_data.append(data)
        total_revenue += data['regional_revenue_usd']
        logger.info(f"Region {region} revenue: ${data['regional_revenue_usd']:,.2f}")
    
    logger.info(f"Total global revenue: ${total_revenue:,.2f}")
    
    # Generate CSV report
    report_df = pd.DataFrame([
        {
            'region': r['region'],
            'original_currency': r['original_currency'],
            'regional_revenue_usd': r['regional_revenue_usd']
        }
        for r in all_data
    ])
    report_df.loc[len(report_df)] = ['GLOBAL', 'USD', total_revenue]
    
    output_path = f"/tmp/global_revenue_report_{context['ds']}.csv"
    report_df.to_csv(output_path, index=False)
    logger.info(f"Global revenue report saved to {output_path}")
    
    task_instance.xcom_push(key='report_path', value=output_path)


with DAG(
    dag_id='multi_region_ecommerce_analytics',
    default_args=default_args,
    description='Multi-region ecommerce analytics pipeline with fan-out fan-in pattern',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'analytics', 'multi-region'],
) as dag:
    
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    
    # Ingestion tasks
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
    
    # Currency conversion tasks
    convert_us_east = PythonOperator(
        task_id='convert_us_east',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-East'},
    )
    
    convert_us_west = PythonOperator(
        task_id='convert_us_west',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-West'},
    )
    
    convert_eu = PythonOperator(
        task_id='convert_eu',
        python_callable=convert_currency,
        op_kwargs={'region': 'EU'},
    )
    
    convert_apac = PythonOperator(
        task_id='convert_apac',
        python_callable=convert_currency,
        op_kwargs={'region': 'APAC'},
    )
    
    # Aggregation task
    aggregate_global_revenue_task = PythonOperator(
        task_id='aggregate_global_revenue',
        python_callable=aggregate_global_revenue,
    )
    
    end_pipeline = EmptyOperator(task_id='end_pipeline')
    
    # Dependencies
    start_pipeline >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]
    
    ingest_us_east >> convert_us_east
    ingest_us_west >> convert_us_west
    ingest_eu >> convert_eu
    ingest_apac >> convert_apac
    
    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate_global_revenue_task
    
    aggregate_global_revenue_task >> end_pipeline