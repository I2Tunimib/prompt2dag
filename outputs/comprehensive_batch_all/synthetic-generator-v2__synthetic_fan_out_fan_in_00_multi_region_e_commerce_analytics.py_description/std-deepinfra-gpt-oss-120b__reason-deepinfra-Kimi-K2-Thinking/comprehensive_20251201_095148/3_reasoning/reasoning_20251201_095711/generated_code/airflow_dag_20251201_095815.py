from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import io
from typing import Dict, List

default_args = {
    'owner': 'ecommerce-analytics',
    'depends_on_past': False,
    'email': ['data-team@example.com', 'analytics@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_simulated_csv_data(region: str) -> str:
    """Generate simulated CSV data for demonstration purposes."""
    if region == 'US-East':
        return "order_id,amount,currency\nORD001,150.00,USD\nORD002,200.50,USD"
    elif region == 'US-West':
        return "order_id,amount,currency\nORD003,175.25,USD\nORD004,225.75,USD"
    elif region == 'EU':
        return "order_id,amount,currency\nORD005,180.00,EUR\nORD006,220.50,EUR"
    elif region == 'APAC':
        return "order_id,amount,currency\nORD007,25000,JPY\nORD008,30000,JPY"
    return ""

def ingest_region_data(region: str, **context) -> List[Dict]:
    """Ingest sales data for a specific region from CSV source."""
    csv_data = get_simulated_csv_data(region)
    csv_reader = csv.DictReader(io.StringIO(csv_data))
    data = list(csv_reader)
    context['ti'].xcom_push(key='raw_data', value=data)
    return data

def convert_currency(region: str, **context) -> List[Dict]:
    """Convert regional currency to USD."""
    ti = context['ti']
    data = ti.xcom_pull(task_ids=f'ingest_{region.replace("-", "_")}', key='raw_data')
    
    if not data:
        raise ValueError(f"No data found for region {region}")
    
    EUR_TO_USD = 1.08
    JPY_TO_USD = 0.0067
    
    converted_data = []
    for row in data:
        amount = float(row['amount'])
        currency = row['currency']
        
        if region in ['US-East', 'US-West']:
            converted_amount = amount
        elif region == 'EU' and currency == 'EUR':
            converted_amount = amount * EUR_TO_USD
        elif region == 'APAC' and currency == 'JPY':
            converted_amount = amount * JPY_TO_USD
        else:
            raise ValueError(f"Unexpected currency {currency} for region {region}")
        
        converted_data.append({
            'order_id': row['order_id'],
            'amount_usd': round(converted_amount, 2),
            'region': region
        })
    
    ti.xcom_push(key='converted_data', value=converted_data)
    return converted_data

def aggregate_revenues(**context):
    """Aggregate all regional data and generate global revenue report."""
    ti = context['ti']
    regions = ['US-East', 'US-West', 'EU', 'APAC']
    
    all_converted_data = []
    for region in regions:
        task_id = f'convert_{region.replace("-", "_")}'
        data = ti.xcom_pull(task_ids=task_id, key='converted_data')
        if data:
            all_converted_data.extend(data)
    
    if not all_converted_data:
        raise ValueError("No converted data found for aggregation")
    
    regional_revenues = {}
    for row in all_converted_data:
        region = row['region']
        amount = row['amount_usd']
        regional_revenues[region] = regional_revenues.get(region, 0) + amount
    
    global_total = sum(regional_revenues.values())
    
    report_lines = [
        "Global Revenue Report",
        f"Generated: {datetime.utcnow().isoformat()}",
        "=" * 50,
        "Regional Revenues (USD):",
    ]
    
    for region in regions:
        revenue = regional_revenues.get(region, 0)
        report_lines.append(f"  {region}: ${revenue:,.2f}")
    
    report_lines.extend([
        "=" * 50,
        f"Global Total: ${global_total:,.2f}",
    ])
    
    report_content = "\n".join(report_lines)
    print("Generated Report:")
    print(report_content)
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['region', 'revenue_usd'])
    for region, revenue in regional_revenues.items():
        writer.writerow([region, f"{revenue:.2f}"])
    writer.writerow(['GLOBAL_TOTAL', f"{global_total:.2f}"])
    
    ti.xcom_push(key='global_report', value=output.getvalue())
    return report_content

with DAG(
    dag_id='multi_region_ecommerce_analytics',
    description='Daily multi-region ecommerce analytics pipeline with fan-out fan-in pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['ecommerce', 'analytics', 'multi-region'],
    max_active_tasks=4,
) as dag:
    
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    
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
    
    aggregate_global_revenue = PythonOperator(
        task_id='aggregate_global_revenue',
        python_callable=aggregate_revenues,
    )
    
    end_pipeline = EmptyOperator(task_id='end_pipeline')
    
    start_pipeline >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]
    
    ingest_us_east >> convert_us_east
    ingest_us_west >> convert_us_west
    ingest_eu >> convert_eu
    ingest_apac >> convert_apac
    
    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate_global_revenue
    
    aggregate_global_revenue >> end_pipeline