import os
import csv
import json
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'retail-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_warehouse_csv(warehouse_name: str, **context) -> str:
    """Simulate fetching CSV file from warehouse and return file path."""
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, f'{warehouse_name}_inventory.csv')
    
    # Mock data generation with warehouse-specific SKU formats
    skus = [f'SKU-{i:04d}' for i in range(1, 1001)]
    if warehouse_name == 'north':
        skus = [f'N-{sku}' for sku in skus]
    elif warehouse_name == 'south':
        skus = [f'sku_{sku.lower()}' for sku in skus]
    elif warehouse_name == 'east':
        skus = [f'EAST-{sku}' for sku in skus]
    else:  # west
        skus = [f'W_{sku}' for sku in skus]
    
    # Add quantity variations to create discrepancies
    quantities = [100 + i % 50 for i in range(len(skus))]
    if warehouse_name == 'north':
        quantities = [q + 5 for q in quantities]
    
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['sku', 'quantity'])
        for sku, qty in zip(skus, quantities):
            writer.writerow([sku, qty])
    
    return file_path


def normalize_sku_format(warehouse_name: str, **context) -> str:
    """Normalize SKU format: uppercase, strip whitespace, standardize prefix."""
    ti = context['task_instance']
    file_path = ti.xcom_pull(task_ids=f'fetch_{warehouse_name}_csv')
    
    df = pd.read_csv(file_path)
    df['sku'] = df['sku'].astype(str).str.upper().str.strip()
    
    prefix_map = {
        'north': 'WH-NORTH-',
        'south': 'WH-SOUTH-',
        'east': 'WH-EAST-',
        'west': 'WH-WEST-'
    }
    
    df['sku'] = df['sku'].str.replace(r'^(N-|SKU_|EAST-|W_)', '', regex=True)
    df['sku'] = prefix_map[warehouse_name] + df['sku']
    
    temp_dir = tempfile.mkdtemp()
    normalized_path = os.path.join(temp_dir, f'{warehouse_name}_normalized.csv')
    df.to_csv(normalized_path, index=False)
    
    return normalized_path


def reconcile_inventory_discrepancies(**context) -> str:
    """Reconcile inventory across all warehouses and generate discrepancy report."""
    ti = context['task_instance']
    
    # Pull normalized file paths from all normalize tasks
    normalized_files = {
        warehouse: ti.xcom_pull(task_ids=f'normalize_{warehouse}_sku')
        for warehouse in ['north', 'south', 'east', 'west']
    }
    
    # Load all dataframes
    dataframes = {
        warehouse: pd.read_csv(file_path)
        for warehouse, file_path in normalized_files.items()
    }
    
    # Merge all dataframes on SKU
    merged_df = dataframes['north'].rename(columns={'quantity': 'north_qty'})
    for warehouse in ['south', 'east', 'west']:
        merged_df = merged_df.merge(
            dataframes[warehouse].rename(columns={'quantity': f'{warehouse}_qty'}),
            on='sku',
            how='outer'
        )
    
    merged_df = merged_df.fillna(0)
    
    # Identify discrepancies
    qty_columns = [f'{w}_qty' for w in ['north', 'south', 'east', 'west']]
    merged_df['max_qty'] = merged_df[qty_columns].max(axis=1)
    merged_df['min_qty'] = merged_df[qty_columns].min(axis=1)
    merged_df['discrepancy'] = merged_df['max_qty'] - merged_df['min_qty']
    
    discrepancy_df = merged_df[merged_df['discrepancy'] > 0].copy()
    discrepancy_df['variance_details'] = discrepancy_df.apply(
        lambda row: json.dumps({
            'north': int(row['north_qty']),
            'south': int(row['south_qty']),
            'east': int(row['east_qty']),
            'west': int(row['west_qty']),
            'variance': int(row['discrepancy'])
        }),
        axis=1
    )
    
    temp_dir = tempfile.mkdtemp()
    discrepancy_path = os.path.join(temp_dir, 'discrepancy_report.csv')
    discrepancy_df[['sku', 'variance_details']].to_csv(discrepancy_path, index=False)
    
    return discrepancy_path


def generate_final_reconciliation_report(**context) -> str:
    """Generate comprehensive PDF reconciliation report."""
    ti = context['task_instance']
    discrepancy_report_path = ti.xcom_pull(task_ids='reconcile_inventory_discrepancies')
    
    df = pd.read_csv(discrepancy_report_path)
    
    temp_dir = tempfile.mkdtemp()
    pdf_path = os.path.join(temp_dir, 'reconciliation_report.pdf')
    
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []
    
    # Title
    title = Paragraph("Retail Inventory Reconciliation Report", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 20))
    
    # Summary
    total_skus = 1000
    discrepancy_count = len(df)
    summary_text = f"""
    <b>Report Summary:</b><br/>
    Total SKUs Processed: {total_skus}<br/>
    SKUs with Discrepancies: {discrepancy_count}<br/>
    Reconciliation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    summary = Paragraph(summary_text, styles['Normal'])
    elements.append(summary)
    elements.append(Spacer(1, 20))
    
    # Discrepancy details table
    if discrepancy_count > 0:
        table_data = [['SKU', 'North', 'South', 'East', 'West', 'Variance']]
        
        for _, row in df.iterrows():
            details = json.loads(row['variance_details'])
            table_data.append([
                row['sku'],
                str(details['north']),
                str(details['south']),
                str(details['east']),
                str(details['west']),
                str(details['variance'])
            ])
        
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        elements.append(table)
    else:
        no_discrepancies = Paragraph("<b>No discrepancies found. All inventories are in sync.</b>", styles['Normal'])
        elements.append(no_discrepancies)
    
    # Recommendations
    elements.append(Spacer(1, 20))
    recommendations = Paragraph(
        "<b>Recommendations:</b><br/>"
        "1. Investigate SKUs with high variance<br/>"
        "2. Verify inventory counts at respective warehouses<br/>"
        "3. Update inventory records to reflect accurate quantities",
        styles['Normal']
    )
    elements.append(recommendations)
    
    doc.build(elements)
    
    return pdf_path


with DAG(
    dag_id='retail_inventory_reconciliation',
    default_args=default_args,
    description='Daily retail inventory reconciliation across four warehouses',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['retail', 'inventory', 'reconciliation']
) as dag:
    
    # Fetch warehouse CSV files in parallel
    fetch_north = PythonOperator(
        task_id='fetch_north_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'north'}
    )
    
    fetch_south = PythonOperator(
        task_id='fetch_south_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'south'}
    )
    
    fetch_east = PythonOperator(
        task_id='fetch_east_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'east'}
    )
    
    fetch_west = PythonOperator(
        task_id='fetch_west_csv',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'west'}
    )
    
    # Normalize SKU formats in parallel
    normalize_north = PythonOperator(
        task_id='normalize_north_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse_name': 'north'}
    )
    
    normalize_south = PythonOperator(
        task_id='normalize_south_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse_name': 'south'}
    )
    
    normalize_east = PythonOperator(
        task_id='normalize_east_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse_name': 'east'}
    )
    
    normalize_west = PythonOperator(
        task_id='normalize_west_sku',
        python_callable=normalize_sku_format,
        op_kwargs={'warehouse_name': 'west'}
    )
    
    # Reconcile inventory discrepancies
    reconcile_inventory = PythonOperator(
        task_id='reconcile_inventory_discrepancies',
        python_callable=reconcile_inventory_discrepancies
    )
    
    # Generate final reconciliation report
    generate_report = PythonOperator(
        task_id='generate_final_reconciliation_report',
        python_callable=generate_final_reconciliation_report
    )
    
    # Define task dependencies
    # Fetch tasks run in parallel
    # Normalize tasks depend on respective fetch tasks and run in parallel
    # Reconciliation depends on all normalization tasks
    # Report generation depends on reconciliation
    
    fetch_north >> normalize_north
    fetch_south >> normalize_south
    fetch_east >> normalize_east
    fetch_west >> normalize_west
    
    [normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile_inventory
    
    reconcile_inventory >> generate_report