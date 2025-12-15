import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle


default_args = {
    'owner': 'retail-analytics',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def fetch_warehouse_csv(warehouse_name, **context):
    """Simulate fetching CSV file from warehouse and return file path."""
    data = {
        'sku': [f'WH-{warehouse_name[:1].upper()}-{i:04d}' for i in range(1, 1001)],
        'quantity': [100 + i % 50 for i in range(1000)],
        'product_name': [f'Product {i}' for i in range(1, 1001)]
    }
    df = pd.DataFrame(data)

    if warehouse_name == 'north':
        df.loc[0:22, 'quantity'] += 10

    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, f'{warehouse_name}_inventory.csv')
    df.to_csv(file_path, index=False)

    return file_path


def normalize_sku_format(warehouse_name, **context):
    """Normalize SKU format: uppercase, strip whitespace, standardize prefix."""
    ti = context['ti']
    fetch_task_id = f'fetch_{warehouse_name}_warehouse'
    input_file = ti.xcom_pull(task_ids=fetch_task_id)

    if not input_file or not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    df = pd.read_csv(input_file)
    df['sku'] = df['sku'].astype(str).str.upper().str.strip()
    df['sku'] = df['sku'].str.replace(r'^WH-[A-Z]-', 'STD-', regex=True)

    temp_dir = tempfile.gettempdir()
    output_file = os.path.join(temp_dir, f'{warehouse_name}_inventory_normalized.csv')
    df.to_csv(output_file, index=False)

    return output_file


def reconcile_inventory_discrepancies(**context):
    """Reconcile inventory across all warehouses and generate discrepancy report."""
    ti = context['ti']
    warehouse_names = ['north', 'south', 'east', 'west']

    normalized_files = {}
    for warehouse in warehouse_names:
        task_id = f'normalize_{warehouse}_sku'
        file_path = ti.xcom_pull(task_ids=task_id)
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Normalized file not found for {warehouse}: {file_path}")
        normalized_files[warehouse] = file_path

    warehouse_data = {}
    for warehouse, file_path in normalized_files.items():
        df = pd.read_csv(file_path)
        df['warehouse'] = warehouse
        warehouse_data[warehouse] = df

    combined_df = pd.concat(warehouse_data.values(), ignore_index=True)
    pivot_df = combined_df.pivot_table(
        index='sku',
        columns='warehouse',
        values='quantity',
        aggfunc='first'
    ).fillna(0)

    discrepancies = []
    for sku, row in pivot_df.iterrows():
        quantities = row.values
        if len(set(quantities)) > 1:
            discrepancy = {
                'sku': sku,
                'variance_detected': True,
                'quantity_variance': max(quantities) - min(quantities),
                'warehouse_quantities': row.to_dict()
            }
            discrepancies.append(discrepancy)

    if discrepancies:
        report_df = pd.DataFrame(discrepancies)
        for warehouse in warehouse_names:
            report_df[f'qty_{warehouse}'] = report_df['warehouse_quantities'].apply(
                lambda x: x.get(warehouse, 0)
            )
        report_df = report_df.drop('warehouse_quantities', axis=1)
    else:
        report_df = pd.DataFrame(columns=['sku', 'variance_detected', 'quantity_variance'] +
                                       [f'qty_{w}' for w in warehouse_names])

    temp_dir = tempfile.gettempdir()
    discrepancy_file = os.path.join(temp_dir, 'inventory_discrepancy_report.csv')
    report_df.to_csv(discrepancy_file, index=False)

    return discrepancy_file


def generate_final_reconciliation_report(discrepancy_file, **context):
    """Generate comprehensive PDF reconciliation report."""
    if not os.path.exists(discrepancy_file):
        raise FileNotFoundError(f"Discrepancy file not found: {discrepancy_file}")

    df = pd.read_csv(discrepancy_file)
    temp_dir = tempfile.gettempdir()
    pdf_file = os.path.join(temp_dir, 'final_reconciliation_report.pdf')

    doc = SimpleDocTemplate(pdf_file, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    title = Paragraph("Retail Inventory Reconciliation Report", styles['Title'])
    elements.append(title)

    total_skus = 1000
    discrepancy_count = len(df)
    summary_text = f"""
    <b>Report Summary:</b><br/>
    Total SKUs processed per warehouse: {total_skus}<br/>
    Warehouses: North, South, East, West<br/>
    SKUs with discrepancies: {discrepancy_count}<br/>
    """
    summary = Paragraph(summary_text, styles['Normal'])
    elements.append(summary)

    if discrepancy_count > 0:
        elements.append(Paragraph("<br/><b>Discrepancy Details:</b><br/>", styles['Normal']))
        headers = ['SKU', 'Variance', 'North', 'South', 'East', 'West']
        table_data = [headers]

        for _, row in df.head(50).iterrows():
            table_data.append([
                row['sku'],
                f"{row['quantity_variance']:.0f}",
                f"{row.get('qty_north', 0):.0f}",
                f"{row.get('qty_south', 0):.0f}",
                f"{row.get('qty_east', 0):.0f}",
                f"{row.get('qty_west', 0):.0f}"
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

    doc.build(elements)

    return pdf_file


with DAG(
    dag_id='retail_inventory_reconciliation',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['retail', 'inventory', 'reconciliation'],
    max_active_runs=1
) as dag:

    fetch_north = PythonOperator(
        task_id='fetch_north_warehouse',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'north'}
    )

    fetch_south = PythonOperator(
        task_id='fetch_south_warehouse',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'south'}
    )

    fetch_east = PythonOperator(
        task_id='fetch_east_warehouse',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'east'}
    )

    fetch_west = PythonOperator(
        task_id='fetch_west_warehouse',
        python_callable=fetch_warehouse_csv,
        op_kwargs={'warehouse_name': 'west'}
    )

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

    reconcile_inventory = PythonOperator(
        task_id='reconcile_inventory_discrepancies',
        python_callable=reconcile_inventory_discrepancies
    )

    generate_report = PythonOperator(
        task_id='generate_final_reconciliation_report',
        python_callable=generate_final_reconciliation_report,
        op_kwargs={'discrepancy_file': '{{ ti.xcom_pull(task_ids="reconcile_inventory_discrepancies") }}'}
    )

    # Define task dependencies
    fetch_north >> normalize_north
    fetch_south >> normalize_south
    fetch_east >> normalize_east
    fetch_west >> normalize_west

    [normalize_north, normalize_south, normalize_east, normalize_west] >> reconcile_inventory

    reconcile_inventory >> generate_report