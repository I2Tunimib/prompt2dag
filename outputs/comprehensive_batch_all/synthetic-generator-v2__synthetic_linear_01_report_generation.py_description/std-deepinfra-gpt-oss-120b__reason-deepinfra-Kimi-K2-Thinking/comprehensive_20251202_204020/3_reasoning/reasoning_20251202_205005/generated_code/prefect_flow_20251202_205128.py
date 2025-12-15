from prefect import flow, task
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from datetime import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from typing import Optional


@task(retries=2, retry_delay_seconds=300)
def query_sales_data(execution_date: str) -> pd.DataFrame:
    """Extract daily sales data aggregated by product from PostgreSQL."""
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'database': os.getenv('POSTGRES_DB', 'sales_db'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    query = """
        SELECT product_name, SUM(quantity) as total_quantity,
               SUM(revenue) as total_revenue
        FROM sales
        WHERE sale_date = %s
        GROUP BY product_name
        ORDER BY product_name
    """
    
    with psycopg2.connect(**conn_params) as conn:
        df = pd.read_sql(query, conn, params=(execution_date,))
    
    return df


@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame, execution_date: str) -> str:
    """Transform query results into a CSV file at /tmp/."""
    csv_path = f"/tmp/sales_report_{execution_date}.csv"
    data.to_csv(csv_path, index=False)
    return csv_path


@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame, execution_date: str) -> str:
    """Create a PDF bar chart visualizing sales by product."""
    pdf_path = f"/tmp/sales_chart_{execution_date}.pdf"
    
    plt.figure(figsize=(12, 8))
    plt.bar(data['product_name'], data['total_revenue'])
    plt.xlabel('Product')
    plt.ylabel('Revenue')
    plt.title(f'Daily Sales by Product - {execution_date}')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()
    
    return pdf_path


@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str, execution_date: str):
    """Send email with CSV and PDF attachments to management."""
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER', 'your-email@gmail.com')
    smtp_password = os.getenv('SMTP_PASSWORD', 'your-app-password')
    
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = 'management@company.com'
    msg['Subject'] = f'Daily Sales Report - {execution_date}'
    
    body = f'Please find attached the daily sales report for {execution_date}.'
    msg.attach(MIMEText(body, 'plain'))
    
    with open(csv_path, 'rb') as f:
        csv_part = MIMEApplication(f.read(), _subtype='csv')
        csv_part.add_header(
            'Content-Disposition', 'attachment',
            filename=os.path.basename(csv_path)
        )
        msg.attach(csv_part)
    
    with open(pdf_path, 'rb') as f:
        pdf_part = MIMEApplication(f.read(), _subtype='pdf')
        pdf_part.add_header(
            'Content-Disposition', 'attachment',
            filename=os.path.basename(pdf_path)
        )
        msg.attach(pdf_part)
    
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


@flow
def daily_sales_reporting_pipeline(execution_date: Optional[str] = None):
    """
    Daily sales reporting pipeline.
    
    Schedule: Daily at midnight starting 2024-01-01, catchup=False
    """
    if execution_date is None:
        execution_date = datetime.now().strftime('%Y-%m-%d')
    
    data = query_sales_data(execution_date)
    csv_path = transform_to_csv(data, execution_date)
    pdf_path = generate_pdf_chart(data, execution_date)
    email_sales_report(csv_path, pdf_path, execution_date)


if __name__ == '__main__':
    daily_sales_reporting_pipeline()