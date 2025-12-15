import os
import smtplib
from datetime import date
from typing import Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from prefect import flow, task


@task
def query_sales_data(execution_date: date) -> pd.DataFrame:
    """Extract daily sales data aggregated by product from PostgreSQL."""
    # In production, consider using Prefect Blocks for connection management
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'database': os.getenv('POSTGRES_DB', 'sales_db'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'password')
    }
    
    query = """
    SELECT product_name, SUM(amount) as total_sales
    FROM sales
    WHERE sale_date = %s
    GROUP BY product_name
    ORDER BY product_name
    """
    
    with psycopg2.connect(**conn_params, cursor_factory=RealDictCursor) as conn:
        df = pd.read_sql(query, conn, params=(execution_date,))
    
    return df


@task
def transform_to_csv(data: pd.DataFrame) -> str:
    """Transform query results into a CSV file at /tmp/sales_report.csv."""
    csv_path = '/tmp/sales_report.csv'
    data.to_csv(csv_path, index=False)
    return csv_path


@task
def generate_pdf_chart(data: pd.DataFrame) -> str:
    """Create a PDF bar chart visualizing sales by product at /tmp/sales_chart.pdf."""
    pdf_path = '/tmp/sales_chart.pdf'
    
    plt.figure(figsize=(12, 8))
    if data.empty:
        plt.text(0.5, 0.5, 'No sales data available', ha='center', va='center')
    else:
        plt.bar(data['product_name'], data['total_sales'])
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.title('Daily Sales by Product')
        plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()
    
    return pdf_path


@task
def email_sales_report(csv_path: str, pdf_path: str, execution_date: date) -> None:
    """Send email with CSV and PDF attachments to management@company.com."""
    smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER', 'noreply@company.com')
    smtp_password = os.getenv('SMTP_PASSWORD', 'app_password')
    
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = 'management@company.com'
    msg['Subject'] = f'Daily Sales Report - {execution_date}'
    
    body = f'Please find attached the daily sales report for {execution_date}.'
    msg.attach(MIMEText(body, 'plain'))
    
    # Attach CSV
    with open(csv_path, 'rb') as f:
        csv_part = MIMEApplication(f.read(), _subtype='csv')
        csv_part.add_header('Content-Disposition', 'attachment', filename='sales_report.csv')
        msg.attach(csv_part)
    
    # Attach PDF
    with open(pdf_path, 'rb') as f:
        pdf_part = MIMEApplication(f.read(), _subtype='pdf')
        pdf_part.add_header('Content-Disposition', 'attachment', filename='sales_chart.pdf')
        msg.attach(pdf_part)
    
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


@flow(
    name="daily-sales-report",
    retries=2,
    retry_delay_seconds=300,
    # Schedule: Daily at midnight starting 2024-01-01, catchup=False
    # Configure via deployment: prefect deployment build ... --schedule "0 0 * * *" --start-date "2024-01-01"
    # Failure notifications: Configure Prefect email notification block for the deployment
)
def daily_sales_report(execution_date: Optional[date] = None) -> None:
    """Linear daily sales reporting pipeline."""
    if execution_date is None:
        execution_date = date.today()
    
    # Linear execution chain
    sales_data = query_sales_data(execution_date)
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(sales_data)
    email_sales_report(csv_path, pdf_path, execution_date)


if __name__ == '__main__':
    daily_sales_report()