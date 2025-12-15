import os
from datetime import datetime
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task
from sqlalchemy import create_engine

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


@task(retries=2, retry_delay_seconds=300)
def query_sales_data(execution_date: datetime) -> pd.DataFrame:
    """
    Extract daily sales data aggregated by product from PostgreSQL.
    """
    connection_string = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/sales_db"
    )
    
    engine = create_engine(connection_string)
    
    query = """
    SELECT 
        product_name,
        SUM(quantity) as total_quantity,
        SUM(revenue) as total_revenue
    FROM sales
    WHERE DATE(sale_date) = %(execution_date)s
    GROUP BY product_name
    ORDER BY total_revenue DESC
    """
    
    with engine.connect() as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params={"execution_date": execution_date.date()}
        )
    
    return df


@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    """
    Transform query results into a CSV file at /tmp/sales_report.csv
    """
    output_path = "/tmp/sales_report.csv"
    data.to_csv(output_path, index=False)
    return output_path


@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame) -> str:
    """
    Create a PDF bar chart at /tmp/sales_chart.pdf visualizing sales by product.
    """
    output_path = "/tmp/sales_chart.pdf"
    
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.bar(data['product_name'], data['total_revenue'])
    ax.set_xlabel('Product Name')
    ax.set_ylabel('Total Revenue')
    ax.set_title('Daily Sales by Product')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_path, format='pdf')
    plt.close()
    
    return output_path


@task(retries=2, retry_delay_seconds=300)
def email_sales_report(
    csv_path: str,
    pdf_path: str,
    execution_date: datetime
) -> None:
    """
    Send email with CSV and PDF attachments to management@company.com
    """
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "noreply@company.com")
    smtp_password = os.getenv("SMTP_PASSWORD", "app_password")
    from_email = os.getenv("FROM_EMAIL", "noreply@company.com")
    to_email = "management@company.com"
    
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = f"Daily Sales Report - {execution_date.strftime('%Y-%m-%d')}"
    
    body = f"Please find attached the daily sales report for {execution_date.strftime('%Y-%m-%d')}."
    msg.attach(MIMEText(body, 'plain'))
    
    with open(csv_path, 'rb') as f:
        csv_part = MIMEApplication(f.read(), _subtype='csv')
        csv_part.add_header('Content-Disposition', 'attachment', filename='sales_report.csv')
        msg.attach(csv_part)
    
    with open(pdf_path, 'rb') as f:
        pdf_part = MIMEApplication(f.read(), _subtype='pdf')
        pdf_part.add_header('Content-Disposition', 'attachment', filename='sales_chart.pdf')
        msg.attach(pdf_part)
    
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


@flow(
    name="daily-sales-report",
    description="Linear daily sales reporting pipeline"
)
# Schedule: Daily at 9 AM UTC starting 2024-01-01, catchup disabled
# Configure during deployment: prefect deployment build ... --cron "0 9 * * *" --start-date "2024-01-01T00:00:00Z" --no-catchup
# Failure notifications: Configure Prefect Automations for task failure alerts
def sales_report_pipeline(execution_date: Optional[datetime] = None):
    """
    Daily sales reporting pipeline that extracts data from PostgreSQL,
    transforms it into CSV and PDF formats, and emails the report.
    """
    if execution_date is None:
        execution_date = datetime.now()
    
    sales_data = query_sales_data(execution_date)
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(sales_data)
    email_sales_report(csv_path, pdf_path, execution_date)


if __name__ == '__main__':
    sales_report_pipeline()