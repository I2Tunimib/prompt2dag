from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.schedules import IntervalSchedule
from datetime import timedelta, datetime
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
import os
import psycopg2

# Task to query sales data from PostgreSQL
@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def query_sales_data(execution_date):
    logger = get_run_logger()
    logger.info(f"Querying sales data for {execution_date}")
    conn = psycopg2.connect("dbname=postgres_default user=postgres password=secret host=localhost")
    query = f"""
    SELECT product, SUM(amount) as total_sales
    FROM sales
    WHERE date = '{execution_date}'
    GROUP BY product
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Task to transform query results into a CSV file
@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(df):
    logger = get_run_logger()
    logger.info("Transforming data to CSV")
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

# Task to generate a PDF chart visualizing sales by product
@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(df):
    logger = get_run_logger()
    logger.info("Generating PDF chart")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 6))
    plt.bar(df['product'], df['total_sales'])
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.title('Sales by Product')
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

# Task to send an email with the CSV and PDF attachments
@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path, pdf_path):
    logger = get_run_logger()
    logger.info("Sending email with sales report")
    sender = 'report@company.com'
    receivers = ['management@company.com']
    subject = 'Daily Sales Report'
    body = 'Please find the attached daily sales report.'

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = COMMASPACE.join(receivers)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    with open(csv_path, "rb") as file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(csv_path)}')
        msg.attach(part)

    with open(pdf_path, "rb") as file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(pdf_path)}')
        msg.attach(part)

    with smtplib.SMTP('localhost') as server:
        server.sendmail(sender, receivers, msg.as_string())

# Flow to orchestrate the pipeline
@flow(name="Daily Sales Reporting Pipeline", 
      schedule=IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1), end_date=None),
      retries=2, retry_delay_seconds=300)
def daily_sales_reporting_pipeline(execution_date):
    logger = get_run_logger()
    logger.info(f"Starting daily sales reporting pipeline for {execution_date}")

    df = query_sales_data(execution_date)
    csv_path = transform_to_csv(df)
    pdf_path = generate_pdf_chart(df)
    email_sales_report(csv_path, pdf_path)

if __name__ == '__main__':
    daily_sales_reporting_pipeline(execution_date=datetime.now().date())