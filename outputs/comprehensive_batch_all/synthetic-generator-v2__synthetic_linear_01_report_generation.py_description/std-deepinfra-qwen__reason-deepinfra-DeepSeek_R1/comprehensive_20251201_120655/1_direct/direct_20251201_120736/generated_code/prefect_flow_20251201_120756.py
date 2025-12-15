from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_email import EmailServerCredentials, email_send_message
from prefect_schedules import IntervalSchedule
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import os
import psycopg2

# Task to query sales data from PostgreSQL
@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def query_sales_data(execution_date):
    logger = get_run_logger()
    logger.info(f"Querying sales data for {execution_date}")
    conn = psycopg2.connect(database="postgres_default")
    query = f"""
    SELECT product, SUM(sales) as total_sales
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

# Task to generate a PDF chart from the CSV data
@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(csv_path):
    logger = get_run_logger()
    logger.info("Generating PDF chart")
    df = pd.read_csv(csv_path)
    plt.figure(figsize=(10, 6))
    plt.bar(df['product'], df['total_sales'])
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.title('Sales by Product')
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path)
    return pdf_path

# Task to send the email with CSV and PDF attachments
@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path, pdf_path):
    logger = get_run_logger()
    logger.info("Sending email with sales report")
    email_credentials = EmailServerCredentials.load("email-credentials")
    email_send_message(
        email_server_credentials=email_credentials,
        subject="Daily Sales Report",
        msg="Please find the attached sales report and chart.",
        email_to="management@company.com",
        attachments=[csv_path, pdf_path]
    )

# Flow to orchestrate the pipeline
@flow(name="Daily Sales Reporting Pipeline", retries=2, retry_delay_seconds=300)
def daily_sales_reporting_pipeline(execution_date):
    logger = get_run_logger()
    logger.info(f"Starting daily sales reporting pipeline for {execution_date}")
    
    # Query sales data
    sales_data = query_sales_data(execution_date)
    
    # Transform to CSV
    csv_path = transform_to_csv(sales_data)
    
    # Generate PDF chart
    pdf_path = generate_pdf_chart(csv_path)
    
    # Email the report
    email_sales_report(csv_path, pdf_path)

# Schedule configuration (optional)
# schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1), end_date=None, timezone="UTC", catchup=False)

if __name__ == "__main__":
    execution_date = datetime.now().strftime("%Y-%m-%d")
    daily_sales_reporting_pipeline(execution_date)