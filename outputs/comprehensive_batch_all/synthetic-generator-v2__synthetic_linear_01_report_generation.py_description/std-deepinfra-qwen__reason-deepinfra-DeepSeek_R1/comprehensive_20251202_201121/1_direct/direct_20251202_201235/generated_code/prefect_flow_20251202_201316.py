from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.schedules import IntervalSchedule
from datetime import timedelta, datetime
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def query_sales_data(execution_date):
    logger = get_run_logger()
    logger.info(f"Querying sales data for {execution_date}")
    # Placeholder for actual PostgreSQL connection and query
    query = f"SELECT product, SUM(sales) AS total_sales FROM sales WHERE date = '{execution_date}' GROUP BY product"
    # Simulate query results
    data = {
        'product': ['Product A', 'Product B', 'Product C'],
        'total_sales': [100, 200, 150]
    }
    df = pd.DataFrame(data)
    return df

@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(df):
    logger = get_run_logger()
    logger.info("Transforming data to CSV")
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(df):
    logger = get_run_logger()
    logger.info("Generating PDF chart")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 5))
    plt.bar(df['product'], df['total_sales'])
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.title('Sales by Product')
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path, pdf_path):
    logger = get_run_logger()
    logger.info("Sending email with sales report")
    from_addr = "report@company.com"
    to_addr = "management@company.com"
    subject = "Daily Sales Report"
    body = "Please find attached the daily sales report and chart."

    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    with open(csv_path, "rb") as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(csv_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(csv_path)}"'
        msg.attach(part)

    with open(pdf_path, "rb") as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(pdf_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(pdf_path)}"'
        msg.attach(part)

    # Placeholder for actual SMTP server configuration
    with smtplib.SMTP('smtp.company.com', 587) as server:
        server.starttls()
        server.login(from_addr, "your_password")
        server.sendmail(from_addr, to_addr, msg.as_string())

@flow(name="Daily Sales Reporting Pipeline", retries=2, retry_delay_seconds=300)
def daily_sales_reporting_pipeline(execution_date):
    logger = get_run_logger()
    logger.info(f"Starting daily sales reporting pipeline for {execution_date}")

    df = query_sales_data(execution_date)
    csv_path = transform_to_csv(df)
    pdf_path = generate_pdf_chart(df)
    email_sales_report(csv_path, pdf_path)

if __name__ == '__main__':
    # Schedule: Daily execution via @daily interval
    # Start Date: January 1, 2024
    # Catchup: Disabled to prevent backfilling
    # Retry Policy: 2 retries with 5-minute delays between attempts
    # Failure Handling: Email notifications on task failures
    schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1), end_date=None)
    daily_sales_reporting_pipeline(execution_date=datetime.now().strftime('%Y-%m-%d'))