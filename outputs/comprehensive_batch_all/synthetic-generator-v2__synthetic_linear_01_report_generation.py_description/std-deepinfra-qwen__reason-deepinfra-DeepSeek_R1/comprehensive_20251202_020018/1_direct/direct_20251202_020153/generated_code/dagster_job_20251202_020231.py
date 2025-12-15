from dagster import op, job, resource, RetryPolicy, Failure, DailyPartitionsDefinition, get_dagster_logger
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import psycopg2

# Resources
@resource(config_schema={"host": str, "port": int, "database": str, "user": str, "password": str})
def postgres_resource(context):
    return psycopg2.connect(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
        database=context.resource_config["database"],
        user=context.resource_config["user"],
        password=context.resource_config["password"]
    )

@resource(config_schema={"smtp_server": str, "smtp_port": int, "email_from": str, "email_to": str, "email_password": str})
def smtp_resource(context):
    return {
        "smtp_server": context.resource_config["smtp_server"],
        "smtp_port": context.resource_config["smtp_port"],
        "email_from": context.resource_config["email_from"],
        "email_to": context.resource_config["email_to"],
        "email_password": context.resource_config["email_password"]
    }

# Ops
@op(required_resource_keys={"postgres"})
def query_sales_data(context):
    logger = get_dagster_logger()
    conn = context.resources.postgres
    cursor = conn.cursor()
    query = """
    SELECT product, SUM(sales) AS total_sales
    FROM sales
    WHERE date = CURRENT_DATE
    GROUP BY product
    """
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    logger.info(f"Extracted {len(df)} rows of sales data")
    return df

@op
def transform_to_csv(context, sales_data):
    logger = get_dagster_logger()
    csv_path = "/tmp/sales_report.csv"
    sales_data.to_csv(csv_path, index=False)
    logger.info(f"Transformed sales data to CSV: {csv_path}")

@op
def generate_pdf_chart(context, sales_data):
    logger = get_dagster_logger()
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 5))
    plt.bar(sales_data['product'], sales_data['total_sales'])
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.title('Daily Sales by Product')
    plt.savefig(pdf_path)
    plt.close()
    logger.info(f"Generated PDF chart: {pdf_path}")

@op(required_resource_keys={"smtp"})
def email_sales_report(context):
    logger = get_dagster_logger()
    smtp_config = context.resources.smtp
    csv_path = "/tmp/sales_report.csv"
    pdf_path = "/tmp/sales_chart.pdf"

    msg = MIMEMultipart()
    msg['From'] = smtp_config["email_from"]
    msg['To'] = smtp_config["email_to"]
    msg['Subject'] = "Daily Sales Report"

    with open(csv_path, "rb") as file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(csv_path)}')
        msg.attach(part)

    with open(pdf_path, "rb") as file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(pdf_path)}')
        msg.attach(part)

    with smtplib.SMTP(smtp_config["smtp_server"], smtp_config["smtp_port"]) as server:
        server.starttls()
        server.login(smtp_config["email_from"], smtp_config["email_password"])
        server.sendmail(smtp_config["email_from"], smtp_config["email_to"], msg.as_string())

    logger.info(f"Sent email with attachments to {smtp_config['email_to']}")

# Job
@job(
    resource_defs={
        "postgres": postgres_resource,
        "smtp": smtp_resource
    },
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Daily sales reporting pipeline"
)
def daily_sales_report_job():
    sales_data = query_sales_data()
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(sales_data)
    email_sales_report()

# Launch pattern
if __name__ == '__main__':
    result = daily_sales_report_job.execute_in_process()