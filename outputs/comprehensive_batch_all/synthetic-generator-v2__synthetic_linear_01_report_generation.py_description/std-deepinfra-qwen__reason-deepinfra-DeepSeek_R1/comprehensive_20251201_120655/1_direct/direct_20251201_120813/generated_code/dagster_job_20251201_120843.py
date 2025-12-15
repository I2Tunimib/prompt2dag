from dagster import op, job, ScheduleDefinition, DefaultScheduleStatus, RetryPolicy, Failure, success_hook, failure_hook
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import psycopg2

# Resources
# postgres_resource = {
#     "config": {
#         "host": "localhost",
#         "user": "user",
#         "password": "password",
#         "dbname": "sales_db"
#     }
# }

# smtp_resource = {
#     "config": {
#         "smtp_server": "smtp.company.com",
#         "smtp_port": 587,
#         "smtp_user": "user@company.com",
#         "smtp_password": "password"
#     }
# }

@op
def query_sales_data(context):
    """Extract daily sales data from PostgreSQL."""
    conn = psycopg2.connect(
        host=context.resources.postgres_resource.config["host"],
        user=context.resources.postgres_resource.config["user"],
        password=context.resources.postgres_resource.config["password"],
        dbname=context.resources.postgres_resource.config["dbname"]
    )
    cursor = conn.cursor()
    query = "SELECT product, SUM(amount) AS total_sales FROM sales WHERE date = CURRENT_DATE GROUP BY product;"
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

@op
def transform_to_csv(context, sales_data):
    """Transform the query results into a CSV file."""
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

@op
def generate_pdf_chart(context, csv_path):
    """Create a PDF bar chart visualizing sales by product."""
    df = pd.read_csv(csv_path)
    plt.figure(figsize=(10, 6))
    plt.bar(df["product"], df["total_sales"])
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45)
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

@op
def email_sales_report(context, csv_path, pdf_path):
    """Send an email with both the CSV data file and PDF chart attached."""
    msg = MIMEMultipart()
    msg["From"] = context.resources.smtp_resource.config["smtp_user"]
    msg["To"] = "management@company.com"
    msg["Subject"] = "Daily Sales Report"

    with open(csv_path, "rb") as file:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(csv_path)}")
        msg.attach(part)

    with open(pdf_path, "rb") as file:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(pdf_path)}")
        msg.attach(part)

    server = smtplib.SMTP(context.resources.smtp_resource.config["smtp_server"], context.resources.smtp_resource.config["smtp_port"])
    server.starttls()
    server.login(context.resources.smtp_resource.config["smtp_user"], context.resources.smtp_resource.config["smtp_password"])
    server.sendmail(msg["From"], msg["To"], msg.as_string())
    server.quit()

@success_hook
def notify_success(context):
    context.log.info("Pipeline executed successfully.")

@failure_hook
def notify_failure(context):
    context.log.error("Pipeline failed.")

@job(
    resource_defs={
        "postgres_resource": postgres_resource,
        "smtp_resource": smtp_resource
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    hooks=[notify_success, notify_failure]
)
def daily_sales_report_job():
    sales_data = query_sales_data()
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(csv_path)
    email_sales_report(csv_path, pdf_path)

daily_sales_report_schedule = ScheduleDefinition(
    job=daily_sales_report_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
    start_date="2024-01-01",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="UTC",
    should_execute=lambda: True,
    tags={"dagster/is_partitioned": "False"},
    name="daily_sales_report_schedule"
)

if __name__ == "__main__":
    result = daily_sales_report_job.execute_in_process()