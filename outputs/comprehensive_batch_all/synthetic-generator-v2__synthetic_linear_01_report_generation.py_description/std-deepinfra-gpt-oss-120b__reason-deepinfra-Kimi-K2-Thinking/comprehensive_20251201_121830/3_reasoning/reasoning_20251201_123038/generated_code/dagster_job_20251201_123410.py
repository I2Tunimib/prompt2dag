from dagster import (
    op,
    job,
    schedule,
    ScheduleDefinition,
    RetryPolicy,
    resource,
    RunRequest,
    DefaultScheduleStatus,
    get_dagster_logger,
    Out,
    In,
    Nothing,
    run_failure_sensor,
    RunFailureSensorContext,
)
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
from datetime import datetime
from typing import List, Dict, Any

logger = get_dagster_logger()

# Resources
@resource
def postgres_resource(context):
    # In production, use environment variables or secrets management
    return {
        "host": "localhost",
        "port": 5432,
        "database": "sales_db",
        "user": "postgres",
        "password": "password",
    }

@resource
def smtp_resource(context):
    # In production, use environment variables or secrets management
    return {
        "host": "smtp.company.com",
        "port": 587,
        "user": "noreply@company.com",
        "password": "smtp_password",
    }

# Ops
@op(
    out={"sales_data": Out(List[Dict[str, Any]])},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"kind": "postgres"},
)
def query_sales_data(context):
    """Extract daily sales data aggregated by product from PostgreSQL."""
    # Get execution date from context
    execution_date = context.partition_key if context.has_partition_key else datetime.now().strftime("%Y-%m-%d")
    
    # Connect to PostgreSQL
    pg_config = context.resources.postgres_resource
    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        database=pg_config["database"],
        user=pg_config["user"],
        password=pg_config["password"],
    )
    
    query = f"""
    SELECT product_name, SUM(sales_amount) as total_sales
    FROM sales
    WHERE sale_date = '{execution_date}'
    GROUP BY product_name
    ORDER BY product_name;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        # Convert to list of dicts for passing between ops
        return df.to_dict("records")
    except Exception as e:
        conn.close()
        logger.error(f"Failed to query sales data: {e}")
        raise

@op(
    ins={"sales_data": In(List[Dict[str, Any]])},
    out={"csv_path": Out(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"kind": "file"},
)
def transform_to_csv(context, sales_data):
    """Transform query results into a CSV file."""
    if not sales_data:
        logger.warning("No sales data to write to CSV")
        return "/tmp/sales_report.csv"  # Return path even if empty
    
    df = pd.DataFrame(sales_data)
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"CSV file created at {csv_path}")
    return csv_path

@op(
    ins={"sales_data": In(List[Dict[str, Any]])},
    out={"pdf_path": Out(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"kind": "file"},
)
def generate_pdf_chart(context, sales_data):
    """Create a PDF bar chart visualizing sales by product."""
    if not sales_data:
        logger.warning("No sales data to generate chart")
        return "/tmp/sales_chart.pdf"  # Return path even if empty
    
    df = pd.DataFrame(sales_data)
    
    plt.figure(figsize=(10, 6))
    plt.bar(df["product_name"], df["total_sales"])
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path)
    plt.close()
    logger.info(f"PDF chart created at {pdf_path}")
    return pdf_path

@op(
    ins={
        "csv_path": In(str),
        "pdf_path": In(str),
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"kind": "email"},
)
def email_sales_report(context, csv_path, pdf_path):
    """Send email with CSV and PDF attachments to management."""
    smtp_config = context.resources.smtp_resource
    
    # Create message
    msg = MIMEMultipart()
    msg["From"] = smtp_config["user"]
    msg["To"] = "management@company.com"
    msg["Subject"] = "Daily Sales Report"
    
    body = "Please find attached the daily sales report (CSV) and chart (PDF)."
    msg.attach(MIMEText(body, "plain"))
    
    # Attach CSV
    try:
        with open(csv_path, "rb") as f:
            csv_part = MIMEApplication(f.read(), _subtype="csv")
            csv_part.add_header("Content-Disposition", "attachment", filename="sales_report.csv")
            msg.attach(csv_part)
    except FileNotFoundError:
        logger.warning(f"CSV file not found at {csv_path}")
    
    # Attach PDF
    try:
        with open(pdf_path, "rb") as f:
            pdf_part = MIMEApplication(f.read(), _subtype="pdf")
            pdf_part.add_header("Content-Disposition", "attachment", filename="sales_chart.pdf")
            msg.attach(pdf_part)
    except FileNotFoundError:
        logger.warning(f"PDF file not found at {pdf_path}")
    
    # Send email
    try:
        server = smtplib.SMTP(smtp_config["host"], smtp_config["port"])
        server.starttls()
        server.login(smtp_config["user"], smtp_config["password"])
        server.send_message(msg)
        server.quit()
        logger.info("Email sent successfully to management@company.com")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise

# Job
@job(
    resource_defs={
        "postgres_resource": postgres_resource,
        "smtp_resource": smtp_resource,
    },
    tags={"pipeline": "daily_sales_report"},
)
def daily_sales_report_job():
    """Daily sales reporting pipeline."""
    sales_data = query_sales_data()
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(sales_data)
    email_sales_report(csv_path, pdf_path)

# Schedule
daily_schedule = ScheduleDefinition(
    job=daily_sales_report_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

# Run failure sensor for email notifications
@run_failure_sensor
def email_on_pipeline_failure(context: RunFailureSensorContext):
    """Send email notification on pipeline failure."""
    # This is a simplified version. In production, you'd use a proper email resource
    logger.error(
        f"Pipeline {context.dagster_run.job_name} failed! "
        f"Run ID: {context.dagster_run.run_id}, "
        f"Error: {context.failure_event.message}"
    )
    # Actual email notification would go here using SMTP resource

# Launch pattern
if __name__ == "__main__":
    # For testing, you can execute the job in-process
    result = daily_sales_report_job.execute_in_process()
    if result.success:
        logger.info("Pipeline executed successfully")
    else:
        logger.error("Pipeline execution failed")