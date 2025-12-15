import datetime
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from dagster import (
    ConfigurableResource,
    HookContext,
    RetryPolicy,
    failure_hook,
    job,
    op,
    resource,
    schedule,
    DefaultScheduleStatus,
)


class PostgresResource(ConfigurableResource):
    """Simple PostgreSQL resource using psycopg2."""

    host: str = "localhost"
    port: int = 5432
    dbname: str = "sales_db"
    user: str = "postgres"
    password: str = "postgres"

    def get_connection(self):
        """Create a new psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )


class SmtpResource(ConfigurableResource):
    """Simple SMTP resource."""

    host: str = "localhost"
    port: int = 25
    username: str | None = None
    password: str | None = None
    use_tls: bool = False

    def get_client(self):
        client = smtplib.SMTP(self.host, self.port)
        if self.use_tls:
            client.starttls()
        if self.username and self.password:
            client.login(self.username, self.password)
        return client


@failure_hook
def email_on_failure(context: HookContext):
    """Placeholder failure hook that could send an alert email."""
    # In a real implementation you would use the SmtpResource to send an email.
    # Here we simply log the failure.
    context.log.error(f"Op {context.op.name} failed. Exception: {context.exception}")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    required_resource_keys={"postgres"},
    tags={"dagster/priority": "high"},
    description="Execute a PostgreSQL query to extract daily sales data aggregated by product.",
    out={"sales_df": None},
    hooks={email_on_failure},
)
def query_sales_data(context) -> pd.DataFrame:
    """Query sales data for the current execution date."""
    execution_date = context.instance.get_current_time().date()
    sql = """
        SELECT product_id, product_name, SUM(quantity) AS total_quantity, SUM(amount) AS total_amount
        FROM sales
        WHERE sale_date = %s
        GROUP BY product_id, product_name
        ORDER BY product_name;
    """
    conn = context.resources.postgres.get_connection()
    try:
        df = pd.read_sql(sql, conn, params=(execution_date,))
        context.log.info(f"Retrieved {len(df)} rows for {execution_date}.")
        return df
    finally:
        conn.close()


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    description="Transform the query results into a CSV file.",
    tags={"dagster/priority": "high"},
    hooks={email_on_failure},
)
def transform_to_csv(context, sales_df: pd.DataFrame) -> str:
    """Write the DataFrame to a CSV file and return the file path."""
    csv_path = Path("/tmp/sales_report.csv")
    sales_df.to_csv(csv_path, index=False)
    context.log.info(f"CSV written to {csv_path}.")
    return str(csv_path)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    description="Generate a PDF bar chart visualizing sales by product.",
    tags={"dagster/priority": "high"},
    hooks={email_on_failure},
)
def generate_pdf_chart(context, sales_df: pd.DataFrame) -> str:
    """Create a bar chart PDF from the sales DataFrame."""
    pdf_path = Path("/tmp/sales_chart.pdf")
    plt.figure(figsize=(10, 6))
    plt.bar(sales_df["product_name"], sales_df["total_quantity"], color="skyblue")
    plt.xlabel("Product")
    plt.ylabel("Total Quantity Sold")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    context.log.info(f"PDF chart written to {pdf_path}.")
    return str(pdf_path)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    required_resource_keys={"smtp"},
    description="Email the CSV and PDF report to management.",
    tags={"dagster/priority": "high"},
    hooks={email_on_failure},
)
def email_sales_report(context, csv_path: str, pdf_path: str) -> None:
    """Send an email with the CSV and PDF attached."""
    msg = EmailMessage()
    msg["Subject"] = "Daily Sales Report"
    msg["From"] = "reports@company.com"
    msg["To"] = "management@company.com"
    msg.set_content("Please find attached the daily sales CSV and PDF chart.")

    # Attach CSV
    with open(csv_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(csv_path),
        )

    # Attach PDF
    with open(pdf_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename=os.path.basename(pdf_path),
        )

    smtp_client = context.resources.smtp.get_client()
    try:
        smtp_client.send_message(msg)
        context.log.info(f"Email sent to {msg['To']}.")
    finally:
        smtp_client.quit()


@job(
    resource_defs={
        "postgres": PostgresResource(),
        "smtp": SmtpResource(),
    },
    description="Daily sales reporting pipeline.",
)
def daily_sales_report_job():
    sales_df = query_sales_data()
    csv_path = transform_to_csv(sales_df)
    pdf_path = generate_pdf_chart(sales_df)
    email_sales_report(csv_path, pdf_path)


@schedule(
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    job=daily_sales_report_job,
    execution_timezone="UTC",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    default_status=DefaultScheduleStatus.RUNNING,
    catchup=False,
    description="Daily execution of the sales reporting pipeline.",
)
def daily_sales_report_schedule():
    return {}


if __name__ == "__main__":
    result = daily_sales_report_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")