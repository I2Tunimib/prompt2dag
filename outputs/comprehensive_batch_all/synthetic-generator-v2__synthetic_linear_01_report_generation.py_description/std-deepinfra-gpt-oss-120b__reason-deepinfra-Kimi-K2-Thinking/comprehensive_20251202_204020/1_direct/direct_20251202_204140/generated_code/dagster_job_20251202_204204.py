import os
import smtplib
import logging
from datetime import datetime, timedelta
from email.message import EmailMessage

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from dagster import (
    op,
    job,
    RetryPolicy,
    ConfigurableResource,
    InitResourceContext,
    ResourceDefinition,
    ScheduleDefinition,
    schedule,
    ScheduleStatus,
    get_dagster_logger,
)


class PostgresResource(ConfigurableResource):
    """Simple PostgreSQL resource using SQLAlchemy."""

    connection_string: str = "postgresql://user:password@localhost:5432/database"

    def get_engine(self):
        return create_engine(self.connection_string)


class EmailResource(ConfigurableResource):
    """Minimal SMTP email resource."""

    smtp_server: str = "localhost"
    smtp_port: int = 25
    username: str | None = None
    password: str | None = None
    use_tls: bool = False
    from_address: str = "noreply@company.com"

    def send_email(self, to_address: str, subject: str, body: str, attachments: list[tuple[str, bytes]]):
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_address
        msg["To"] = to_address
        msg.set_content(body)

        for filename, content in attachments:
            maintype, subtype = ("application", "octet-stream")
            msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.send_message(msg)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"postgres"},
    description="Execute a PostgreSQL query to extract daily sales data aggregated by product.",
)
def query_sales_data(context: InitResourceContext) -> pd.DataFrame:
    logger = get_dagster_logger()
    engine = context.resources.postgres.get_engine()
    query = """
        SELECT product_id, product_name, SUM(sales_amount) AS total_sales
        FROM sales
        WHERE sale_date = CURRENT_DATE
        GROUP BY product_id, product_name
        ORDER BY total_sales DESC;
    """
    logger.info("Running sales query against PostgreSQL.")
    df = pd.read_sql_query(query, con=engine)
    logger.info("Query returned %d rows.", len(df))
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Transform query results into a CSV file stored at /tmp/sales_report.csv.",
)
def transform_to_csv(context: InitResourceContext, sales_df: pd.DataFrame) -> str:
    logger = get_dagster_logger()
    csv_path = "/tmp/sales_report.csv"
    logger.info("Writing DataFrame to CSV at %s.", csv_path)
    sales_df.to_csv(csv_path, index=False)
    logger.info("CSV file written successfully.")
    return csv_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    description="Create a PDF bar chart visualizing sales by product stored at /tmp/sales_chart.pdf.",
)
def generate_pdf_chart(context: InitResourceContext, csv_path: str) -> str:
    logger = get_dagster_logger()
    logger.info("Reading CSV data from %s for chart generation.", csv_path)
    df = pd.read_csv(csv_path)

    plt.figure(figsize=(10, 6))
    plt.bar(df["product_name"], df["total_sales"], color="skyblue")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    pdf_path = "/tmp/sales_chart.pdf"
    logger.info("Saving PDF chart to %s.", pdf_path)
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info("PDF chart generated successfully.")
    return pdf_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    required_resource_keys={"email"},
    description="Send an email with both the CSV data file and PDF chart attached to management.",
)
def email_sales_report(context: InitResourceContext, csv_path: str, pdf_path: str) -> None:
    logger = get_dagster_logger()
    email_res: EmailResource = context.resources.email

    logger.info("Preparing email attachments.")
    attachments = []
    for path in (csv_path, pdf_path):
        with open(path, "rb") as f:
            content = f.read()
            filename = os.path.basename(path)
            attachments.append((filename, content))

    subject = f"Daily Sales Report - {datetime.utcnow().date()}"
    body = "Please find attached the daily sales CSV report and the sales chart PDF."

    logger.info("Sending email to management@company.com.")
    email_res.send_email(
        to_address="management@company.com",
        subject=subject,
        body=body,
        attachments=attachments,
    )
    logger.info("Email sent successfully.")


@job(
    description="Linear daily sales reporting pipeline: query → CSV → PDF → email.",
    resource_defs={
        "postgres": ResourceDefinition.hardcoded_resource(PostgresResource()),
        "email": ResourceDefinition.hardcoded_resource(EmailResource()),
    },
)
def daily_sales_report_job():
    sales_df = query_sales_data()
    csv_path = transform_to_csv(sales_df)
    pdf_path = generate_pdf_chart(csv_path)
    email_sales_report(csv_path, pdf_path)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=daily_sales_report_job,
    execution_timezone="UTC",
    start_date=datetime(2024, 1, 1),
    default_status=ScheduleStatus.RUNNING,
    description="Daily execution of the sales reporting pipeline.",
)
def daily_sales_report_schedule():
    """Schedule that triggers the daily_sales_report_job each day."""
    return {}


if __name__ == "__main__":
    result = daily_sales_report_job.execute_in_process()
    if result.success:
        logging.info("Pipeline executed successfully.")
    else:
        logging.error("Pipeline execution failed.")