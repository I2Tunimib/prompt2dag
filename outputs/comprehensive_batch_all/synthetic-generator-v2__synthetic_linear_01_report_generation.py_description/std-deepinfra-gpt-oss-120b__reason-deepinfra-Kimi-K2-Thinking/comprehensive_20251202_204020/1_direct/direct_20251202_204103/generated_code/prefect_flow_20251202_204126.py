import os
import smtplib
import logging
from datetime import datetime, date
from email.message import EmailMessage

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.logging import get_logger

# Configure logger
logger = get_logger(__name__)

# Connection details – replace with actual credentials or use environment variables
POSTGRES_CONN_STR = os.getenv(
    "POSTGRES_CONN_STR",
    "postgresql://user:password@localhost:5432/database",
)

SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

RECIPIENT_EMAIL = "management@company.com"
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "noreply@company.com")

CSV_PATH = "/tmp/sales_report.csv"
PDF_PATH = "/tmp/sales_chart.pdf"


def _send_failure_email(task_name: str, exc: Exception) -> None:
    """Send a simple email notification when a task fails."""
    try:
        msg = EmailMessage()
        msg["Subject"] = f"Prefect Task Failure: {task_name}"
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECIPIENT_EMAIL
        msg.set_content(
            f"The task '{task_name}' failed with the following exception:\n\n{exc}"
        )
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_USER and SMTP_PASSWORD:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        logger.info("Failure notification email sent for task %s", task_name)
    except Exception as email_exc:  # pragma: no cover
        logger.error(
            "Failed to send failure notification email for task %s: %s",
            task_name,
            email_exc,
        )


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=[_send_failure_email],
    cache_key_fn=task_input_hash,
    cache_expiration=None,
)
def query_sales_data(execution_date: date | None = None) -> pd.DataFrame:
    """
    Execute a PostgreSQL query to extract daily sales data aggregated by product.

    Args:
        execution_date: The date for which to retrieve sales data. Defaults to today.

    Returns:
        DataFrame containing product, total_sales columns.
    """
    if execution_date is None:
        execution_date = datetime.utcnow().date()
    logger.info("Querying sales data for %s", execution_date)

    engine = create_engine(POSTGRES_CONN_STR)
    query = text(
        """
        SELECT
            product_id,
            SUM(quantity) AS total_quantity,
            SUM(total_price) AS total_sales
        FROM sales
        WHERE sale_date = :sale_date
        GROUP BY product_id
        ORDER BY product_id;
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"sale_date": execution_date})
    logger.info("Retrieved %d rows of sales data", len(df))
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=[_send_failure_email],
)
def transform_to_csv(df: pd.DataFrame) -> str:
    """
    Write the sales DataFrame to a CSV file.

    Args:
        df: DataFrame with sales data.

    Returns:
        Path to the generated CSV file.
    """
    logger.info("Writing sales data to CSV at %s", CSV_PATH)
    df.to_csv(CSV_PATH, index=False)
    logger.info("CSV file written successfully")
    return CSV_PATH


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=[_send_failure_email],
)
def generate_pdf_chart(df: pd.DataFrame) -> str:
    """
    Generate a PDF bar chart visualizing sales by product.

    Args:
        df: DataFrame with sales data.

    Returns:
        Path to the generated PDF chart.
    """
    logger.info("Generating PDF chart at %s", PDF_PATH)
    plt.figure(figsize=(10, 6))
    plt.bar(df["product_id"].astype(str), df["total_sales"], color="skyblue")
    plt.xlabel("Product ID")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(PDF_PATH, format="pdf")
    plt.close()
    logger.info("PDF chart generated successfully")
    return PDF_PATH


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=[_send_failure_email],
)
def email_sales_report(csv_path: str, pdf_path: str) -> None:
    """
    Email the CSV and PDF report to management.

    Args:
        csv_path: Path to the CSV file.
        pdf_path: Path to the PDF chart.
    """
    logger.info("Composing email with attachments: %s, %s", csv_path, pdf_path)
    msg = EmailMessage()
    msg["Subject"] = f"Daily Sales Report - {datetime.utcnow().date()}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL
    msg.set_content(
        "Please find attached the daily sales CSV report and the sales chart PDF."
    )

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

    # Send email
    logger.info("Sending email to %s", RECIPIENT_EMAIL)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_USER and SMTP_PASSWORD:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    logger.info("Email sent successfully")


@flow
def daily_sales_reporting_flow() -> None:
    """
    Orchestrates the daily sales reporting pipeline.

    Schedule (to be configured in deployment):
        - Interval: daily
        - Start date: 2024-01-01
        - Catchup: disabled
    """
    df = query_sales_data()
    csv_path = transform_to_csv(df)
    pdf_path = generate_pdf_chart(df)
    email_sales_report(csv_path, pdf_path)


if __name__ == "__main__":
    daily_sales_reporting_flow()