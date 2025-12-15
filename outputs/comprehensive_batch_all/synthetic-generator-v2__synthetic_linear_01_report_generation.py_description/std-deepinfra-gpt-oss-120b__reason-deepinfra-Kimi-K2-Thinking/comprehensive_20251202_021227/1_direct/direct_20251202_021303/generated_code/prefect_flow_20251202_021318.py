import os
import smtplib
import pathlib
from email.message import EmailMessage

import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


def _send_failure_email(task_name: str, error: str) -> None:
    """Send a simple email notification when a task fails."""
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    sender = os.getenv("SMTP_SENDER", "prefect@company.com")
    recipient = os.getenv("SMTP_RECIPIENT", "admin@company.com")

    msg = EmailMessage()
    msg["Subject"] = f"Prefect Task Failure: {task_name}"
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(f"The task **{task_name}** failed with the following error:\n\n{error}")

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.send_message(msg)
    except Exception as exc:  # pragma: no cover
        logger = get_run_logger()
        logger.error("Failed to send failure notification email: %s", exc)


def _failure_handler(task, state, **kwargs):
    """Prefect on_failure hook."""
    error_msg = str(state.result) if state.result else "No error message"
    _send_failure_email(task.name, error_msg)


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=_failure_handler,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def query_sales_data() -> pd.DataFrame:
    """
    Execute a PostgreSQL query to extract daily aggregated sales data.

    Returns:
        pandas.DataFrame: Query result with columns `product` and `sales`.
    """
    logger = get_run_logger()
    dsn = os.getenv(
        "POSTGRES_DSN",
        "host=localhost dbname=postgres user=postgres password=postgres port=5432",
    )
    sql = """
        SELECT product, SUM(quantity) AS sales
        FROM sales
        WHERE sale_date = CURRENT_DATE
        GROUP BY product
        ORDER BY product;
    """
    logger.info("Connecting to PostgreSQL.")
    conn = psycopg2.connect(dsn)
    try:
        df = pd.read_sql_query(sql, conn)
        logger.info("Retrieved %d rows from PostgreSQL.", len(df))
        return df
    finally:
        conn.close()
        logger.debug("PostgreSQL connection closed.")


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=_failure_handler,
)
def transform_to_csv(df: pd.DataFrame) -> pathlib.Path:
    """
    Write the DataFrame to a CSV file.

    Args:
        df: DataFrame containing sales data.

    Returns:
        Path to the generated CSV file.
    """
    logger = get_run_logger()
    csv_path = pathlib.Path("/tmp/sales_report.csv")
    df.to_csv(csv_path, index=False)
    logger.info("CSV report written to %s.", csv_path)
    return csv_path


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=_failure_handler,
)
def generate_pdf_chart(df: pd.DataFrame) -> pathlib.Path:
    """
    Generate a PDF bar chart visualizing sales by product.

    Args:
        df: DataFrame containing sales data.

    Returns:
        Path to the generated PDF chart.
    """
    logger = get_run_logger()
    pdf_path = pathlib.Path("/tmp/sales_chart.pdf")
    plt.figure(figsize=(10, 6))
    plt.bar(df["product"], df["sales"], color="steelblue")
    plt.xlabel("Product")
    plt.ylabel("Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info("PDF chart saved to %s.", pdf_path)
    return pdf_path


@task(
    retries=2,
    retry_delay_seconds=300,
    on_failure=_failure_handler,
)
def email_sales_report(csv_path: pathlib.Path, pdf_path: pathlib.Path) -> None:
    """
    Email the CSV and PDF report to management.

    Args:
        csv_path: Path to the CSV report.
        pdf_path: Path to the PDF chart.
    """
    logger = get_run_logger()
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    sender = os.getenv("SMTP_SENDER", "reports@company.com")
    recipient = "management@company.com"

    msg = EmailMessage()
    msg["Subject"] = f"Daily Sales Report - {pd.Timestamp('today').date()}"
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content("Please find attached the daily sales CSV report and PDF chart.")

    # Attach CSV
    with open(csv_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=csv_path.name,
        )
    # Attach PDF
    with open(pdf_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename=pdf_path.name,
        )

    logger.info("Sending email to %s via %s:%s.", recipient, smtp_host, smtp_port)
    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.send_message(msg)
    logger.info("Email sent successfully.")


@flow
def daily_sales_report_flow() -> None:
    """
    Orchestrates the daily sales reporting pipeline.

    Execution order:
        query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report
    """
    df = query_sales_data()
    csv_path = transform_to_csv(df)
    pdf_path = generate_pdf_chart(df)
    email_sales_report(csv_path, pdf_path)


# Schedule configuration (to be applied during deployment):
# - Interval: daily
# - Start date: 2024-01-01
# - Catchup: disabled
# - Retries: 2 with 5‑minute delay (configured per task)

if __name__ == "__main__":
    daily_sales_report_flow()