from datetime import datetime
import os
import smtplib
import ssl
from email.message import EmailMessage

import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from prefect import flow, task
from prefect.logging import get_logger


@task(retries=2, retry_delay_seconds=300)
def query_sales_data() -> pd.DataFrame:
    """
    Execute a PostgreSQL query to extract daily sales data aggregated by product
    for the current execution date.

    Returns:
        pandas.DataFrame: Query results with columns ['product', 'total_sales'].
    """
    logger = get_logger()
    # Connection parameters can be set via environment variables or defaults
    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "salesdb"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }

    query_date = datetime.utcnow().date()
    sql = """
        SELECT product, SUM(amount) AS total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product
        ORDER BY product;
    """

    logger.info("Connecting to PostgreSQL and executing query for %s", query_date)
    with psycopg2.connect(**conn_params) as conn:
        df = pd.read_sql(sql, conn, params=(query_date,))
    logger.info("Query returned %d rows", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(sales_df: pd.DataFrame) -> str:
    """
    Transform the sales DataFrame into a CSV file.

    Args:
        sales_df: DataFrame containing sales data.

    Returns:
        Path to the generated CSV file.
    """
    logger = get_logger()
    csv_path = "/tmp/sales_report.csv"
    logger.info("Writing sales data to CSV at %s", csv_path)
    sales_df.to_csv(csv_path, index=False)
    return csv_path


@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(sales_df: pd.DataFrame) -> str:
    """
    Generate a PDF bar chart visualizing sales by product.

    Args:
        sales_df: DataFrame containing sales data.

    Returns:
        Path to the generated PDF chart.
    """
    logger = get_logger()
    pdf_path = "/tmp/sales_chart.pdf"
    logger.info("Generating PDF chart at %s", pdf_path)

    plt.figure(figsize=(10, 6))
    plt.bar(sales_df["product"], sales_df["total_sales"], color="skyblue")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    return pdf_path


@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str) -> None:
    """
    Email the CSV report and PDF chart to management.

    Args:
        csv_path: Path to the CSV file.
        pdf_path: Path to the PDF chart.
    """
    logger = get_logger()
    smtp_server = os.getenv("SMTP_SERVER", "smtp.example.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "no-reply@example.com")
    smtp_password = os.getenv("SMTP_PASSWORD", "")
    recipient = "management@company.com"

    logger.info("Composing email to %s", recipient)
    message = EmailMessage()
    message["Subject"] = f"Daily Sales Report - {datetime.utcnow().date()}"
    message["From"] = smtp_user
    message["To"] = recipient
    message.set_content(
        "Please find attached the daily sales CSV report and the sales chart PDF."
    )

    # Attach CSV
    with open(csv_path, "rb") as f:
        message.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(csv_path),
        )

    # Attach PDF
    with open(pdf_path, "rb") as f:
        message.add_attachment(
            f.read(),
            maintype="application",
            subtype="pdf",
            filename=os.path.basename(pdf_path),
        )

    logger.info("Sending email via SMTP server %s:%s", smtp_server, smtp_port)
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls(context=context)
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.send_message(message)
    logger.info("Email sent successfully")


@flow
def daily_sales_reporting_flow() -> None:
    """
    Orchestrates the daily sales reporting pipeline:
    1. Query sales data from PostgreSQL.
    2. Transform results to CSV.
    3. Generate PDF chart.
    4. Email the compiled report.
    """
    sales_df = query_sales_data()
    csv_path = transform_to_csv(sales_df)
    pdf_path = generate_pdf_chart(sales_df)
    email_sales_report(csv_path, pdf_path)


# Schedule configuration (to be applied in deployment):
# - Interval: daily
# - Start date: 2024-01-01
# - Catchup: disabled
# - Retry policy: 2 retries with 5‑minute delay (configured per task)

if __name__ == "__main__":
    daily_sales_reporting_flow()