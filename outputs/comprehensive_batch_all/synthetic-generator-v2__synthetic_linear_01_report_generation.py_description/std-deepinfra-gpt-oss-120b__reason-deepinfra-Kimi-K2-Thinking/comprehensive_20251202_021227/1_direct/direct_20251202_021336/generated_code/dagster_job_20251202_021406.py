from datetime import datetime, time, timedelta
import os
import mimetypes

import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.message import EmailMessage

from dagster import (
    op,
    job,
    RetryPolicy,
    ConfigurableResource,
    HookContext,
    failure_hook,
    daily_schedule,
)


class PostgresResource(ConfigurableResource):
    """Simple PostgreSQL connection resource."""

    host: str
    port: int = 5432
    dbname: str
    user: str
    password: str

    def get_connection(self):
        import psycopg2

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )


class SMTPResource(ConfigurableResource):
    """SMTP resource for sending emails."""

    host: str
    port: int = 587
    user: str
    password: str

    def send_email(self, subject: str, body: str, to: str, attachments: list[str]):
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.user
        msg["To"] = to
        msg.set_content(body)

        for attachment_path in attachments:
            with open(attachment_path, "rb") as f:
                data = f.read()
                ctype, encoding = mimetypes.guess_type(attachment_path)
                if ctype is None or encoding is not None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)
                filename = os.path.basename(attachment_path)
                msg.add_attachment(
                    data,
                    maintype=maintype,
                    subtype=subtype,
                    filename=filename,
                )

        with smtplib.SMTP(self.host, self.port) as server:
            server.starttls()
            server.login(self.user, self.password)
            server.send_message(msg)


RETRY_POLICY = RetryPolicy(max_retries=2, delay=timedelta(minutes=5))


@op(required_resource_keys={"postgres"}, retry_policy=RETRY_POLICY)
def query_sales_data(context) -> pd.DataFrame:
    """Execute a PostgreSQL query to retrieve daily sales aggregated by product."""
    conn = context.resources.postgres.get_connection()
    try:
        query = """
            SELECT product_id, product_name, SUM(quantity) AS total_quantity,
                   SUM(total_price) AS total_sales
            FROM sales
            WHERE sale_date = CURRENT_DATE
            GROUP BY product_id, product_name
            ORDER BY total_sales DESC;
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    context.log.info(f"Retrieved {len(df)} rows of sales data.")
    return df


@op(retry_policy=RETRY_POLICY)
def transform_to_csv(context, sales_df: pd.DataFrame) -> str:
    """Write the sales DataFrame to a CSV file."""
    csv_path = "/tmp/sales_report.csv"
    sales_df.to_csv(csv_path, index=False)
    context.log.info(f"CSV report written to {csv_path}.")
    return csv_path


@op(retry_policy=RETRY_POLICY)
def generate_pdf_chart(context, csv_path: str) -> str:
    """Create a PDF bar chart visualizing sales by product."""
    df = pd.read_csv(csv_path)
    top_products = df.nlargest(10, "total_sales")
    plt.figure(figsize=(10, 6))
    plt.bar(top_products["product_name"], top_products["total_sales"], color="steelblue")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Top 10 Products by Sales")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    context.log.info(f"PDF chart saved to {pdf_path}.")
    return pdf_path


@op(required_resource_keys={"smtp"}, retry_policy=RETRY_POLICY)
def email_sales_report(context, csv_path: str, pdf_path: str):
    """Email the CSV and PDF report to management."""
    subject = "Daily Sales Report"
    body = "Please find attached the daily sales CSV report and PDF chart."
    to_address = "management@company.com"
    attachments = [csv_path, pdf_path]
    context.resources.smtp.send_email(subject, body, to_address, attachments)
    context.log.info(f"Email sent to {to_address} with attachments.")


@failure_hook
def notify_failure(context: HookContext):
    """Send a failure notification email."""
    smtp: SMTPResource = context.resources.smtp
    subject = f"Dagster Job Failure: {context.job_name}"
    body = (
        f"The operation `{context.op_name}` failed.\n"
        f"Error: {context.error}\n"
        f"Timestamp: {datetime.utcnow().isoformat()}Z"
    )
    to_address = "admin@company.com"
    smtp.send_email(subject, body, to_address, attachments=[])


postgres_resource = PostgresResource(
    host="localhost",
    dbname="salesdb",
    user="sales_user",
    password="sales_pass",
)

smtp_resource = SMTPResource(
    host="smtp.example.com",
    user="no-reply@example.com",
    password="smtp_pass",
)


@job(
    resource_defs={"postgres": postgres_resource, "smtp": smtp_resource},
    hooks=[notify_failure],
)
def daily_sales_report():
    sales_df = query_sales_data()
    csv_path = transform_to_csv(sales_df)
    pdf_path = generate_pdf_chart(csv_path)
    email_sales_report(csv_path, pdf_path)


@daily_schedule(
    job=daily_sales_report,
    start_date=datetime(2024, 1, 1),
    execution_time=time(hour=0, minute=0),
    execution_timezone="UTC",
    default_status="RUNNING",
    description="Runs the daily sales reporting pipeline each day at midnight UTC.",
    catchup=False,
)
def daily_sales_report_schedule():
    return {}


if __name__ == "__main__":
    result = daily_sales_report.execute_in_process()
    if result.success:
        print("Daily sales report job completed successfully.")
    else:
        print("Daily sales report job failed.")