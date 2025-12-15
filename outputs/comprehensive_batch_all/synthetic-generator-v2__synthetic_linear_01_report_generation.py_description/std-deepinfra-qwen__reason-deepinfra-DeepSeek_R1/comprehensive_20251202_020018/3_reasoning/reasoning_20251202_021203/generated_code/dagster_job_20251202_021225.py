<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is linear with four steps: querying sales data, transforming to CSV, generating a PDF chart, and emailing the report. Each step depends on the previous one, so the dependencies are straightforward.

First, I'll create each step as a Dagster op using @op. The names should match the steps: query_sales_data, transform_to_csv, generate_pdf_chart, email_sales_report. Each op will take the previous op's output as input, except the first one which might not need any input.

For query_sales_data, I'll need to connect to PostgreSQL. The user mentioned using a resource called postgres_default. So I'll define a PostgreSQL resource in the job. The op could use a context parameter to access resources. The query should fetch daily sales data aggregated by product for the execution date. Since the schedule is daily, the execution date can be obtained from the run config. I'll add a configuration schema for the date, maybe using a string that the op can parse.

Next, transform_to_csv takes the query results and uses pandas to create a CSV. I'll need to import pandas and write the DataFrame to /tmp/sales_report.csv. The op should return the path to the CSV so the next op can use it.

generate_pdf_chart will take the CSV path, read the data, create a bar chart with matplotlib, and save it as a PDF. Again, the path is hardcoded to /tmp/sales_chart.pdf. This op returns the PDF path for the email step.

email_sales_report needs both CSV and PDF paths. It uses smtplib to send an email with attachments. The SMTP settings (server, port, credentials) should come from a resource. I'll create an smtp_resource that provides the connection details. The op will handle attaching the files and sending the email.

Now, setting up the job with @job. The resources (postgres and smtp) need to be configured. I'll add them to the job's config. The retry policy is 2 retries with 5-minute delays, so I'll include that in the job decorator.

The schedule is daily starting January 1, 2024, with catchup disabled. But the user didn't ask for the schedule code, just the job module. So maybe the schedule is out of scope here. The main code should focus on the job and ops.

For the launch pattern, include the if __name__ == '__main__' block that runs the job in-process. Also, provide example configurations as comments for resources and run config.

Testing each op's functionality: Ensure that the PostgreSQL query uses the execution date from the config. The transform and generate steps write to /tmp, which should be accessible. The email op uses the SMTP resource; maybe stub the sendmail part for testing.

Potential issues: Hardcoding file paths might not be best practice, but the user specified /tmp. Resources need to be properly configured with examples. Also, handling dates correctly in the query.

Putting it all together: Define each op with necessary inputs and outputs, set up the job with dependencies, include resources, and add the execution block. Make sure to follow PEP8, use docstrings, and keep the code executable.
</think>

from dagster import op, job, RetryPolicy, ResourceParam
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import psycopg2

# --- Resources ---
# Example PostgreSQL config:
# resources:
#   postgres:
#     config:
#       host: localhost
#       dbname: sales
#       user: postgres
#       password: postgres

# Example SMTP config:
# resources:
#   smtp:
#     config:
#       server: smtp.company.com
#       port: 587
#       username: reports@company.com
#       password: securepass

@op(required_resource_keys={"postgres"})
def query_sales_data(context):
    """Query daily sales data from PostgreSQL database."""
    execution_date = context.op_config["execution_date"]
    with context.resources.postgres.cursor() as cursor:
        cursor.execute("""
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product
        """, (execution_date,))
        results = cursor.fetchall()
    return {"date": execution_date, "data": results}

@op
def transform_to_csv(sales_data):
    """Transform sales data to CSV format."""
    df = pd.DataFrame(sales_data["data"], columns=["product", "total_sales"])
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    return {"csv_path": csv_path, "date": sales_data["date"]}

@op
def generate_pdf_chart(context, csv_data):
    """Generate PDF chart from sales data."""
    df = pd.read_csv(csv_data["csv_path"])
    plt.figure(figsize=(10,6))
    plt.bar(df["product"], df["total_sales"])
    plt.title(f"Sales Report - {csv_data['date']}")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    return {"pdf_path": pdf_path, "csv_path": csv_data["csv_path"]}

@op(required_resource_keys={"smtp"})
def email_sales_report(context, file_paths):
    """Send email with sales report attachments."""
    msg = MIMEMultipart()
    msg["Subject"] = f"Daily Sales Report - {datetime.today().strftime('%Y-%m-%d')}"
    msg["From"] = context.resources.smtp["username"]
    msg["To"] = "management@company.com"

    # Attach CSV
    with open(file_paths["csv_path"], "rb") as f:
        part = MIMEApplication(f.read(), Name="sales_report.csv")
        part["Content-Disposition"] = 'attachment; filename="sales_report.csv"'
        msg.attach(part)

    # Attach PDF
    with open(file_paths["pdf_path"], "rb") as f:
        part = MIMEApplication(f.read(), Name="sales_chart.pdf")
        part["Content-Disposition"] = 'attachment; filename="sales_chart.pdf"'
        msg.attach(part)

    # Send email
    with smtplib.SMTP(
        context.resources.smtp["server"],
        context.resources.smtp["port"]
    ) as server:
        server.starttls()
        server.login(
            context.resources.smtp["username"],
            context.resources.smtp["password"]
        )
        server.send_message(msg)

@job(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    resource_defs={
        "postgres": ResourceParam(psycopg2.connect),
        "smtp": ResourceParam(dict)
    }
)
def daily_sales_report_job():
    csv_data = transform_to_csv(query_sales_data())
    file_paths = generate_pdf_chart(csv_data)
    email_sales_report(file_paths)

if __name__ == "__main__":
    # Example run config:
    # run_config = {
    #     "ops": {
    #         "query_sales_data": {
    #             "config": {"execution_date": "2024-01-01"}
    #         }
    #     },
    #     "resources": {
    #         "postgres": {
    #             "config": {
    #                 "host": "localhost",
    #                 "dbname": "sales",
    #                 "user": "postgres",
    #                 "password": "postgres"
    #             }
    #         },
    #         "smtp": {
    #             "config": {
    #                 "server": "smtp.company.com",
    #                 "port": 587,
    #                 "username": "reports@company.com",
    #                 "password": "securepass"
    #             }
    #         }
    #     }
    # }
    result = daily_sales_report_job.execute_in_process()