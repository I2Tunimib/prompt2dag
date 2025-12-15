<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements.

First, the pipeline has four steps that run sequentially: query_sales_data, transform_to_csv, generate_pdf_chart, and email_sales_report. Each task depends on the previous one. Since there's no parallelism, I don't need to use .submit(), just call them normally in order.

The user mentioned using @task and @flow decorators. So each step will be a task, and the main flow will orchestrate them. I should import flow and task from prefect.

For the schedule, the problem says to include a comment about deployment/schedule configuration. So I'll add a note there but not implement the schedule in code since that's usually handled via deployment.

Retries and failure handling: Prefect has built-in retries. I can set retries=2 and retry_delay_seconds=300 (5 minutes) in the @task decorators. Notifications on failure might require more setup, but the question doesn't specify code for that, so maybe just a comment.

Dependencies: PostgreSQL connection, pandas, matplotlib. The code should import these, but the user said to assume required packages are installed. So include the necessary imports like psycopg2, pandas, matplotlib, smtplib, etc.

Each task function needs to have parameters for the previous step's output if needed. For example, query_sales_data returns data that transform_to_csv uses. Wait, but looking at the steps, query_sales_data executes a query and returns data, which transform_to_csv uses to create a CSV. Then generate_pdf_chart might also use the same data to create the chart. Then email_sales_report doesn't need the data but the file paths.

Wait, the description says data flows from PostgreSQL to CSV, then PDF, then email. So maybe the CSV and PDF are generated from the same data. So query_sales_data returns data, which is passed to both transform_to_csv and generate_pdf_chart. But according to the execution order, it's sequential: query -> transform_to_csv -> generate_pdf -> email. Wait, no, the steps are:

1. query_sales_data
2. transform_to_csv (uses query result)
3. generate_pdf_chart (uses query result?)
4. email_sales_report (uses CSV and PDF files)

Wait, the description says "transforms the query results into CSV" and "generates PDF chart from query results". So maybe both transform_to_csv and generate_pdf_chart depend on the output of query_sales_data. But according to the orchestration order, the steps are executed in sequence: query, then transform, then generate, then email. That suggests that transform_to_csv comes before generate_pdf_chart, but both might take the same data. Or perhaps the generate_pdf step uses the CSV? No, the description says "transforms the query results into CSV" and "creates PDF bar chart visualizing sales by product". So both tasks take the data from query_sales_data.

But according to the execution order, it's a strict linear sequence. So maybe the data flows from query to transform_to_csv, which saves the CSV, then generate_pdf_chart reads the CSV? Or does generate_pdf use the original data? The description says "query results" for both steps, so perhaps both tasks take the data from query_sales_data. But in the execution order, transform_to_csv comes before generate_pdf_chart. So maybe the flow is:

query_sales_data returns data → transform_to_csv(data) → generate_pdf_chart(data) → email_sales_report()

But in that case, transform and generate could be parallel, but the problem says the pipeline is sequential with no parallelization. So the order is query, then transform, then generate, then email. So each task depends on the previous one's completion, but data is passed through.

Wait, but how does generate_pdf_chart get the data? If transform_to_csv is called after query, and generate_pdf is called after transform, then maybe generate_pdf needs to take the data from query as well. But in a linear sequence, the data from query would be passed to transform, which saves to CSV and perhaps returns the file path. Then generate_pdf would need the data again. Hmm, maybe the data is passed along each step. Alternatively, maybe each task saves the data to a file and the next task reads it. But the problem says data flows from PostgreSQL to CSV to PDF to email. Wait, no: the data flow is PostgreSQL → CSV file → PDF chart → Email attachments. Wait, that suggests that the CSV is generated from PostgreSQL, then the PDF is generated from the CSV? Or is the PDF generated from the same query data as the CSV?

The description says "transforms the query results into CSV" and "creates PDF bar chart visualizing sales by product". So both tasks use the query results. Therefore, the data from query_sales_data is passed to both transform_to_csv and generate_pdf_chart. But according to the execution order, transform_to_csv comes before generate_pdf_chart. So perhaps the flow is:

data = query_sales_data()

csv_path = transform_to_csv(data)

pdf_path = generate_pdf_chart(data)

email_sales_report(csv_path, pdf_path)

But that would allow transform and generate to run in parallel, but the problem says the pipeline is sequential. So maybe the tasks are called in sequence, but both take the data from the first task. So the flow would be:

data = query_sales_data()

csv_path = transform_to_csv(data)

pdf_path = generate_pdf_chart(data)

email_sales_report(csv_path, pdf_path)

But in this case, transform and generate are called sequentially, but they don't depend on each other. However, the problem states that each task depends on the previous one's completion. So perhaps the data is passed through each task, even if not needed. Wait, maybe the steps are:

query → transform (using data) → generate (using data) → email. But how to pass data through each step? Or maybe each task returns something that the next task needs. For example, query returns data, transform returns csv_path, generate returns pdf_path, email takes both paths.

But according to the problem description, the CSV and PDF are both generated from the query data. So transform_to_csv takes data and saves CSV, returns the path. generate_pdf_chart takes data and saves PDF, returns the path. Then email takes both paths.

But in that case, the flow would be:

data = query_sales_data()

csv_path = transform_to_csv(data)

pdf_path = generate_pdf_chart(data)

email_sales_report(csv_path, pdf_path)

But according to the execution order, each task depends on the previous. So transform depends on query, generate depends on transform, email depends on generate. But in this code, generate doesn't depend on transform. So that's not sequential. So perhaps the correct order is:

data = query_sales_data()

csv_path = transform_to_csv(data)

pdf_path = generate_pdf_chart(csv_path)  # But why would PDF depend on CSV?

No, the problem says generate_pdf uses the query results, not the CSV. So maybe the flow should be:

data = query_sales_data()

csv_path = transform_to_csv(data)

pdf_path = generate_pdf_chart(data)

email_sales_report(csv_path, pdf_path)

But in this case, generate_pdf doesn't depend on transform_to_csv. So according to Prefect's task dependencies, since generate_pdf is called after transform_to_csv, it would wait for transform_to_csv to complete. Because in Prefect, tasks are executed in the order they are called within the flow. So even if generate_pdf doesn't take the output of transform_to_csv, since it's called after, it will run after transform_to_csv finishes.

So the flow code would be:

@flow
def sales_reporting_flow():
    data = query_sales_data()
    csv_path = transform_to_csv(data)
    pdf_path = generate_pdf_chart(data)
    email_sales_report(csv_path, pdf_path)

But according to the problem's orchestration order, each task depends on the previous. So even if generate_pdf doesn't need the CSV, it must wait for transform_to_csv to complete. By calling generate_pdf after transform_to_csv, Prefect ensures that order.

So that's acceptable. Now, let's outline each task.

query_sales_data: connects to PostgreSQL, runs a query for daily sales aggregated by product. The execution date is needed. Wait, the pipeline is scheduled daily, so the execution date is the current date. But how to pass that? Maybe the flow takes a date parameter, defaulting to today.

But the problem says the pipeline is scheduled daily with start date Jan 1, 2024, catchup disabled. So perhaps the flow uses the current date each time it runs. So in the query, the date is the execution date. So the query might be something like "SELECT ... WHERE date = %s", using the current date.

But how to handle that in the code? The flow could accept a date parameter, which defaults to datetime.today(). But for scheduling, when deployed, Prefect would pass the appropriate date. For local execution, it would use the default.

So the flow could be defined as:

@flow
def sales_reporting_flow(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.today()
    # rest of the code

But the problem doesn't mention parameters, so perhaps the query uses the current date. Alternatively, the flow could get the current date within the query task.

But for the code to be executable, perhaps the query_sales_data task uses the current date. Let's proceed with that.

In the query_sales_data task, connect to PostgreSQL using a prefect block (postgres_default connection). But the problem says to use the postgres_default connection. Prefect 2.x uses blocks for configurations. So the code should use a PostgreSQL block named 'postgres_default'.

But the user's code needs to be executable. So in the task, we can fetch the postgres connection from the block. However, the code would need to have that block registered. But since the user said to assume required packages are installed, perhaps the code uses a connection string directly, or uses the block.

Alternatively, the code can use psycopg2 to connect, using a connection string from a Prefect secret or block. But for simplicity, perhaps the code uses psycopg2.connect with parameters, but in reality, the user would need to have the block configured.

But the problem says to make the code executable, so perhaps we can use placeholders for the connection details, but the user expects that. However, the user said to avoid placeholders like <TODO>. So perhaps use sensible defaults with comments indicating where to configure.

But the problem says to avoid placeholders and use minimal safe defaults. So perhaps in the query_sales_data task, we connect to PostgreSQL using psycopg2, with some default parameters, but in reality, the user would need to adjust these.

But perhaps the code uses Prefect's SqlAlchemyConnector block. Alternatively, use psycopg2 directly.

Alternatively, use a context manager from prefect_sqlalchemy import SqlAlchemyConnector.

But the problem says to use minimal dependencies beyond what's mentioned. The user mentioned PostgreSQL and SMTP as integrations, and pandas, matplotlib as Python libraries.

So perhaps the code uses psycopg2 to connect. Let's proceed.

In query_sales_data task:

import psycopg2
from psycopg2 import sql

@task(retries=2, retry_delay_seconds=300)
def query_sales_data():
    # Connect to PostgreSQL using postgres_default connection details
    conn = psycopg2.connect(
        dbname="sales",
        user="user",
        password="pass",
        host="localhost",
        port=5432
    )
    # Execute query for daily sales aggregated by product
    query = sql.SQL("""
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product
    """)
    current_date = datetime.today().date()
    with conn.cursor() as cur:
        cur.execute(query, (current_date,))
        results = cur.fetchall()
    conn.close()
    return results

But using hard-coded credentials is bad practice. However, the problem says to make the code executable, so perhaps this is acceptable for the sake of example, with a comment indicating that in production, a Prefect block should be used.

Alternatively, use a Prefect Secret for the connection details. But that complicates the code. Since the user didn't specify, perhaps proceed with placeholders but replace with a comment.

Wait, the user said to avoid placeholders. So perhaps use sensible defaults and add a comment that in production, a Prefect block should be used.

So:

@task(retries=2, retry_delay_seconds=300)
def query_sales_data():
    """Execute daily sales query from PostgreSQL database."""
    # In production, use Prefect's PostgresConnector block for credentials
    conn = psycopg2.connect(
        dbname="sales",
        user="user",
        password="pass",
        host="localhost",
        port=5432
    )
    # ... rest of the code

But the user may want to use a block. Alternatively, use SqlAlchemyConnector from prefect_sqlalchemy.

But perhaps the code should use the block. Let's see.

First, install prefect-sqlalchemy:

pip install prefect-sqlalchemy

Then, in the code:

from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=2, retry_delay_seconds=300)
def query_sales_data():
    """Execute daily sales query from PostgreSQL database."""
    with SqlAlchemyConnector.load("postgres_default") as connector:
        with connector.get_connection(begin=False) as connection:
            query = """
                SELECT product, SUM(amount) as total_sales
                FROM sales
                WHERE sale_date = %s
                GROUP BY product
            """
            current_date = datetime.today().date()
            results = pd.read_sql(query, connection, params=(current_date,))
    return results

But this uses pandas to read the SQL query, which is convenient. Then the task returns a DataFrame. That would make sense for the next task, transform_to_csv, which can use pandas to write to CSV.

But the problem says that transform_to_csv transforms the query results into a CSV. So if query_sales_data returns a DataFrame, then transform_to_csv can just call df.to_csv.

So perhaps:

@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame):
    """Transform sales data to CSV file."""
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path

Similarly, generate_pdf_chart takes the DataFrame and creates a PDF.

But wait, the generate_pdf_chart task is supposed to create a PDF bar chart. So using matplotlib:

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame):
    """Generate PDF bar chart from sales data."""
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 6))
    plt.bar(data['product'], data['total_sales'])
    plt.title('Daily Sales by Product')
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

Then, email_sales_report takes the CSV and PDF paths and sends an email.

For sending email via SMTP, the code can use smtplib. But again, credentials are needed. So:

@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str):
    """Send email with sales report attachments."""
    sender_email = "reports@company.com"
    receiver_email = "management@company.com"
    smtp_server = "smtp.company.com"
    smtp_port = 587
    smtp_user = "user"
    smtp_password = "pass"

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = "Daily Sales Report"

    body = "Attached are the daily sales report and chart."
    message.attach(MIMEText(body, "plain"))

    for file_path in [csv_path, pdf_path]:
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(file_path)}",
        )
        message.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, receiver_email, message.as_string())

Putting it all together:

Imports needed:

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

Wait, but the problem says the data source is PostgreSQL, so using SqlAlchemyConnector makes sense. But if the user doesn't have prefect-sqlalchemy installed, the code won't run. However, the problem says to assume required packages are installed. So including the import is okay.

But perhaps the code uses psycopg2 instead. Let's adjust.

Alternatively, use the SqlAlchemyConnector as it's a Prefect integration, which is more aligned with best practices.

But the code needs to be executable. So if the user has prefect-sqlalchemy installed, it's fine.

So the query_sales_data task uses SqlAlchemyConnector to connect to PostgreSQL.

Putting all together:

The complete code would look like:

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=2, retry_delay_seconds=300)
def query_sales_data() -> pd.DataFrame:
    """Execute PostgreSQL query for daily sales data."""
    with SqlAlchemyConnector.load("postgres_default") as connector:
        current_date = datetime.today().date()
        query = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE sale_date = %s
            GROUP BY product
        """
        data = pd.read_sql(query, connector.get_connection(), params=(current_date,))
    return data

@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    """Convert DataFrame to CSV file."""
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame) -> str:
    """Generate PDF chart from sales data."""
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 6))
    plt.bar(data["product"], data["total_sales"])
    plt.title("Daily Sales by Product")
    plt.xlabel("Product")
    plt.ylabel("Total Sales ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str) -> None:
    """Send email with report attachments."""
    sender = "reports@company.com"
    receiver = "management@company.com"
    password = "email_password"  # Prefect Secret recommended in production

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = "Daily Sales Report"

    body = "Please find attached the daily sales report and chart."
    msg.attach(MIMEText(body, "plain"))

    for file in [csv_path, pdf_path]:
        with open(file, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(file)}",
            )
            msg.attach(part)

    with smtplib.SMTP("smtp.company.com", 587) as server:
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())

@flow
def sales_reporting_flow():
    """Daily sales reporting pipeline."""
    sales_data = query_sales_data()
    csv_file = transform_to_csv(sales_data)
    pdf_file = generate_pdf_chart(sales_data)
    email_sales_report(csv_file, pdf_file)

if __name__ == "__main__":
    sales_reporting_flow()

But wait, the email task uses 'password' which is hard-coded. The problem says to use minimal safe defaults, so perhaps replace with a comment suggesting using a Prefect Secret. Also, the SMTP server details are hard-coded, which should be handled via blocks or environment variables in production.

But according to the problem's constraints, the code must be executable. So including the hard-coded values is necessary, but with comments indicating that they should be configured properly.

Also, the flow uses the SqlAlchemyConnector block named 'postgres_default', which needs to be configured in Prefect. But the code will run if that block exists.

The schedule is mentioned, so include a comment indicating deployment/schedule configuration. The problem says to include a brief comment. So in the flow's docstring or a comment:

# To deploy with schedule:
# prefect deploy --name sales-report --interval 1440 --start-date 2024-01-01

But the user said to return only the code. So perhaps add a comment near the flow function.

But the problem says "optional" for schedule comments. So adding a comment indicating deployment configuration.

Putting it all together, the code should have:

- Imports for necessary libraries.
- Tasks decorated with @task and retry settings.
- Flow function that calls tasks in order.
- if __name__ == '__main__' guard.

Check for PEP 8 compliance: line lengths, whitespace, etc.

The code should be as described.
</think>

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=2, retry_delay_seconds=300)
def query_sales_data() -> pd.DataFrame:
    """Execute PostgreSQL query for daily sales data."""
    with SqlAlchemyConnector.load("postgres_default") as connector:
        current_date = datetime.today().date()
        query = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE sale_date = %s
            GROUP BY product
        """
        data = pd.read_sql(query, connector.get_connection(), params=(current_date,))
    return data

@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    """Convert DataFrame to CSV file."""
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame) -> str:
    """Generate PDF chart from sales data."""
    pdf_path = "/tmp/sales_chart.pdf"
    plt.figure(figsize=(10, 6))
    plt.bar(data["product"], data["total_sales"])
    plt.title("Daily Sales by Product")
    plt.xlabel("Product")
    plt.ylabel("Total Sales ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path

@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str) -> None:
    """Send email with report attachments."""
    sender = "reports@company.com"
    receiver = "management@company.com"
    password = "email_password"  # Prefect Secret recommended in production

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = "Daily Sales Report"

    body = "Please find attached the daily sales report and chart."
    msg.attach(MIMEText(body, "plain"))

    for file in [csv_path, pdf_path]:
        with open(file, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(file)}",
            )
            msg.attach(part)

    with smtplib.SMTP("smtp.company.com", 587) as server:
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())

@flow
def sales_reporting_flow():
    """Daily sales reporting pipeline. Schedule: @daily with start 2024-01-01"""
    sales_data = query_sales_data()
    csv_file = transform_to_csv(sales_data)
    pdf_file = generate_pdf_chart(sales_data)
    email_sales_report(csv_file, pdf_file)

if __name__ == "__main__":
    sales_reporting_flow()