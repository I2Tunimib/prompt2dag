<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let's start by understanding the requirements. The pipeline is a linear daily sales report that extracts data from PostgreSQL, transforms it into CSV and PDF, then emails the report. All steps are sequential with no parallelism.

First, I'll create a Prefect flow and tasks. Each step in the pipeline becomes a task. The flow will orchestrate these tasks in order. Since the steps are sequential, each task depends on the previous one, so I'll call them normally without .submit().

For the tasks:
1. query_sales_data: Uses PostgreSQL connection. I'll need to use a Prefect block for the PostgreSQL connection. The task executes a SQL query for the current date. Maybe use parameters for the execution date, but since it's scheduled daily, perhaps the flow can take a date parameter with a default.

2. transform_to_csv: Takes the data and saves to CSV. Use pandas to handle the DataFrame and write to CSV.

3. generate_pdf_chart: Uses matplotlib to create a bar chart and save as PDF. Need to make sure the figure is properly closed after saving.

4. email_sales_report: Sends an email with attachments. Use smtplib or a Prefect email block. Since the email system is mentioned as SMTP, maybe use the EmailMessage block or a task that handles SMTP.

Scheduling and deployment: The user mentioned a daily schedule starting Jan 1, 2024, catchup disabled, retries, and email notifications. But the code should include a comment about deployment configuration. So in the code, I'll add a comment indicating that the schedule is set via deployment, maybe using a cron schedule.

Retries and failure handling: The tasks should have retries. I'll add @task with retries=2 and retry_delay_seconds=300. For email notifications on failure, that's typically handled via Prefect notifications (like automation), but since the code needs to be self-contained, maybe add a note as a comment.

Dependencies: Each task is called in order, so the output of one is passed as input to the next. Wait, but in the description, data flows from PostgreSQL to CSV to PDF to email. So query_sales_data returns data, transform_to_csv takes that data and writes CSV, generate_pdf_chart might also take the data to generate the chart, and email_sales_report doesn't need data but needs the file paths.

Wait, the steps are:

- query_sales_data returns the data (DataFrame?)
- transform_to_csv takes the data, writes to CSV, returns the CSV path?
- generate_pdf_chart takes the data, creates PDF, returns PDF path?
- email_sales_report takes both paths and sends email.

So each task's output is passed to the next. But according to the pipeline description, each task depends on the previous's completion. So maybe each task is called in sequence, with the output passed as arguments.

But in the problem statement, the steps are strictly sequential, each depending on the previous. So in code:

@flow
def sales_reporting_flow():
    data = query_sales_data()
    csv_path = transform_to_csv(data)
    pdf_path = generate_pdf_chart(data)
    email_sales_report(csv_path, pdf_path)

But wait, the email task needs both CSV and PDF paths. So the flow would run query, then transform (which depends on query), then generate_pdf (depends on query?), or does generate_pdf depend on transform? According to the description, it's a linear sequence: query -> transform -> generate_pdf -> email. So each step must wait for the previous. So the code should be:

data = query_sales_data()
csv_path = transform_to_csv(data)
pdf_path = generate_pdf_chart(csv_path)  # Or does generate_pdf use the data?

Wait, the description says "transforms it into CSV and PDF chart formats". So maybe both transform_to_csv and generate_pdf_chart depend on the data from query_sales_data. But according to the orchestration order, the steps are sequential: query, then transform, then generate_pdf, then email. So perhaps transform_to_csv runs first, then generate_pdf_chart, each using the data from query.

Wait, the pipeline steps are:

1. query_sales_data
2. transform_to_csv
3. generate_pdf_chart
4. email_sales_report

So the order is query -> transform -> generate -> email. So each task is called after the previous completes. So data flows from query to transform, which produces CSV. Then generate_pdf uses the data (from query) to create the chart. Then email takes both CSV and PDF paths.

Wait, but how does generate_pdf get the data? If transform_to_csv takes the data and writes CSV, then generate_pdf_chart would also need the data. So perhaps the flow is:

data = query_sales_data()
csv_path = transform_to_csv(data)
pdf_path = generate_pdf_chart(data)
email_sales_report(csv_path, pdf_path)

In this case, transform and generate are both dependent on data, but run sequentially. So first transform runs, then generate runs, both using the same data. Then email takes both paths.

That makes sense. So the tasks are sequential but both transform and generate depend on the data from query.

Now, implementing each task:

For query_sales_data, use psycopg2 to connect to PostgreSQL. Use Prefect's PostgresConnector block. The task should execute a SQL query for the execution date. The flow might take a date parameter, defaulting to today.

But the schedule is daily, so the execution date is probably passed as a parameter. So the flow function should have a date parameter, maybe with a default of None, and if None, use datetime.today().

But Prefect flows can take parameters. So in the flow definition:

@flow
def sales_reporting_flow(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.utcnow()
    # rest of the code

Then, the SQL query would be for the run_date's date.

The task query_sales_data would take run_date as an argument.

But how to handle the PostgreSQL connection? Using a Prefect block. So the code should use a PostgresConnector block named 'postgres_default' as per the description.

In the task, we can load the PostgresConnector block:

from prefect_sqlalchemy import SqlAlchemyConnector

But wait, Prefect has a PostgresConnector in prefect-sqlalchemy? Or maybe use the SqlAlchemyConnector. Alternatively, use psycopg2 directly with a PostgresCredentials block.

Alternatively, use the prefect-postgres package's PostgresConnector.

But perhaps the user expects using the SqlAlchemyConnector from prefect-sqlalchemy. Let's assume that.

So, in the task:

@task(retries=2, retry_delay_seconds=300)
def query_sales_data(run_date: datetime) -> pd.DataFrame:
    # Connect to PostgreSQL using the 'postgres_default' connection block
    with SqlAlchemyConnector.load("postgres_default") as connector:
        # Execute query for run_date's date
        query = "SELECT product, SUM(amount) as total_sales FROM sales WHERE date = %s GROUP BY product;"
        result = connector.execute(query, parameters=(run_date.date(),))
        df = pd.DataFrame(result, columns=["product", "total_sales"])
        return df

Wait, SqlAlchemyConnector's execute method returns a list of rows? Or maybe we can use pandas' read_sql.

Alternatively, using the connection as an engine:

engine = connector.get_engine()
with engine.connect() as connection:
    df = pd.read_sql_query(query, connection, params=(run_date.date(),))

But perhaps using the SqlAlchemyConnector correctly.

Alternatively, maybe the code uses psycopg2 directly, but using a PostgresCredentials block. Let's check.

The user mentioned the PostgreSQL connection is named 'postgres_default'. So perhaps using the PostgresCredentials block from prefect-postgres.

But if the user hasn't installed prefect-postgres, maybe use the SqlAlchemyConnector.

Alternatively, use a generic approach with a block. However, the code needs to be executable, so assuming that the necessary blocks are set up.

But for the code to be complete, perhaps the tasks should include the connection handling.

Alternatively, the code can have placeholders for the connection, but the user said to avoid placeholders. So perhaps use SqlAlchemyConnector and assume that the block is registered.

So, the code would import SqlAlchemyConnector from prefect_sqlalchemy, and use it.

Next, transform_to_csv takes the DataFrame and writes to CSV. Using pandas:

@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path

Then generate_pdf_chart creates a PDF bar chart. Using matplotlib:

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame) -> str:
    pdf_path = "/tmp/sales_chart.pdf"
    fig, ax = plt.subplots()
    data.plot.bar(x='product', y='total_sales', ax=ax)
    plt.title(f"Sales by Product - {datetime.today().date()}")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    fig.savefig(pdf_path, format='pdf')
    plt.close(fig)
    return pdf_path

Then email_sales_report sends the email with attachments. Using smtplib and email libraries:

@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str):
    # Using SMTP service
    # Assuming SMTP settings are configured in Prefect blocks or environment variables
    # For example, using an EmailMessage block
    from prefect_email import EmailServerCredentials, EmailMessage

    email_credentials = EmailServerCredentials.load("smtp-credentials")
    
    subject = "Daily Sales Report"
    body = "Attached are the daily sales report CSV and PDF chart."
    recipient = "management@company.com"
    
    message = EmailMessage(
        subject=subject,
        sender=email_credentials.username,
        to=recipient,
        body=body,
        attachments=[csv_path, pdf_path],
    )
    email_credentials.send_message(message)

But the prefect_email package is needed. Alternatively, use smtplib directly, but that's more code. However, using Prefect's EmailServerCredentials block would be better.

But the user might not have prefect_email installed. However, the problem statement mentions integrations include SMTP service, so perhaps using the EmailServerCredentials from prefect_email.

Assuming that the user has prefect_email installed and an SMTP block named 'smtp-credentials' exists. But the code should be executable, so the task should handle that.

Alternatively, the code can use placeholders, but the user said to avoid placeholders. So perhaps include the code with EmailServerCredentials.

So, the email task uses prefect_email's EmailServerCredentials.

Putting it all together:

Imports needed:

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailServerCredentials, EmailMessage

Then, the flow function with parameters.

Now, scheduling: The user wants a daily schedule starting Jan 1, 2024, catchup disabled. But the code should include a comment indicating deployment/schedule configuration. So in the module, after the flow definition, add a comment like:

# To deploy with a daily schedule:
# prefect deploy --name daily-sales-report --interval 86400 --start "2024-01-01" --no-catchup

But Prefect 2.x uses deployments with cron or interval schedules. Alternatively, in the code, add a comment indicating how to set up the schedule via deployment.

But the code itself can't include the schedule; it's part of the deployment YAML. So the comment can mention that.

Now, the retry policy is 2 retries with 5-minute delays. Each task has retries=2 and retry_delay_seconds=300 (5 minutes).

Failure handling: Email notifications on task failures. This is typically handled via Prefect's notifications, which are part of the deployment or automations. Since the code can't set that up directly, perhaps add a comment.

Putting it all together:

The code starts with imports, then defines the tasks with retries, then the flow that calls them in order. The flow takes a run_date parameter. The email task uses EmailServerCredentials.

Testing for if __name__ == '__main__': to run the flow.

Now, possible issues:

- The generate_pdf_chart task uses datetime.today().date(), but should it use the run_date? Because the report is for the execution date. So in the generate_pdf_chart, the title should use run_date.date().

But the run_date is passed to the flow. So the flow's run_date is passed to the tasks as needed.

Wait, in the flow:

def sales_reporting_flow(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.utcnow()
    data = query_sales_data(run_date)
    csv_path = transform_to_csv(data)
    pdf_path = generate_pdf_chart(data, run_date)
    email_sales_report(csv_path, pdf_path)

Then, in generate_pdf_chart:

@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame, run_date: datetime) -> str:
    pdf_path = "/tmp/sales_chart.pdf"
    fig, ax = plt.subplots()
    data.plot.bar(x='product', y='total_sales', ax=ax)
    plt.title(f"Sales by Product - {run_date.date()}")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    fig.savefig(pdf_path, format='pdf')
    plt.close(fig)
    return pdf_path

But then, the flow passes run_date to generate_pdf_chart. So the task would need to accept run_date as an argument.

Alternatively, the run_date is part of the data, but the data is a DataFrame that includes the date. But according to the pipeline steps, the query is for the execution date, so the data is for that date. Therefore, the title can use the run_date passed to the flow.

So adjusting generate_pdf_chart to take run_date as a parameter.

But wait, the flow's run_date is passed to query_sales_data, which uses it in the SQL query. Then, the data is for that date. So the PDF title can use that date. Therefore, the generate_pdf_chart task should receive the run_date as an argument.

But in the current flow code, generate_pdf_chart is called with data, which is the output of query_sales_data. So to pass run_date to generate_pdf_chart, the flow would need to pass it as an argument.

So modifying the flow:

data = query_sales_data(run_date)
csv_path = transform_to_csv(data)
pdf_path = generate_pdf_chart(data, run_date)
email_sales_report(csv_path, pdf_path)

Thus, the generate_pdf_chart task takes data and run_date.

Alternatively, since the data is derived from the run_date, perhaps the run_date is not needed, and the title can be generated from the data's date. But the data may not include the date in the DataFrame, as the query aggregates by product for the execution date. So the date is not in the DataFrame, but the run_date is known.

Therefore, passing run_date to generate_pdf_chart makes sense.

So, updating the generate_pdf_chart task to accept run_date as an argument.

Now, putting all together:

The code would look like this:

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailServerCredentials, EmailMessage


@task(retries=2, retry_delay_seconds=300)
def query_sales_data(run_date: datetime) -> pd.DataFrame:
    """Execute PostgreSQL query for daily sales data."""
    with SqlAlchemyConnector.load("postgres_default") as connector:
        query = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product;
        """
        result = connector.execute(query, parameters=(run_date.date(),))
        df = pd.DataFrame(result, columns=["product", "total_sales"])
        return df


@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    """Transform sales data to CSV file."""
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path


@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame, run_date: datetime) -> str:
    """Generate PDF chart from sales data."""
    pdf_path = "/tmp/sales_chart.pdf"
    fig, ax = plt.subplots(figsize=(10, 6))
    data.plot.bar(x="product", y="total_sales", ax=ax)
    plt.title(f"Sales by Product - {run_date.date()}")
    plt.xlabel("Product")
    plt.ylabel("Total Sales ($)")
    plt.tight_layout()
    fig.savefig(pdf_path, format="pdf")
    plt.close(fig)
    return pdf_path


@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str):
    """Send email with sales report attachments."""
    email_credentials = EmailServerCredentials.load("smtp-credentials")
    message = EmailMessage(
        subject="Daily Sales Report",
        sender=email_credentials.username,
        to="management@company.com",
        body="Attached are the daily sales report CSV and PDF chart.",
        attachments=[csv_path, pdf_path],
    )
    email_credentials.send_message(message)


@flow
def sales_reporting_flow(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.utcnow()
    sales_data = query_sales_data(run_date)
    csv_file = transform_to_csv(sales_data)
    pdf_file = generate_pdf_chart(sales_data, run_date)
    email_sales_report(csv_file, pdf_file)


if __name__ == "__main__":
    # Example: For local execution with specific date
    # sales_reporting_flow(datetime(2024, 1, 1))
    sales_reporting_flow()

# Deployment configuration note:
# Use prefect deploy with cron schedule: '@daily' starting 2024-01-01
</think>
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailServerCredentials, EmailMessage


@task(retries=2, retry_delay_seconds=300)
def query_sales_data(run_date: datetime) -> pd.DataFrame:
    """Execute PostgreSQL query for daily sales data."""
    with SqlAlchemyConnector.load("postgres_default") as connector:
        query = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product;
        """
        result = connector.execute(query, parameters=(run_date.date(),))
        df = pd.DataFrame(result, columns=["product", "total_sales"])
        return df


@task(retries=2, retry_delay_seconds=300)
def transform_to_csv(data: pd.DataFrame) -> str:
    """Transform sales data to CSV file."""
    csv_path = "/tmp/sales_report.csv"
    data.to_csv(csv_path, index=False)
    return csv_path


@task(retries=2, retry_delay_seconds=300)
def generate_pdf_chart(data: pd.DataFrame, run_date: datetime) -> str:
    """Generate PDF chart from sales data."""
    pdf_path = "/tmp/sales_chart.pdf"
    fig, ax = plt.subplots(figsize=(10, 6))
    data.plot.bar(x="product", y="total_sales", ax=ax)
    plt.title(f"Sales by Product - {run_date.date()}")
    plt.xlabel("Product")
    plt.ylabel("Total Sales ($)")
    plt.tight_layout()
    fig.savefig(pdf_path, format="pdf")
    plt.close(fig)
    return pdf_path


@task(retries=2, retry_delay_seconds=300)
def email_sales_report(csv_path: str, pdf_path: str):
    """Send email with sales report attachments."""
    email_credentials = EmailServerCredentials.load("smtp-credentials")
    message = EmailMessage(
        subject="Daily Sales Report",
        sender=email_credentials.username,
        to="management@company.com",
        body="Attached are the daily sales report CSV and PDF chart.",
        attachments=[csv_path, pdf_path],
    )
    email_credentials.send_message(message)


@flow
def sales_reporting_flow(run_date: datetime = None):
    if run_date is None:
        run_date = datetime.utcnow()
    sales_data = query_sales_data(run_date)
    csv_file = transform_to_csv(sales_data)
    pdf_file = generate_pdf_chart(sales_data, run_date)
    email_sales_report(csv_file, pdf_file)


if __name__ == "__main__":
    sales_reporting_flow()

# To deploy with daily schedule:
# prefect deployment build --name daily-sales --schedule "0 0 * * *" --apply