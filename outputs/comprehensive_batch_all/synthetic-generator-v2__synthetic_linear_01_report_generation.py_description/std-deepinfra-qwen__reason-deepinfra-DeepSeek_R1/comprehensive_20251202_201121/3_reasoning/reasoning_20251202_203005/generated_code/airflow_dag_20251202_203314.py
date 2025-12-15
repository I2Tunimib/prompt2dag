<think>
Okay, I need to create an Apache Airflow DAG for this daily sales reporting pipeline. Let's start by understanding the requirements. The pipeline is linear with four steps: querying PostgreSQL, transforming to CSV, generating a PDF chart, and emailing the report. Each task depends on the previous one, so dependencies are straightforward.

First, I'll set up the DAG with the specified parameters. The schedule_interval is @daily, start_date is January 1, 2024, catchup is False. Default_args should include retries (2), retry_delay (5 minutes), and email on failure. The owner and email settings need to be filled, maybe using placeholders like 'data_team' and 'admin@company.com'.

Next, each task corresponds to a Python function. For query_sales_data, I'll use a PythonOperator. The function will connect to PostgreSQL using the postgres_default connection. It should execute a SQL query for the execution date. I'll need to fetch the date using kwargs['execution_date'] or the context. Maybe use a parameterized query with the date.

Then, transform_to_csv uses pandas to read the data and save as CSV. The function will take the data from the previous task, probably via XCom. So in the query_sales_data task, push the result to XCom, then pull it in transform_to_csv. Wait, but PostgreSQL might return a lot of data. Maybe better to write to a file directly instead of XCom? But the user mentioned using XCom for data flow. Hmm, but XCom has size limits. Oh, the problem says data flows from PostgreSQL to CSV file. Maybe the query task writes to /tmp/sales_data.csv, then transform_to_csv processes it? Or perhaps the query task saves the data into a file, and the next task reads it. But the original steps say transform_to_csv takes the query results. Maybe the first task returns a DataFrame, which is passed via XCom. But for large data, XCom isn't ideal. However, since the user specified the steps as written, perhaps proceed with XCom, assuming the data isn't too big.

Wait, the pipeline steps mention that the CSV is stored at /tmp/sales_report.csv. So maybe the transform_to_csv task reads the data from the query and writes to that file. So the query task returns the data (e.g., a list of tuples or a pandas DataFrame), then the transform task writes it to CSV. Similarly, generate_pdf_chart reads the CSV or the data from XCom to create the PDF. But the steps say transform_to_csv creates the CSV, then generate_pdf_chart uses that CSV? Or maybe the PDF generation uses the same data as the query. The description says "transforms the query results into CSV" and "creates a PDF bar chart visualizing sales by product". So perhaps both tasks use the data from the query. But if the CSV is written, maybe the PDF task reads from the CSV. But the user might have intended that the data is passed through XCom. Hmm, but the problem says data flow is PostgreSQL → CSV file → PDF chart. Wait, no: the data flow is PostgreSQL → CSV file → PDF chart → email. So the CSV is an intermediate file, then the PDF is generated from the CSV? Or both are generated from the query results. The steps say transform_to_csv transforms the query results into CSV, and generate_pdf_chart creates the PDF from the same query results. So perhaps the query task's output is used by both the CSV and PDF tasks. But in the pipeline steps, the tasks are sequential: query, then transform to CSV, then generate PDF, then email. So each task depends on the previous. So maybe the CSV is generated first, then the PDF is generated from the CSV? Or both are generated from the same data. The description isn't entirely clear. Let me recheck the pipeline steps:

- query_sales_data: extracts data for the execution date.

- transform_to_csv: transforms query results into CSV stored at /tmp/sales_report.csv.

- generate_pdf_chart: creates PDF bar chart stored at /tmp/sales_chart.pdf.

So the PDF is generated from the query results, not the CSV. Because the transform_to_csv task is before generate_pdf_chart. So the data flows from query to CSV, then query results (or CSV?) to PDF. Wait, the steps are sequential. So perhaps the PDF is generated from the CSV data. Because the CSV is created first. So the generate_pdf_chart task would read the CSV file. Alternatively, each task passes data via XCom. But for larger datasets, using files is better. The problem mentions that the CSV and PDF are stored in /tmp, so maybe each task writes to a file, and the next task reads it. But the initial query task would need to write the data to a file or pass via XCom.

Alternatively, the query task retrieves the data and stores it in a variable, which is passed via XCom to the next tasks. But XCom has size limitations, so for larger data, files are better. However, given that the user's example uses /tmp files, perhaps the tasks write to these files. Let's proceed under the assumption that each task writes to a file, and the next task reads it. But the problem says the data flow is PostgreSQL → CSV → PDF → email. So the CSV is generated from PostgreSQL data, then PDF is generated from CSV data? Or both are generated from the same PostgreSQL data. The description says "transforms the query results into CSV" and "creates a PDF bar chart visualizing sales by product". So the PDF is generated from the same query results as the CSV. Therefore, both tasks depend on the query_sales_data task's output.

But according to the orchestration, the tasks are in sequence: query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report. So the PDF task comes after the CSV task. That suggests that the PDF task might depend on the CSV task. But according to the description, the PDF is generated from the query results. So perhaps the data is passed through XCom from query_sales_data to both transform_to_csv and generate_pdf_chart. But since the tasks are sequential, maybe the data is passed along each step. Alternatively, the CSV and PDF are both generated from the same data retrieved by the query. But the order of tasks would require that transform_to_csv runs before generate_pdf_chart, even though they both depend on the query. That doesn't make sense. Wait, the pipeline steps say "executes sequentially with no parallelization", so each task depends on the previous. So the order is query → transform → generate → email. Therefore, the transform task must come after the query, generate after transform, etc. But if the generate_pdf task needs the query data, then it can't come after the transform task unless the data is passed through. So perhaps the query task's data is passed via XCom to transform_to_csv, which then writes the CSV. Then generate_pdf_chart reads the CSV file (since it's after the transform task), generates the PDF, and then email sends both files.

Alternatively, maybe the query task's data is stored in XCom, then transform_to_csv takes that data and writes CSV, generate_pdf_chart takes the same XCom data and writes PDF. But in that case, generate_pdf_chart could run in parallel with transform_to_csv, but the pipeline is sequential. So the user specified that the tasks run in sequence, so generate_pdf_chart must come after transform_to_csv. Therefore, perhaps the data flows from query to transform (via XCom), transform writes CSV, then generate reads CSV to create PDF. Then email sends both files. That makes sense. So the steps would be:

1. Query the database, get data, pass to transform via XCom.

2. Transform takes data, writes CSV.

3. Generate reads CSV, creates PDF.

4. Email sends both CSV and PDF.

But according to the pipeline steps, generate_pdf_chart is supposed to create the PDF from the query results, not the CSV. Hmm, this is a bit conflicting. The user might have intended that both CSV and PDF are generated from the same query data, but the tasks are sequential. So perhaps the query task's data is stored in XCom, then transform_to_csv uses that data to write CSV, then generate_pdf_chart uses the same XCom data to generate the PDF. But since the tasks are sequential, generate_pdf_chart comes after transform_to_csv. So the data is passed through XCom from query to transform, then from transform to generate? Or maybe each task pushes its output to XCom. But the PDF generation doesn't need the CSV data; it needs the original query data. So perhaps the query task pushes the data to XCom, and both transform and generate tasks pull from that same XCom. But since the tasks are sequential, the generate task would come after transform, but both use the same data from the query task. That's possible. So the dependencies are linear, but both transform and generate tasks depend on the query task. However, the user specified that the execution order is a strict linear sequence where each task depends on the previous. So transform depends on query, generate depends on transform, email depends on generate. Therefore, generate can't run until transform is done, even though it uses data from query. That's a problem because generate needs the data from query, which is two steps back. But in Airflow, XCom can be accessed across tasks. So the generate task can pull the XCom value from the query task. So even though generate comes after transform, it can still get the data from query. That's possible.

So the plan is:

- query_sales_data: executes SQL, pushes result (e.g., a DataFrame or list of records) to XCom.

- transform_to_csv: pulls data from query_sales_data's XCom, writes to CSV.

- generate_pdf_chart: pulls data from query_sales_data's XCom, generates PDF.

- email_sales_report: sends both files.

But according to the task dependencies, generate_pdf_chart depends on transform_to_csv. So even though generate_pdf_chart doesn't need the output of transform_to_csv, it has to wait for it to complete. That's because the user specified a strict linear sequence. So the tasks must run in order, even if they don't strictly need the prior task's output. So perhaps the CSV and PDF are both generated from the same data, but the tasks are sequenced. That's acceptable as per the user's description.

Now, coding this.

First, import necessary modules. The user specified Airflow 2.x style imports. So:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging

Wait, but the user mentioned that if sensors are needed, use them. But in this case, there are no sensors mentioned. So no need for sensors here.

Default_args should include owner, email, retries, retry_delay, etc. So:

default_args = {
    'owner': 'data_team',
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

Then, define the DAG:

with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reporting', 'sales'],
) as dag:

Then, define the Python functions for each task.

First task: query_sales_data.

def query_sales_data(**context):
    # Use PostgresHook to get connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Get execution date
    execution_date = context['execution_date']
    # Or format it as needed
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Execute query
    query = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product
        ORDER BY total_sales DESC;
    """
    cursor.execute(query, (date_str,))
    results = cursor.fetchall()
    
    # Close connection
    cursor.close()
    conn.close()
    
    # Push results to XCom
    context['ti'].xcom_push(key='sales_data', value=results)
    
    logging.info(f"Retrieved {len(results)} sales records for {date_str}")

Next, transform_to_csv.

def transform_to_csv(**context):
    # Pull data from XCom
    sales_data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    
    # Convert to DataFrame
    df = pd.DataFrame(sales_data, columns=['product', 'total_sales'])
    
    # Save to CSV
    csv_path = '/tmp/sales_report.csv'
    df.to_csv(csv_path, index=False)
    logging.info(f"CSV report saved to {csv_path}")

Then, generate_pdf_chart.

def generate_pdf_chart(**context):
    # Pull data from XCom
    sales_data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    
    # Convert to DataFrame
    df = pd.DataFrame(sales_data, columns=['product', 'total_sales'])
    
    # Create bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(df['product'], df['total_sales'])
    plt.title('Daily Sales by Product')
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.xticks(rotation=45)
    
    # Save to PDF
    pdf_path = '/tmp/sales_chart.pdf'
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.close()
    logging.info(f"PDF chart saved to {pdf_path}")

Finally, email_sales_report.

def email_sales_report(**context):
    # Email details
    to = ['management@company.com']
    subject = 'Daily Sales Report'
    body = 'Attached are the daily sales report CSV and chart PDF.'
    
    # Attachments
    csv_path = '/tmp/sales_report.csv'
    pdf_path = '/tmp/sales_chart.pdf'
    
    # Send email
    send_email(
        to=to,
        subject=subject,
        html_content=body,
        files=[csv_path, pdf_path]
    )
    logging.info("Email sent with attachments")

Now, create the tasks using PythonOperator.

query_task = PythonOperator(
    task_id='query_sales_data',
    python_callable=query_sales_data,
    provide_context=True,
)

transform_task = PythonOperator(
    task_id='transform_to_csv',
    python_callable=transform_to_csv,
    provide_context=True,
)

generate_task = PythonOperator(
    task_id='generate_pdf_chart',
    python_callable=generate_pdf_chart,
    provide_context=True,
)

email_task = PythonOperator(
    task_id='email_sales_report',
    python_callable=email_sales_report,
    provide_context=True,
)

Set dependencies: query_task >> transform_task >> generate_task >> email_task.

But wait, according to the pipeline steps, each task depends on the previous. So the order is correct.

But in the code above, both transform and generate tasks pull data from query_sales_data's XCom. So even though generate comes after transform, it's still using the same XCom data. That's okay.

Now, check for potential issues. For example, the email function uses send_email from airflow.utils.email, which requires the correct SMTP setup. The user mentioned that the email system uses SMTP service, so assuming that the Airflow configuration has SMTP properly set up.

Also, the PostgresHook uses 'postgres_default' connection, which should be defined in Airflow.

The functions use logging.info to log steps.

Now, putting it all together. Also, ensure that all imports are included. The code should be PEP 8 compliant, proper indentation, etc.

Wait, the email_sales_report function uses send_email with 'files' parameter. Looking at the Airflow documentation, the send_email function's parameters may vary. The standard EmailOperator uses 'files' as a parameter for attachments, but the airflow.utils.email.send_email function may not have that. Wait, the send_email function in Airflow is part of the email utils and may not support file attachments directly. Oh, right, the send_email function from airflow.utils.email does not handle attachments. So using that function won't work for sending attachments. Therefore, the email_sales_report task needs to use a different method to send emails with attachments.

In that case, perhaps using the EmailOperator with the 'files' parameter, but the EmailOperator is from airflow.operators.email. However, the user's constraints specify using PythonOperator, so perhaps the email task uses smtplib directly to send emails with attachments. Alternatively, use the EmailMessage class from airflow.utils.email to construct the email.

Alternatively, use the EmailOperator with the files parameter. Let me check.

The EmailOperator in Airflow 2.x has a 'files' parameter which is a list of file paths to attach. So perhaps using EmailOperator instead of PythonOperator for the email task. But the user's pipeline steps specify that email_sales_report is a PythonOperator task. Wait, no, the user didn't specify the operator types, just the steps. So perhaps using EmailOperator is better here.

But the user's code example uses PythonOperator for all tasks. Let me check the pipeline steps: the email step is "Sends an email with both the CSV data file and PDF chart attached". So using EmailOperator would be more efficient, but the user's code may require PythonOperator. However, the user's constraints say to use appropriate operators. Since sending email with attachments can be done via EmailOperator, perhaps that's better.

Wait, the problem says: "If sensors appear in the description, demonstrate proper use". There are no sensors here, so no need. But for operators, use appropriate ones. The EmailOperator is suitable for sending emails. So perhaps using EmailOperator for the email task.

But the original code uses PythonOperator. Let me think. The user's pipeline steps mention using Python libraries like pandas and matplotlib, but for email, using SMTP service. So perhaps using the EmailOperator is better here. Let's adjust.

So, modifying the email task:

from airflow.operators.email import EmailOperator

...

email_task = EmailOperator(
    task_id='email_sales_report',
    to='management@company.com',
    subject='Daily Sales Report',
    html_content='Attached are the daily sales report CSV and chart PDF.',
    files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf'],
)

This would simplify the code and avoid having to write a custom Python function for sending emails. However, the EmailOperator requires the 'files' parameter to be a list of file paths, which are attached to the email. This would work if the files exist by the time the email task runs.

But according to the task sequence, the CSV and PDF files are generated in previous tasks, so they should exist. Therefore, using EmailOperator is better here.

So adjusting the code:

Remove the email_sales_report function and replace with EmailOperator.

But then, the code would have:

from airflow.operators.email import EmailOperator

...

email_task = EmailOperator(
    task_id='email_sales_report',
    to='management@company.com',
    subject='Daily Sales Report',
    html_content='Attached are the daily sales report CSV and chart PDF.',
    files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf'],
)

This is cleaner and uses the built-in operator. However, the original problem statement says that the email_sales_report step is part of the pipeline steps, and the user didn't specify operator types. So using EmailOperator is acceptable and better practice.

Therefore, modifying the code accordingly.

So, updating the code:

Remove the email_sales_report Python function and use EmailOperator.

Now, the tasks are:

query_task = PythonOperator(...)

transform_task = PythonOperator(...)

generate_task = PythonOperator(...)

email_task = EmailOperator(...)

Then dependencies: query_task >> transform_task >> generate_task >> email_task.

Wait, but according to the pipeline steps, the email task needs both the CSV and PDF files. The generate_pdf_chart task generates the PDF, and the transform_to_csv generates the CSV. So the email task should depend on both? No, because the tasks are sequential: transform comes before generate. So generate depends on transform, and email depends on generate. Therefore, by the time email runs, both files should have been created. Because transform creates CSV, then generate creates PDF. So email comes after generate, so both files exist.

Yes, because the tasks are in order: query → transform (CSV) → generate (PDF) → email. So when email runs, both files are present.

Therefore, the EmailOperator can safely attach both files.

So the code is correct.

Now, putting all together.

Imports:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging

Wait, the start_date is January 1, 2024. So in the DAG definition:

start_date=datetime(2024, 1, 1),

But when using days_ago, it's better to use a fixed start date. So the code uses datetime(2024, 1, 1).

Now, default_args:

default_args = {
    'owner': 'data_team',
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG definition:

with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reporting', 'sales'],
) as dag:

Then the functions:

def query_sales_data(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    query = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product
        ORDER BY total_sales DESC;
    """
    cursor.execute(query, (date_str,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    context['ti'].xcom_push(key='sales_data', value=results)
    logging.info(f"Retrieved {len(results)} sales records for {date_str}")

def transform_to_csv(**context):
    sales_data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    df = pd.DataFrame(sales_data, columns=['product', 'total_sales'])
    csv_path = '/tmp/sales_report.csv'
    df.to_csv(csv_path, index=False)
    logging.info(f"CSV report saved to {csv_path}")

def generate_pdf_chart(**context):
    sales_data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    df = pd.DataFrame(sales_data, columns=['product', 'total_sales'])
    
    plt.figure(figsize=(10, 6))
    plt.bar(df['product'], df['total_sales'])
    plt.title('Daily Sales by Product')
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.xticks(rotation=45)
    
    pdf_path = '/tmp/sales_chart.pdf'
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.close()
    logging.info(f"PDF chart saved to {pdf_path}")

Then the tasks:

query_task = PythonOperator(
    task_id='query_sales_data',
    python_callable=query_sales_data,
)

transform_task = PythonOperator(
    task_id='transform_to_csv',
    python_callable=transform_to_csv,
)

generate_task = PythonOperator(
    task_id='generate_pdf_chart',
    python_callable=generate_pdf_chart,
)

email_task = EmailOperator(
    task_id='email_sales_report',
    to='management@company.com',
    subject='Daily Sales Report',
    html_content='Attached are the daily sales report CSV and chart PDF.',
    files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf'],
)

Dependencies:

query_task >> transform_task >> generate_task >> email_task

Now, check for potential issues:

- The EmailOperator's 'files' parameter expects the files to exist. Since transform_task creates the CSV and generate_task creates the PDF, by the time email_task runs, both files should be present.

- The PostgresHook uses 'postgres_default' connection, which must be set up in Airflow.

- The functions use context['ti'].xcom_push and xcom_pull correctly.

- The matplotlib figure is closed after saving to avoid memory issues.

- The DAG is properly structured with the context manager.

- All imports are included.

- PEP 8 compliance: lines under 79 chars, proper spacing, etc.

This should be a complete, executable DAG.
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import logging

default_args = {
    'owner': 'data_team',
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def query_sales_data(**context):
    """Execute daily sales query from PostgreSQL database."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    query = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product
        ORDER BY total_sales DESC
    """
    cursor.execute(query, (execution_date,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    context['ti'].xcom_push(key='sales_data', value=results)
    logging.info(f"Retrieved {len(results)} sales records")

def transform_to_csv(**context):
    """Convert query results to CSV format."""
    data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    df = pd.DataFrame(data, columns=['product', 'total_sales'])
    csv_path = '/tmp/sales_report.csv'
    df.to_csv(csv_path, index=False)
    logging.info(f"Generated CSV report at {csv_path}")

def generate_pdf_chart(**context):
    """Create PDF bar chart from sales data."""
    data = context['ti'].xcom_pull(task_ids='query_sales_data', key='sales_data')
    df = pd.DataFrame(data, columns=['product', 'total_sales'])
    
    plt.figure(figsize=(10, 6))
    plt.bar(df['product'], df['total_sales'])
    plt.title('Daily Sales by Product')
    plt.xlabel('Product')
    plt.ylabel('Sales Amount')
    plt.xticks(rotation=45)
    
    pdf_path = '/tmp/sales_chart.pdf'
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.close()
    logging.info(f"Generated PDF chart at {pdf_path}")

with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reporting', 'sales']
) as dag:
    
    query_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data
    )
    
    csv_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv
    )
    
    pdf_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart
    )
    
    email_task = EmailOperator(
        task_id='email_sales_report',
        to='management@company.com',
        subject='Daily Sales Report',
        html_content='Attached find today\'s sales report',
        files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf']
    )
    
    query_task >> csv_task >> pdf_task >> email_task