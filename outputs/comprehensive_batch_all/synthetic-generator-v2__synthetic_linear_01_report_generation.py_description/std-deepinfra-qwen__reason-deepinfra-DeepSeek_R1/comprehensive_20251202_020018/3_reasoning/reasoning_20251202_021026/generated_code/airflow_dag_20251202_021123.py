<think>
Okay, let's tackle this problem. The user wants an Apache Airflow DAG that represents a daily sales reporting pipeline. The steps are linear: query PostgreSQL, transform to CSV, generate a PDF chart, and email the report. 

First, I need to make sure I'm using Airflow 2.x style imports. So, I'll import DAG from airflow, and PythonOperator from airflow.operators.python. Since there are no sensors mentioned, I don't need to include those. But wait, the data source is PostgreSQL, so maybe a PostgresHook would be useful here. Oh right, the user mentioned using operators, so perhaps the PostgresOperator could be used for querying. But the step says 'query_sales_data' executes a PostgreSQL query, so maybe a PythonOperator that uses the PostgresHook to fetch data.

Wait, the first step is to extract daily sales data aggregated by product for the execution date. So the query probably needs the execution date as a parameter. In Airflow, we can access the execution date via the context variables, like ds or data_interval_start. So the function for query_sales_data should pull the data for that date.

Next, transforming to CSV. That would involve using pandas to take the query results and write to a CSV file. Similarly, generating a PDF chart would use matplotlib to create a bar chart and save it as PDF. Then, the email task would use the EmailOperator to send the email with attachments. Wait, but EmailOperator is part of airflow.operators.email. So I need to import that.

Wait, the user's constraints mention that if sensors are in the description, use them. But the pipeline doesn't mention any sensors, so no need for FileSensor or others. The steps are all sequential.

Default args should include the start date, retries, retry delay, email on failure, etc. The DAG should have schedule_interval set to '@daily', catchup=False, and the default_args as specified.

Each task depends on the previous one, so the dependencies are linear: query_sales_data >> transform_to_csv >> generate_pdf_chart >> email_sales_report.

Now, functions for each task. For query_sales_data, using PostgresHook to get the connection, execute the SQL, and save the results. Maybe store the data in a variable that can be passed to the next task. But since tasks are separate, how to pass data between them? Using XCom. So the query task will push the data to XCom, then the transform task pulls it. But wait, for larger datasets, XCom might not be ideal. However, the user's pipeline uses local files for CSV and PDF, so maybe the query task writes to a temporary file, but the steps mention that CSV is stored at /tmp/sales_report.csv. Wait, no: the transform_to_csv step is supposed to take the query results and write to CSV. So perhaps the query task fetches the data and returns it (via XCom), then the transform task takes that data and writes to CSV. But if the data is large, XCom might not be suitable. Alternatively, maybe the query task writes directly to the CSV, but the description says the transform step does that. Hmm.

Wait, the pipeline steps say:

- query_sales_data: Executes a PostgreSQL query to extract daily sales data aggregated by product for the execution date

- transform_to_csv: Transforms the query results into a CSV file stored at /tmp/sales_report.csv

So the query task gets the data, and the transform task processes it into CSV. So the data needs to be passed from query_sales_data to transform_to_csv. Since it's a daily report, maybe the data isn't too big, so using XCom is acceptable. But XCom has size limits. Alternatively, the query task could save the data to a file, and the transform task reads it. But according to the description, the transform task is the one that creates the CSV. So perhaps the query task fetches the data and stores it in a variable, which is then passed via XCom to the transform task. Then the transform task uses pandas to convert it to CSV.

Similarly, the generate_pdf_chart task would read the CSV or use the data from XCom to create the PDF. Wait, but the CSV is stored at /tmp/sales_report.csv, so maybe the transform task writes that file, and the generate_pdf reads it. Or perhaps the data is passed through XCom. Let me think.

Alternatively, the query task saves the data into a pandas DataFrame, which is then passed via XCom (as a JSON string or pickled), then the transform task writes it to CSV. Then the PDF task reads the CSV or uses the same DataFrame. But the steps say transform_to_csv creates the CSV, and generate_pdf_chart creates the PDF from the same data. So perhaps the data is passed through XCom from query to transform, then transform writes CSV, and generate_pdf reads the CSV. Or maybe the data is passed through XCom all the way. But for larger data, reading from CSV might be better.

But given that the user's example uses /tmp for storage, perhaps the CSV and PDF are written there, and the email task attaches those files. So the tasks would need to generate those files in /tmp, and the email task sends them as attachments.

So, the steps would be:

1. query_sales_data: runs a SQL query, fetches the data (using PostgresHook), and pushes the result (maybe a list of tuples or a DataFrame) to XCom.

2. transform_to_csv: takes the data from XCom, converts to a DataFrame, writes to CSV at /tmp/sales_report.csv.

3. generate_pdf_chart: reads the CSV (or uses the DataFrame from XCom) to generate a bar chart PDF at /tmp/sales_chart.pdf.

4. email_sales_report: uses EmailOperator to send an email with both attachments.

Wait, but the EmailOperator doesn't support attachments by default. Oh right, the standard EmailOperator doesn't handle attachments. So perhaps we need to use a PythonOperator that uses the smtplib library or another method to send the email with attachments. Alternatively, use the EmailMessage from airflow.utils.email, but that also might not handle attachments. Hmm, this could be a problem.

The user's pipeline says the email system is SMTP, so perhaps the email task uses SMTP settings in Airflow and a custom Python function to send the email with attachments. So the email_sales_report task would be a PythonOperator that calls a function to send the email with the CSV and PDF files as attachments.

So, putting this together:

For each task:

- query_sales_data: PythonOperator that uses PostgresHook to get data, returns it (so pushed to XCom).

- transform_to_csv: PythonOperator that takes the data from XCom (via ti.xcom_pull), converts to DataFrame, writes CSV.

- generate_pdf_chart: PythonOperator that reads the CSV (or uses the DataFrame from XCom) to generate the PDF.

Wait, but if transform_to_csv writes the CSV, then generate_pdf_chart can read that CSV file. So maybe the transform task doesn't need to pass data via XCom. Instead, the query task writes the data to XCom, transform reads it and writes CSV, generate reads CSV and makes PDF. Or perhaps generate_pdf reads the CSV. Alternatively, generate_pdf could take the data from XCom as well. Let's see.

But the pipeline steps say that each task depends on the previous one. So the data flow is PostgreSQL → CSV → PDF → Email. So perhaps the CSV is written by the transform task, then the PDF task reads that CSV. But that's not efficient. Alternatively, the data is passed through XCom from query to transform, then transform writes CSV and perhaps passes the data to generate_pdf via XCom again. But that's possible.

Alternatively, maybe the data is passed through each task via XCom. So query pushes data to XCom, transform pulls it, processes it, writes CSV, and perhaps pushes some data (like the path to the CSV) to XCom. Then generate_pdf pulls that path, reads the CSV, generates PDF, and pushes the PDF path. Then email task pulls both paths and attaches them.

But perhaps it's simpler to have each task generate the necessary files and the email task just sends them. So:

- query_sales_data: gets data, stores in XCom.

- transform_to_csv: reads data from XCom, writes CSV to /tmp/sales_report.csv.

- generate_pdf_chart: reads data from XCom (or CSV), generates PDF to /tmp/sales_chart.pdf.

- email_sales_report: sends email with both files as attachments.

But how to handle the data flow. Let's outline the functions.

For query_sales_data:

def query_sales_data(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT product, SUM(amount) FROM sales WHERE date = %s GROUP BY product;"
    execution_date = context['execution_date']
    # Format the execution date as a string suitable for the SQL query
    date_str = execution_date.strftime('%Y-%m-%d')
    results = hook.get_records(sql, parameters=(date_str,))
    # Convert to a DataFrame for easier handling
    import pandas as pd
    df = pd.DataFrame(results, columns=['product', 'total_sales'])
    # Return the DataFrame as a dictionary to store in XCom
    return df.to_dict('records')

Wait, but XCom can't store DataFrames directly, so converting to a dictionary or JSON is better. Then, the transform task can read this and create a DataFrame.

transform_to_csv:

def transform_to_csv(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='query_sales_data')
    import pandas as pd
    df = pd.DataFrame(data, columns=['product', 'total_sales'])
    df.to_csv('/tmp/sales_report.csv', index=False)

generate_pdf_chart:

def generate_pdf_chart(**context):
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    ti = context['ti']
    data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(data, columns=['product', 'total_sales'])
    
    with PdfPages('/tmp/sales_chart.pdf') as pdf:
        plt.figure(figsize=(10,6))
        plt.bar(df['product'], df['total_sales'])
        plt.title('Sales by Product')
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

email_sales_report:

def email_sales_report(**context):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    # Email configuration
    sender_email = 'noreply@company.com'
    receiver_email = 'management@company.com'
    subject = 'Daily Sales Report'
    body = 'Please find attached the daily sales report and chart.'

    # Create message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Attach CSV
    csv_path = '/tmp/sales_report.csv'
    with open(csv_path, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= sales_report.csv')
    msg.attach(part)

    # Attach PDF
    pdf_path = '/tmp/sales_chart.pdf'
    with open(pdf_path, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= sales_chart.pdf')
    msg.attach(part)

    # Send email
    smtp_server = 'smtp.company.com'
    smtp_port = 587
    smtp_username = 'user'
    smtp_password = 'password'

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()

But wait, the SMTP credentials should be handled via Airflow connections. So instead of hardcoding, we can use the SMTP connection settings from Airflow. However, the EmailOperator doesn't support attachments, so using a Python function is necessary. Alternatively, use the EmailMessage class with attachments, but I'm not sure how that's done in Airflow. Alternatively, use the send_email method with attachments via the SMTP hook.

Alternatively, use the airflow.utils.email.send_email method, but I'm not sure if it supports attachments. So perhaps using the SMTPHook from Airflow.

Wait, there's an SmtpHook in Airflow providers. So maybe:

from airflow.providers.smtp.hooks.smtp import SmtpHook

But I need to check. Alternatively, use the SmtpHook to get the connection and send the email with attachments.

But perhaps it's easier to handle it via the code above, but using Airflow's connection for SMTP. So in the code, retrieve the SMTP connection details from Airflow's connection.

Modify the email_sales_report function to get SMTP settings from the connection.

So:

def email_sales_report(**context):
    from airflow.providers.smtp.hooks.smtp import SmtpHook
    hook = SmtpHook(smtp_conn_id='smtp_default')
    sender_email = 'noreply@company.com'
    receiver_email = 'management@company.com'
    subject = 'Daily Sales Report'
    body = 'Please find attached the daily sales report and chart.'
    files = ['/tmp/sales_report.csv', '/tmp/sales_chart.pdf']

    # Use the hook to send the email with attachments
    hook.send_email_smtp(
        to=receiver_email,
        subject=subject,
        html_content=body,
        files=files,
        from_email=sender_email
    )

Wait, but does the SmtpHook's send_email_smtp method support attachments? Looking at the documentation, the send_email_smtp method has a 'files' parameter which is a list of file paths to attach. So yes, this should work. So using the SmtpHook would be better, as it uses the connection configured in Airflow (smtp_default), which includes the SMTP server, port, login, password, etc.

So the email_sales_report task can use the SmtpHook to send the email with attachments. That's better than hardcoding the SMTP settings.

So, putting it all together:

The DAG will have default_args with the specified parameters: start_date is 2024-01-01, retries=2, retry_delay=5 minutes, email on failure, etc.

The tasks are PythonOperators for each step, with dependencies set in a linear chain.

Now, code:

Import necessary modules:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

Default args:

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alert@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

DAG definition:

with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    description='Daily sales report generation and distribution',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'reporting'],
) as dag:

Then define each task.

But wait, functions inside the DAG definition? Or outside? In Python, functions can be defined inside or outside, but typically, if they are specific to the DAG, they can be inside.

But for clarity, perhaps define the functions inside the DAG context. Alternatively, define them outside. However, since the code needs to be a single module, functions can be defined inside or outside. Let's structure it properly.

So, code outline:

imports

default_args = { ... }

def query_sales_data(**context):
    # code

def transform_to_csv(**context):
    # code

def generate_pdf_chart(**context):
    # code

def email_sales_report(**context):
    # code

with DAG(...) as dag:
    query_task = PythonOperator(...)
    transform_task = PythonOperator(...)
    generate_task = PythonOperator(...)
    email_task = PythonOperator(...)

    query_task >> transform_task >> generate_task >> email_task

But wait, the functions need to be defined before the DAG block. So the order would be:

Imports, default_args, function definitions, DAG definition with tasks.

Now, code:

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alert@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def query_sales_data(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    sql = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE date = %s
        GROUP BY product;
    """
    results = hook.get_records(sql, parameters=(execution_date,))
    df = pd.DataFrame(results, columns=['product', 'total_sales'])
    return df.to_dict('records')

def transform_to_csv(**context):
    ti = context['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(sales_data)
    df.to_csv('/tmp/sales_report.csv', index=False)

def generate_pdf_chart(**context):
    ti = context['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(sales_data)
    
    with PdfPages('/tmp/sales_chart.pdf') as pdf:
        plt.figure(figsize=(10, 6))
        plt.bar(df['product'], df['total_sales'])
        plt.title('Sales by Product - ' + context['ds'])
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

def email_sales_report(**context):
    hook = SmtpHook(smtp_conn_id='smtp_default')
    to_email = 'management@company.com'
    subject = f"Daily Sales Report - {context['ds']}"
    body = 'Attached are the daily sales report CSV and PDF chart.'
    files = ['/tmp/sales_report.csv', '/tmp/sales_chart.pdf']
    hook.send_email_smtp(
        to=to_email,
        subject=subject,
        html_content=body,
        files=files,
        from_email='noreply@company.com'
    )

with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

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

    email_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report,
    )

    query_task >> transform_task >> generate_task >> email_task

Wait, but the email_sales_report uses SmtpHook's send_email_smtp, which requires the 'files' parameter. Does that method actually attach the files? Looking at the source code for SmtpHook in Airflow, the send_email_smtp method does support a 'files' argument which is a list of file paths to attach. So this should work.

Also, the PostgresHook.get_records returns a list of tuples, which is then converted to a DataFrame with columns. The XCom is storing the DataFrame as a list of dicts via to_dict('records'), which is JSON serializable.

In transform_to_csv, the data is pulled from XCom and converted back to a DataFrame. Similarly for generate_pdf_chart.

The functions use the context to get the execution date (in query_sales_data) and the task instance (ti) to pull XCom data.

The DAG is set with catchup=False, schedule_interval='@daily', start_date as Jan 1, 2024.

The email task's subject includes the execution date (context['ds']), which is the logical date.

Potential issues:

- The /tmp directory might not be writable by Airflow workers, especially if they are running in different containers or hosts. But the user's description mentions using local temporary storage at /tmp/, so assuming that's acceptable.

- The SMTP and PostgreSQL connections must be set up in Airflow with the IDs 'postgres_default' and 'smtp_default'. The code uses these connection IDs.

- The functions don't have error handling. For example, if the query returns no data, the CSV and PDF generation might fail. But the user's retry policy is 2 retries with 5-minute delays, which is handled by the default_args.

- The email_sales_report function uses 'noreply@company.com' as the from email, which should be configured in the SMTP connection or allowed by the SMTP server.

This code should meet all the user's requirements: linear tasks, proper imports, DAG settings, minimal defaults, and uses the required libraries (pandas, matplotlib).
</think>

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alert@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def query_sales_data(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    sql = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE date = %s
        GROUP BY product;
    """
    results = hook.get_records(sql, parameters=(execution_date,))
    df = pd.DataFrame(results, columns=['product', 'total_sales'])
    return df.to_dict('records')

def transform_to_csv(**context):
    ti = context['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(sales_data)
    df.to_csv('/tmp/sales_report.csv', index=False)

def generate_pdf_chart(**context):
    ti = context['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(sales_data)
    
    with PdfPages('/tmp/sales_chart.pdf') as pdf:
        plt.figure(figsize=(10, 6))
        plt.bar(df['product'], df['total_sales'])
        plt.title(f"Sales by Product - {context['ds']}")
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

def email_sales_report(**context):
    hook = SmtpHook(smtp_conn_id='smtp_default')
    to_email = 'management@company.com'
    subject = f"Daily Sales Report - {context['ds']}"
    body = 'Attached are the daily sales report CSV and PDF chart.'
    files = ['/tmp/sales_report.csv', '/tmp/sales_chart.pdf']
    hook.send_email_smtp(
        to=to_email,
        subject=subject,
        html_content=body,
        files=files,
        from_email='noreply@company.com'
    )

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

    email_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report,
    )

    query_task >> transform_task >> generate_task >> email_task