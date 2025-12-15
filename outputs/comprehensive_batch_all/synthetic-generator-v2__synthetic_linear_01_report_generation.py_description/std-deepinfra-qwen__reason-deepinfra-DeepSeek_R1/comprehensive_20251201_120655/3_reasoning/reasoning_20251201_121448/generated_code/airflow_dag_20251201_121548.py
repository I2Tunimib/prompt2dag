<think>
Okay, let's tackle this problem. The user wants to convert a pipeline description into an Apache Airflow DAG. The pipeline is a linear daily sales reporting process. Let me break down the requirements step by step.

First, the DAG needs to have daily scheduling starting January 1, 2024, with catchup disabled. The tasks are sequential: query data, transform to CSV, generate PDF chart, and email the report. Each task depends on the previous one's success.

I should start by setting up the DAG with the correct parameters. The schedule_interval is '@daily', catchup is False. Default_args should include the start_date, retries (2), retry_delay (5 minutes), and email on failure. Need to make sure the owner and email are placeholders, maybe 'data_team' and 'admin@company.com' as defaults.

Next, each task corresponds to a Python function. The query_sales_data task uses PostgresHook to fetch data from PostgreSQL. The connection is specified as 'postgres_default', so I'll use that in the hook. The execution date can be obtained from the context, then formatted as a date to filter the query.

For transform_to_csv, pandas will be used to convert the data to a CSV. The function will take the data from the previous task via XCom, maybe using ti.xcom_pull. Then save to '/tmp/sales_report.csv'.

Generate_pdf_chart uses matplotlib to create a bar chart. Again, data from XCom, create a bar chart, save as PDF in '/tmp/sales_chart.pdf'. Need to make sure the directory exists, but since it's /tmp/, maybe assume it's there.

Email_sales_report uses EmailOperator, but wait, the user specified to use PythonOperator. Wait, the pipeline steps mention sending an email with attachments. The EmailOperator can handle that, but maybe the user expects a Python function using SMTP. However, using the EmailOperator might be simpler if the SMTP connection is set up. But the user's constraints say to use PythonOperator if the step is in the description. Hmm, the pipeline step says 'Sends an email', so perhaps the EmailOperator is appropriate here. But the user's example in the problem uses PythonOperator. Wait, the constraints mention that if sensors are needed, use them, but here there are no sensors. Wait, the problem says to use PythonOperator for the steps listed. So maybe each step is a PythonOperator, including the email one. But sending an email with attachments via Python code would require using smtplib or similar. Alternatively, maybe the EmailOperator can be used if the attachments are handled. However, the EmailOperator might not support attachments out of the box. So perhaps the user expects a Python function that uses email libraries to send the email with attachments. Let me check the pipeline description's integrations: SMTP service is used. So the code should use a Python function that sends the email via SMTP, attaching the CSV and PDF files.

So, for email_sales_report, I'll write a Python function that uses smtplib or email library to construct the message with attachments. But that's a bit involved. Alternatively, maybe use the SendEmailOperator with the files parameter, but I'm not sure. Wait, the Airflow EmailOperator doesn't support attachments by default. So the correct approach is to use a PythonOperator that implements sending the email with attachments via SMTP.

Therefore, the email task will be a PythonOperator calling a function that handles attaching the files. The function will need to read the files from /tmp/, create the email message, and send it via SMTP. The SMTP connection details (like host, port, credentials) should come from an Airflow connection, perhaps the 'smtp_default' connection. So in the code, I'll use the SmtpHook to get the SMTP connection details.

Putting it all together: each task is a PythonOperator, with the functions performing the necessary operations. The dependencies are linear, so each task is set to run after the previous one using the >> operator.

Now, considering the imports: need to import DAG, PythonOperator, PostgresHook, maybe SmtpHook. Also, pandas and matplotlib for the transformations. The functions will be defined inside the DAG file.

Wait, but the user's constraints mention using Airflow 2.x style imports. So for PostgresHook, it's from airflow.providers.postgres.hooks.postgres import PostgresHook. Similarly, SmtpHook is from airflow.providers.smtp.hooks.smtp import SmtpHook. But the EmailOperator is from airflow.providers.smtp.operators.smtp import SmtpOperator, but if we're using a Python function, perhaps we'll use SmtpHook directly.

Also, the functions need to pull data from XCom. For example, query_sales_data will push the data to XCom, which transform_to_csv will pull.

Let me outline the code structure:

1. Import necessary modules: DAG, PythonOperator, PostgresHook, SmtpHook, etc.
2. Define default_args with the required parameters.
3. Instantiate the DAG with the specified parameters.
4. Define the four tasks as PythonOperators.
5. Set the dependencies using >>.

Now, let's draft each function.

For query_sales_data:
- Use PostgresHook to connect to 'postgres_default'.
- Get the execution date from context['execution_date'], format it as YYYY-MM-DD.
- Execute SQL query to select daily sales aggregated by product.
- Return the result (maybe as a list of dicts or a DataFrame) so it can be passed via XCom.

Transform_to_csv:
- Pull the data from XCom (ti.xcom_pull(task_ids='query_sales_data')).
- Convert to DataFrame, save to CSV.

Generate_pdf_chart:
- Pull data from XCom again (or read from CSV? But the previous task saves to CSV, so maybe read from the file. Wait, the data flow is PostgreSQL → CSV → PDF. So the transform_to_csv task writes to CSV, then generate_pdf reads from that CSV. But in the pipeline description, the data flows from PostgreSQL to CSV to PDF. So perhaps each task uses the output file from the previous step. So query_sales_data writes to the database (no, it's querying), then transform_to_csv writes to CSV, generate_pdf reads CSV and writes PDF, email sends both files.

Wait, the pipeline steps say:

- query_sales_data: extract data from PostgreSQL for the execution date.

- transform_to_csv: transforms query results into CSV.

So the CSV is generated from the query results, not from the database directly. So the data is passed via XCom? Or maybe the query_sales_data task writes the data to a file, but the description says the CSV is stored at /tmp/sales_report.csv. So perhaps each task writes to the file system, and the next task reads from there. But in that case, the tasks are not passing data via XCom but through files. However, XCom is for small data, so if the data is large, using files is better. But for the sake of the example, maybe the query_sales_data returns the data as a list of records, which is then used by transform_to_csv to write to CSV. But if the data is large, XCom might not be suitable. However, the problem states that the data flow is PostgreSQL → CSV file → PDF chart → Email attachments. So perhaps the CSV is written by transform_to_csv, which is then read by generate_pdf_chart to create the PDF, and both files are attached by email.

In that case, the query_sales_data task would execute the SQL and return the data (maybe as a DataFrame), which is then passed to transform_to_csv via XCom. But if the data is large, this might not be efficient. Alternatively, the query_sales_data could write the data to a temporary file, and the next task reads it. However, the problem states that the data flow is through the tasks via the files. Wait, the pipeline steps mention that the CSV and PDF are stored in /tmp/, so perhaps each task writes to /tmp/ and the next task reads from there. But how does the data get from query_sales_data to transform_to_csv? The query_sales_data would need to write the data to a file, but the description says transform_to_csv transforms the query results into CSV. So maybe the query_sales_data task returns the data, which is then used by transform_to_csv to create the CSV. Let's proceed with that approach.

So, query_sales_data executes the SQL, fetches the data, and returns it (perhaps as a list of tuples or a pandas DataFrame). Then transform_to_csv takes that data, converts it to CSV, and saves it to /tmp/sales_report.csv. Then generate_pdf_chart reads that CSV, generates the PDF chart, and saves it to /tmp/sales_chart.pdf. Finally, email_sales_report attaches both files.

But wait, generate_pdf_chart could read the CSV file created by transform_to_csv. So maybe transform_to_csv doesn't need to receive the data via XCom, but instead writes it to the file, and generate_pdf_chart reads the file. That would be more efficient, especially for larger data. However, the pipeline description says the data flows from PostgreSQL to CSV to PDF. So the CSV is an intermediate step stored in /tmp/, which is then used by the PDF task. So in that case, the transform_to_csv task writes the CSV file, and generate_pdf_chart reads it from the filesystem. Then the data isn't passed via XCom, which is better for larger datasets.

But how does query_sales_data pass the data to transform_to_csv? If the data is written to a file by query_sales_data, then transform_to_csv reads it. But the description says transform_to_csv transforms the query results into CSV. So perhaps query_sales_data doesn't write any file, but returns the data via XCom, and transform_to_csv takes that data and writes the CSV. That makes sense if the data isn't too large.

So, let's structure it that way. The query_sales_data task returns the data (maybe a list of dictionaries), which is pulled by transform_to_csv via XCom, then written to CSV. Then generate_pdf_chart reads the CSV file, creates the PDF, and saves it. Then email task attaches both files.

Wait, but generate_pdf_chart could also take the data from XCom, but the description says it's transforming the CSV into a PDF. So maybe the CSV is the source for the PDF. So transform_to_csv writes the CSV, then generate_pdf_chart reads that CSV to create the PDF. That way, the data flow is through the filesystem, which is more realistic for larger data.

In that case, the query_sales_data task would write the data to a temporary file (maybe in /tmp/), then transform_to_csv reads that file and writes the CSV. Wait, but the description says transform_to_csv transforms the query results into CSV. So perhaps the query_sales_data task returns the data, which is then processed by transform_to_csv to write the CSV. That would mean the data is passed via XCom, which is okay for small datasets.

Alternatively, maybe the query_sales_data writes the data to a file (like /tmp/raw_data.json), and transform_to_csv reads that file to create the CSV. But the problem states that the intermediate files are CSV and PDF, so maybe the raw data isn't stored, and the transform_to_csv task directly processes the query results.

I think for the purpose of this example, using XCom is acceptable, even though it's not suitable for very large data. The problem doesn't mention data size, so we'll proceed with XCom.

So, query_sales_data executes the SQL, fetches the data (say, as a list of tuples or a pandas DataFrame), and returns it. The transform_to_csv task takes that data, converts to CSV, saves to /tmp/sales_report.csv. Then generate_pdf_chart reads that CSV, generates the chart, saves as PDF. Finally, the email task sends both files.

Wait, but if generate_pdf_chart reads the CSV, then the CSV must be written by transform_to_csv. So transform_to_csv's function is to take the data from XCom and write to CSV. Then generate_pdf_chart's function is to read that CSV and create the PDF. That makes sense.

So, the code outline:

1. query_sales_data uses PostgresHook to get data, returns it.
2. transform_to_csv takes data from XCom, writes to CSV.
3. generate_pdf_chart reads CSV, creates PDF.
4. email_sales_report sends email with both attachments.

But wait, how does generate_pdf_chart get the CSV data? It reads from the file written by transform_to_csv. So in the generate_pdf_chart function, we'll have to read '/tmp/sales_report.csv' using pandas, then plot.

Similarly, the email task will attach '/tmp/sales_report.csv' and '/tmp/sales_chart.pdf'.

Now, writing the functions:

For query_sales_data:

def query_sales_data(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    sql = f"""
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = '{execution_date}'
        GROUP BY product;
    """
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    # Convert to list of dicts for easier handling
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row)) for row in results]
    cursor.close()
    conn.close()
    return data

But using pandas might be easier for transformation. Alternatively, return a DataFrame. But the function returns data that can be serialized by XCom. So maybe returning a list of dictionaries is better.

Then transform_to_csv:

def transform_to_csv(**context):
    import pandas as pd
    ti = context['ti']
    data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(data)
    df.to_csv('/tmp/sales_report.csv', index=False)

Generate_pdf_chart:

def generate_pdf_chart(**context):
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    import pandas as pd

    df = pd.read_csv('/tmp/sales_report.csv')
    
    with PdfPages('/tmp/sales_chart.pdf') as pdf:
        plt.figure(figsize=(10, 6))
        plt.bar(df['product'], df['total_sales'])
        plt.title('Sales by Product')
        plt.xlabel('Product')
        plt.ylabel('Total Sales')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

Email_sales_report:

def email_sales_report(**context):
    from airflow.providers.smtp.hooks.smtp import SmtpHook
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    import os

    smtp_hook = SmtpHook(smtp_conn_id='smtp_default')
    mail_from = 'noreply@company.com'
    mail_to = 'management@company.com'

    msg = MIMEMultipart()
    msg['Subject'] = f"Sales Report {context['execution_date'].strftime('%Y-%m-%d')}"
    msg['From'] = mail_from
    msg['To'] = mail_to

    body = "Please find attached the daily sales report."
    msg.attach(MIMEText(body))

    csv_path = '/tmp/sales_report.csv'
    pdf_path = '/tmp/sales_chart.pdf'

    for file_path in [csv_path, pdf_path]:
        with open(file_path, 'rb') as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

    smtp_hook.send_email(
        from_email=mail_from,
        to=mail_to,
        msg=msg.as_string(),
    )

But this function uses SmtpHook, which requires the SMTP connection to be set up in Airflow with conn_id 'smtp_default'. The user's pipeline description mentions SMTP service for report distribution, so this is appropriate.

Now, putting it all together in the DAG:

The DAG is named 'daily_sales_reporting', schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False.

Default_args includes 'email_on_failure': True, 'email': 'admin@company.com' (assuming this is the email to notify on failure), 'retries': 2, 'retry_delay': timedelta(minutes=5), and other standard parameters.

Each task is a PythonOperator with the respective function.

Dependencies are linear: query_sales_data >> transform_to_csv >> generate_pdf_chart >> email_sales_report.

Potential issues to check:

- Are the file paths correct? Using '/tmp/' might require that Airflow has write access, which is usually the case, but in a real setup, maybe a different directory is better. However, the problem specifies '/tmp/', so proceed.

- The SQL query uses the execution date directly. Should be careful about SQL injection, but since the date is controlled by Airflow, it's acceptable here. Alternatively, use parameters, but with PostgresHook, maybe using the .get_records() method with parameters would be better. Let me adjust the query_sales_data function to use parameters:

sql = """
    SELECT product, SUM(amount) as total_sales
    FROM sales
    WHERE sale_date = %s
    GROUP BY product;
"""
execution_date = context['execution_date'].strftime('%Y-%m-%d')
results = hook.get_records(sql, parameters=(execution_date,))

But using hook.get_records would return a list of tuples, which would need to be converted to a list of dicts. Alternatively, use hook.get_pandas_df to get a DataFrame directly.

Wait, PostgresHook has a get_pandas_df method that executes SQL and returns a DataFrame. That could simplify the code.

Rewriting query_sales_data:

def query_sales_data(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    sql = """
        SELECT product, SUM(amount) as total_sales
        FROM sales
        WHERE sale_date = %s
        GROUP BY product;
    """
    df = hook.get_pandas_df(sql, parameters=(execution_date,))
    return df.to_dict('records')

This way, the data is returned as a list of dicts, which can be easily converted back to a DataFrame in transform_to_csv.

Then, transform_to_csv:

def transform_to_csv(**context):
    import pandas as pd
    ti = context['ti']
    data = ti.xcom_pull(task_ids='query_sales_data')
    df = pd.DataFrame(data)
    df.to_csv('/tmp/sales_report.csv', index=False)

This should work.

For generate_pdf_chart, reading the CSV is okay, but since transform_to_csv writes it, the file should exist.

Testing the code for any syntax errors. Also, ensure that all necessary imports are included.

The DAG code would look like this:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['admin@company.com']
}

with DAG(
    dag_id='daily_sales_reporting',
    default_args=default_args,
    description='Daily sales reporting pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'reporting'],
) as dag:

    def query_sales_data(**context):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        sql = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE sale_date = %s
            GROUP BY product;
        """
        df = hook.get_pandas_df(sql, parameters=(execution_date,))
        return df.to_dict('records')

    def transform_to_csv(**context):
        ti = context['ti']
        data = ti.xcom_pull(task_ids='query_sales_data')
        df = pd.DataFrame(data)
        df.to_csv('/tmp/sales_report.csv', index=False)

    def generate_pdf_chart(**context):
        df = pd.read_csv('/tmp/sales_report.csv')
        
        with PdfPages('/tmp/sales_chart.pdf') as pdf:
            plt.figure(figsize=(10, 6))
            plt.bar(df['product'], df['total_sales'])
            plt.title('Sales by Product')
            plt.xlabel('Product')
            plt.ylabel('Total Sales')
            plt.xticks(rotation=45)
            plt.tight_layout()
            pdf.savefig()
            plt.close()

    def email_sales_report(**context):
        smtp_hook = SmtpHook(smtp_conn_id='smtp_default')
        mail_from = 'noreply@company.com'
        mail_to = 'management@company.com'

        msg = MIMEMultipart()
        msg['Subject'] = f"Sales Report {context['execution_date'].strftime('%Y-%m-%d')}"
        msg['From'] = mail_from
        msg['To'] = mail_to

        body = "Please find attached the daily sales report."
        msg.attach(MIMEText(body))

        csv_path = '/tmp/sales_report.csv'
        pdf_path = '/tmp/sales_chart.pdf'

        for file_path in [csv_path, pdf_path]:
            with open(file_path, 'rb') as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)

        smtp_hook.send_email(
            from_email=mail_from,
            to=mail_to,
            msg=msg.as_string(),
        )

    query_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data,
    )

    transform_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv,
    )

    pdf_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart,
    )

    email_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report,
    )

    query_task >> transform_task >> pdf_task >> email_task

Wait, but some imports are inside the functions. For example, PostgresHook is imported inside query_sales_data. That's okay, but according to PEP 8, imports should be at the top of the module unless avoiding circular imports. However, in Airflow DAG files, sometimes operators are imported at the top, but functions may have their own imports if needed. However, in this case, the code may have issues if the worker doesn't have the necessary libraries. But the problem states that the code should assume required packages are installed.

But the user's constraints mention that the code must be executable. So, all necessary imports must be present. For example, pandas and matplotlib are used, so they need to be imported. However, in the code above, pandas is imported at the top, but matplotlib's pyplot and PdfPages are imported inside generate_pdf_chart. That's not allowed; all imports must be at the top. So I need to adjust that.

So, correct the imports:

At the top of the file:

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

Then, in generate_pdf_chart, remove the imports inside the function.

Similarly, for email-related imports:

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

These should be at the top.

So the corrected code has all necessary imports at the top.

Now, checking the code again:

The functions query_sales_data, transform_to_csv, etc., are defined inside the DAG context manager. That's acceptable in Airflow, as the DAG file is parsed periodically, and the functions are part of the DAG's definition.

The default_args include 'email_on_failure': True and 'email': ['admin@company.com'], which will send emails to admin@company.com on task failures.

Each task is a PythonOperator with the respective callable.

Dependencies are set with >> operators.

Potential issues:

- The SMTP connection 'smtp_default' must be configured in Airflow with the correct SMTP server details.

- The PostgreSQL connection 'postgres_default' must be set up.

- The /tmp/ directory must be writable by the Airflow worker.

- The email_sales_report function uses 'noreply@company.com' as the from address, which should be adjusted as per the organization's setup.

But the code is complete and meets the problem's requirements.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['admin@company.com']
}

with DAG(
    dag_id='daily_sales_reporting',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'reporting']
) as dag:

    def query_sales_data(**context):
        """Query daily sales data from PostgreSQL database"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        sql = """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE sale_date = %s
            GROUP BY product;
        """
        df = hook.get_pandas_df(sql, parameters=(execution_date,))
        return df.to_dict('records')

    def transform_to_csv(**context):
        """Convert sales data to CSV format"""
        ti = context['ti']
        data = ti.xcom_pull(task_ids='query_sales_data')
        df = pd.DataFrame(data)
        df.to_csv('/tmp/sales_report.csv', index=False)

    def generate_pdf_chart(**context):
        """Generate PDF chart from sales data"""
        df = pd.read_csv('/tmp/sales_report.csv')
        
        with PdfPages('/tmp/sales_chart.pdf') as pdf:
            plt.figure(figsize=(10, 6))
            plt.bar(df['product'], df['total_sales'])
            plt.title(f"Sales Report {context['execution_date'].strftime('%Y-%m-%d')}")
            plt.xlabel('Product')
            plt.ylabel('Total Sales')
            plt.xticks(rotation=45)
            plt.tight_layout()
            pdf.savefig()
            plt.close()

    def email_sales_report(**context):
        """Send email with sales report attachments"""
        smtp_hook = SmtpHook(smtp_conn_id='smtp_default')
        msg = MIMEMultipart()
        msg['Subject'] = f"Sales Report {context['execution_date'].strftime('%Y-%m-%d')}"
        msg['From'] = 'noreply@company.com'
        msg['To'] = 'management@company.com'

        body = MIMEText('Please find attached the daily sales report.')
        msg.attach(body)

        for filename in ['sales_report.csv', 'sales_chart.pdf']:
            path = f'/tmp/{filename}'
            with open(path, 'rb') as f:
                part = MIMEApplication(f.read(), Name=filename)
                part['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(part)

        smtp_hook.send_email(
            from_email=msg['From'],
            to=msg['To'],
            subject=msg['Subject'],
            html_content=msg.as_string()
        )

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

    email_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report
    )

    query_task >> csv_task >> pdf_task >> email_task