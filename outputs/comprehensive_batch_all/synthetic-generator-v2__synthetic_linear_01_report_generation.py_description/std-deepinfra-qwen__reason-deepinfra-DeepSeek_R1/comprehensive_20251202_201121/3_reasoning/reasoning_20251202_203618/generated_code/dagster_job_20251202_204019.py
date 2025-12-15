<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is linear with four steps: querying sales data, transforming to CSV, generating a PDF chart, and emailing the report. Each step depends on the previous one, so the dependencies are straightforward.

First, I'll create each step as a Dagster op using the @op decorator. The ops are query_sales_data, transform_to_csv, generate_pdf_chart, and email_sales_report. Since the flow is linear, each op will take the output from the previous one as an input, except the first one which starts the chain.

For query_sales_data, it needs to connect to PostgreSQL. I'll use psycopg2 to execute the query. The config should include the database connection details. I'll add a config_schema for the op using Dagster's ConfigMapping. Wait, maybe using a resource for the database connection would be better. But the user mentioned to provide simplified resources or config as comments or stubs. So perhaps I'll use a config for the connection parameters. Alternatively, maybe the PostgreSQL connection is handled via a resource. The problem states that the data source is PostgreSQL with postgres_default connection. So maybe I should create a postgres_resource. But the user wants minimal stubs. Let me include a resource for PostgreSQL and SMTP as stubs.

Wait, the constraints say if resources/config are needed, provide simplified examples as comments or minimal stubs. So perhaps I should define a postgres_resource and an smtp_resource as resources. For example, using psycopg2 to connect, and smtplib for email. But in the code, resources are passed to ops via context. So each op that needs these resources would require them in their configuration.

Alternatively, maybe the database connection details are passed via config. Let me check the original pipeline steps. The query_sales_data op executes a PostgreSQL query. So the op needs to connect to the database. So perhaps the op's config includes the connection parameters. But storing credentials in config isn't best practice, but for simplicity, maybe that's acceptable here.

Alternatively, using a resource for the PostgreSQL connection. Let me define a postgres_resource that provides a connection. Similarly, an smtp_resource for the email. Then, the ops can require these resources via context.

So, in the code, I'll define the resources and then have the ops use them. For example, the query_sales_data op would use the postgres_resource to get a connection. The email_sales_report op would use the smtp_resource.

But the user's constraints mention to provide simplified resource or config examples as comments or minimal stubs. So perhaps I should create stub resources. Let me proceed.

First, define the resources. For PostgreSQL:

def postgres_resource(_):
    # In a real scenario, return a psycopg2 connection
    pass

Similarly for SMTP:

def smtp_resource(_):
    # In real scenario, return smtplib SMTP connection
    pass

Then, in the job, include these resources.

But the user's code needs to be executable. So maybe using ConfigurableResource from Dagster's new resources system? Or perhaps using the legacy resource system. Since the user's example uses @op and @job, perhaps the resources are defined as such.

Wait, the code needs to be executable. So perhaps the resources are stubbed out. For example, the query_sales_data op might take a context parameter, and use context.resources.postgres to get the connection. But since the resources are stubs, maybe they just return a mock connection. Alternatively, perhaps the code uses a context's config to get the connection parameters.

Alternatively, maybe the PostgreSQL connection is handled via a configurable op. Let's see.

The query_sales_data op could be written to accept a config_schema that includes the database connection details. For example:

@op(config_schema={
    "db_host": str,
    "db_name": str,
    "db_user": str,
    "db_password": str,
})
def query_sales_data(context):
    # Use context.op_config to get the details
    conn = psycopg2.connect(
        host=context.op_config["db_host"],
        database=context.op_config["db_name"],
        user=context.op_config["db_user"],
        password=context.op_config["db_password"],
    )
    # Execute query...

But including all that might complicate the code. However, the user wants the code to be executable. So perhaps the code should include the necessary imports, like psycopg2, pandas, matplotlib, etc. But the user might not have those installed. Wait, the constraints say "assuming required packages are installed". So the code can include those imports.

So, let's proceed step by step.

First, the imports:

from dagster import op, job, ResourceDefinition, get_dagster_logger
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
import os

But maybe the user wants minimal code. So perhaps the resources are stubbed, and the actual connection handling is simplified. For example, in query_sales_data, perhaps we just mock the connection for the sake of the example. But the code needs to be executable. Hmm.

Alternatively, perhaps the PostgreSQL connection is handled via a context resource. Let's define a postgres_resource as a ResourceDefinition. Similarly for SMTP.

But the code needs to be executable. Let's try to make it as real as possible, but with placeholders where credentials would be.

For the query_sales_data op:

@op(required_resource_keys={"postgres"})
def query_sales_data(context):
    logger = get_dagster_logger()
    # Execute query
    with context.resources.postgres.cursor() as cursor:
        cursor.execute("""
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product
        """, (context.run_config.get("execution_date"),))  # Need to handle execution date?
    results = cursor.fetchall()
    logger.info(f"Fetched {len(results)} sales records")
    return results

Wait, the execution date comes from the schedule. How to pass that? The schedule is daily, starting Jan 1, 2024. The execution date would be passed via the run config. So perhaps the op's config includes the execution_date as a parameter. Alternatively, the job is configured with a date.

But in the job definition, how to pass the execution date? Maybe via the run config. So the job's config would have an execution_date field.

But the user's constraints mention that if resources/config are needed, provide simplified examples. So perhaps the query_sales_data op's config includes the execution_date. Let's adjust:

In the query_sales_data op, the config_schema includes execution_date as a string.

Wait, but the user's pipeline description says the query is for the execution date. So the execution date is part of the job's configuration. Therefore, the job's config should include the execution_date. Then, each op that needs it can access it via context.op_config.

Wait, but the job's config is different from the op's config. Alternatively, the execution date is part of the job's run config, and passed to the ops that need it via their config.

Alternatively, perhaps the execution date is handled via Dagster's partitioning, but the user's description mentions a daily schedule with start date and catchup disabled. But the code example may not need to handle partitioning if it's simplified.

Alternatively, the execution date is passed as a run config parameter. For example, when the job is executed, it's provided with a config that includes the execution_date.

So, the query_sales_data op would have a config_schema that expects execution_date. Let's define that.

But perhaps the job is scheduled, and the schedule passes the execution date as a parameter. So in the job's config, we can have:

But the user's code needs to be a single module. So perhaps the schedule is not part of the code, but the job is set up to accept an execution_date via config.

So, modifying the query_sales_data op to accept execution_date as part of its config.

So:

@op(config_schema={"execution_date": str})
def query_sales_data(context):
    execution_date = context.op_config["execution_date"]
    # Connect to PostgreSQL using some method...

But how to handle the PostgreSQL connection? Let's use a resource for that. So the op requires the postgres resource.

So, the op would have required_resource_keys={"postgres"}, and the resource provides the connection.

But the resource needs to be defined. Let's create a postgres_resource as a ResourceDefinition.

postgres_resource = ResourceDefinition.mock_resource()

Wait, but in practice, the resource would be configured with connection details. So perhaps:

from dagster import resource

@resource(config_schema={
    "host": str,
    "dbname": str,
    "user": str,
    "password": str,
})
def postgres_resource(context):
    return psycopg2.connect(
        host=context.resource_config["host"],
        dbname=context.resource_config["dbname"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
    )

Similarly for SMTP:

@resource(config_schema={
    "host": str,
    "port": int,
    "username": str,
    "password": str,
})
def smtp_resource(context):
    return smtplib.SMTP(
        host=context.resource_config["host"],
        port=context.resource_config["port"],
    )

But then, in the job, these resources are configured with the necessary parameters.

But the user's code needs to be executable. So perhaps the resources are stubbed with placeholder configs. But for the code to run, perhaps the resources are mocked or use default values.

Alternatively, perhaps the code uses the default resources without full configuration, but that might not be executable. Hmm.

Alternatively, perhaps the PostgreSQL connection is handled via a context manager within the op, using hard-coded credentials for the sake of example. But that's not best practice, but the user wants the code to be executable.

Alternatively, the code could use environment variables for credentials, but that complicates things. The user's constraints say to provide minimal stubs.

Perhaps, for simplicity, the code will include the resource definitions with config schemas, but not fully implement them, assuming that the user would provide the actual configuration when running the job.

But the code needs to be executable. So perhaps the resources are defined, and the job includes them with sample configurations.

Wait, but in the job definition, resources are specified with their configuration. So, for example:

@job(
    resource_defs={
        "postgres": postgres_resource.configured({
            "host": "localhost",
            "dbname": "sales",
            "user": "user",
            "password": "password",
        }),
        "smtp": smtp_resource.configured({
            "host": "smtp.example.com",
            "port": 587,
            "username": "user@example.com",
            "password": "password",
        }),
    }
)
def sales_report_job():
    data = query_sales_data()
    csv_path = transform_to_csv(data)
    pdf_path = generate_pdf_chart(data)
    email_sales_report(csv_path, pdf_path)

But then, when executing the job, it would use these configured resources. However, if the user doesn't have a PostgreSQL server running, the code would fail. But the constraints say to assume required packages are installed, but not that the external systems are available. So perhaps the code is correct, but when run, it would fail unless the resources are properly configured.

But the user wants the code to be executable. So perhaps the code includes the necessary try-except blocks or mocks to allow it to run without actual connections. But that's complicating.

Alternatively, perhaps the code uses print statements instead of actual database operations, but that's not aligned with the problem statement.

Hmm. This is a bit tricky. Let's proceed step by step.

First, define the resources:

- postgres_resource: connects to PostgreSQL using psycopg2.
- smtp_resource: connects to SMTP using smtplib.

Then, define the ops with required_resource_keys.

The query_sales_data op requires the postgres resource and the execution_date from config.

The email_sales_report op requires the smtp resource.

Now, writing the code:

First, the imports:

import os
from dagster import op, job, ResourceDefinition, get_dagster_logger
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib

Then, define the resources:

@resource(config_schema={
    "host": str,
    "dbname": str,
    "user": str,
    "password": str,
})
def postgres_resource(context):
    return psycopg2.connect(
        host=context.resource_config["host"],
        dbname=context.resource_config["dbname"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
    )

@resource(config_schema={
    "host": str,
    "port": int,
    "username": str,
    "password": str,
})
def smtp_resource(context):
    smtp = smtplib.SMTP(context.resource_config["host"], context.resource_config["port"])
    smtp.starttls()
    smtp.login(context.resource_config["username"], context.resource_config["password"])
    return smtp

Then, the ops:

@op(required_resource_keys={"postgres"}, config_schema={"execution_date": str})
def query_sales_data(context):
    logger = get_dagster_logger()
    execution_date = context.op_config["execution_date"]
    conn = context.resources.postgres
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product
            """,
            (execution_date,)
        )
        results = cursor.fetchall()
    logger.info(f"Retrieved {len(results)} sales records for {execution_date}")
    return results

@op
def transform_to_csv(context, sales_data):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"CSV report generated at {csv_path}")
    return csv_path

@op
def generate_pdf_chart(context, sales_data):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    plt.figure()
    df.plot.bar(x="product", y="total_sales")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info(f"PDF chart generated at {pdf_path}")
    return pdf_path

@op(required_resource_keys={"smtp"})
def email_sales_report(context, csv_path, pdf_path):
    logger = get_dagster_logger()
    smtp = context.resources.smtp
    msg = MIMEMultipart()
    msg["From"] = "reports@company.com"
    msg["To"] = "management@company.com"
    msg["Subject"] = "Daily Sales Report"

    body = "Please find attached the daily sales report."
    msg.attach(MIMEText(body, "plain"))

    for file_path in [csv_path, pdf_path]:
        with open(file_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(file_path)}"'
            msg.attach(part)

    smtp.send_message(msg)
    logger.info("Sales report email sent")

Then, define the job with resources configured:

@job(
    resource_defs={
        "postgres": postgres_resource.configured({
            "host": "localhost",
            "dbname": "sales_db",
            "user": "user",
            "password": "password",
        }),
        "smtp": smtp_resource.configured({
            "host": "smtp.example.com",
            "port": 587,
            "username": "user@example.com",
            "password": "password",
        }),
    }
)
def sales_report_job():
    sales_data = query_sales_data()
    csv_file = transform_to_csv(sales_data)
    pdf_file = generate_pdf_chart(sales_data)
    email_sales_report(csv_file, pdf_file)

Finally, the main block:

if __name__ == "__main__":
    result = sales_report_job.execute_in_process(
        run_config={
            "ops": {
                "query_sales_data": {
                    "config": {
                        "execution_date": "2024-01-01"
                    }
                }
            }
        }
    )

But wait, the generate_pdf_chart uses matplotlib, which might need a non-interactive backend. So perhaps adding plt.switch_backend('Agg') to avoid GUI issues.

So, adding:

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

But the order matters. So adjusting the imports:

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

Putting it all together, the code would look like this. Also, ensure that the resources are properly imported and defined.

But the user's constraints mention to avoid placeholders. So the code should have realistic resource configurations, even if they are example values.

Testing for PEP 8 compliance and ensuring that all ops are connected in the job.

The email_sales_report op takes two inputs: csv_path and pdf_path. So in the job definition, the email_sales_report is called with both outputs from transform_to_csv and generate_pdf_chart.

Yes, the dependencies are linear: query_sales_data → transform_to_csv and generate_pdf_chart both depend on query_sales_data, then email depends on both. Wait, no. The original pipeline steps are strictly linear: each task depends on the previous. So the steps are:

query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report

Wait, no. According to the pipeline description, the steps are:

1. query_sales_data
2. transform_to_csv (depends on 1)
3. generate_pdf_chart (depends on 1?)
4. email_sales_report (depends on 2 and 3)

Wait, the original description says:

"Execution Order: Strict linear sequence with each task depending on the successful completion of its predecessor."

Wait, that's conflicting. If it's strictly linear, then each step depends on the previous. So:

query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report.

But transform_to_csv and generate_pdf_chart both take the output of query_sales_data. So they can run in parallel if possible. But the description says no parallelization. So perhaps the steps are sequential. Wait, the pipeline is linear with no parallelization. So the steps must be executed in sequence. But transform_to_csv and generate_pdf_chart both depend on query_sales_data. So they can't run in parallel. Therefore, the correct order is:

query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report.

But that doesn't make sense because generate_pdf_chart doesn't depend on transform_to_csv. Unless the pipeline is designed such that each step must wait for the previous, even if not data-dependent.

But according to the pipeline description, it's a linear sequence where each task depends on the previous. So the dependencies are:

query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report.

But that would mean that generate_pdf_chart depends on transform_to_csv, which may not be necessary. However, the description says the execution order is strict linear sequence. So even if there's no data dependency between steps, each task must wait for the previous.

But in Dagster, dependencies are based on data flow. So to enforce execution order without data dependencies, you can structure the ops to pass a dummy value. But the user's pipeline says data flow is PostgreSQL → CSV → PDF → Email. So the CSV is generated from the query, then the PDF is generated from the query data (not the CSV), and the email sends both files. So the data flow is:

query_sales_data → transform_to_csv (creates CSV)
query_sales_data → generate_pdf_chart (creates PDF)
transform_to_csv and generate_pdf_chart → email_sales_report (sends both files)

But according to the pipeline's execution order, it's strictly linear. So perhaps the user intended that each step runs after the previous, even if there's no data dependency. That would require a different structure, perhaps using non-data dependencies. But in Dagster, ops only run when their inputs are available. So to enforce order without data dependencies, you can have each op output a value that the next op takes as input, even if it's not used.

But the pipeline description says the data flow is PostgreSQL → CSV → PDF → Email. That doesn't make sense because the PDF is generated from the same data as the CSV, not from the CSV file. So perhaps the correct data dependencies are:

query_sales_data → transform_to_csv (outputs CSV path)
query_sales_data → generate_pdf_chart (outputs PDF path)
transform_to_csv and generate_pdf_chart → email_sales_report (takes both paths)

But according to the execution order being linear, each task depends on the previous. So perhaps the intended flow is:

query_sales_data runs first.

Then transform_to_csv runs after query_sales_data.

Then generate_pdf_chart runs after transform_to_csv.

Then email_sales_report runs after generate_pdf_chart.

But that would mean generate_pdf_chart depends on transform_to_csv, which isn't necessary. But the pipeline description says strict linear sequence. So perhaps the user wants each step to run sequentially, even if there's no data dependency. In that case, each op must take the output of the previous op, even if it doesn't use it.

For example:

query_sales_data → transform_to_csv (takes data, outputs CSV path)
transform_to_csv → generate_pdf_chart (takes CSV path, outputs PDF path)
generate_pdf_chart → email_sales_report (takes PDF path, and also needs CSV path)

But that's not possible unless the generate_pdf_chart also passes along the CSV path. Alternatively, use a tuple to pass both paths.

Alternatively, use a dummy parameter. For example:

def transform_to_csv(context, sales_data):
    # process and output CSV path
    return csv_path

def generate_pdf_chart(context, csv_path):
    # but actually uses sales_data from query_sales_data
    # but how to get sales_data?

This approach won't work because generate_pdf_chart needs the sales_data, not the CSV path.

So this suggests that the initial understanding was incorrect. The correct data flow is:

query_sales_data (outputs sales_data)

transform_to_csv (takes sales_data, outputs csv_path)

generate_pdf_chart (takes sales_data, outputs pdf_path)

email_sales_report (takes csv_path and pdf_path)

So the dependencies are:

sales_data → transform_to_csv → csv_path

sales_data → generate_pdf_chart → pdf_path

csv_path and pdf_path → email_sales_report

But in Dagster, this would allow transform_to_csv and generate_pdf_chart to run in parallel after query_sales_data. However, the pipeline description says there's no parallelization; it's strictly linear. So the user wants transform_to_csv to run after query_sales_data, then generate_pdf_chart after transform_to_csv, then email after generate_pdf_chart.

But that would require generate_pdf_chart to depend on transform_to_csv, even though it doesn't use its output. To enforce this, generate_pdf_chart would take the csv_path as an input but not use it. Then, email_sales_report would take the pdf_path and also need the csv_path from transform_to_csv.

But how to structure that? For example:

@op
def generate_pdf_chart(context, sales_data, csv_path):
    # generate PDF using sales_data, ignore csv_path
    return pdf_path

But then generate_pdf_chart depends on both sales_data and csv_path. But sales_data is from query_sales_data, and csv_path is from transform_to_csv. So the execution order would be query_sales_data → transform_to_csv → generate_pdf_chart.

Then email_sales_report depends on generate_pdf_chart's pdf_path and transform_to_csv's csv_path.

But this way, the execution order is enforced as linear: each step runs after the previous.

So the job would be structured as:

def sales_report_job():
    sales_data = query_sales_data()
    csv_path = transform_to_csv(sales_data)
    pdf_path = generate_pdf_chart(sales_data, csv_path)
    email_sales_report(csv_path, pdf_path)

In this case, generate_pdf_chart takes both sales_data and csv_path, but only uses sales_data. The csv_path is passed to enforce the execution order.

This way, the steps run in sequence: query_sales_data → transform_to_csv → generate_pdf_chart → email_sales_report.

But this requires modifying generate_pdf_chart to accept csv_path as an input, even though it doesn't use it. This is a way to enforce the execution order in Dagster, as an op won't run until all its inputs are available.

So adjusting the generate_pdf_chart op:

@op
def generate_pdf_chart(context, sales_data, _csv_path):
    # _csv_path is not used, but required for dependency
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    plt.figure()
    df.plot.bar(x="product", y="total_sales")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info(f"PDF chart generated at {pdf_path}")
    return pdf_path

Then, in the job:

sales_data = query_sales_data()
csv_path = transform_to_csv(sales_data)
pdf_path = generate_pdf_chart(sales_data, csv_path)
email_sales_report(csv_path, pdf_path)

This way, generate_pdf_chart depends on both sales_data and csv_path, ensuring it runs after transform_to_csv.

Similarly, email_sales_report depends on both csv_path and pdf_path, ensuring it runs after generate_pdf_chart.

This enforces the linear execution order as per the pipeline description.

So incorporating these changes into the code.

Now, putting it all together:

The query_sales_data op requires the postgres resource and execution_date config.

The transform_to_csv op takes sales_data and outputs csv_path.

The generate_pdf_chart op takes sales_data and csv_path (even though it doesn't use csv_path), outputs pdf_path.

The email_sales_report op takes both paths and sends the email.

The job is defined with the resources configured.

The main block includes the execution_date in the run config.

Also, adding the matplotlib backend setting to avoid GUI issues.

So the complete code would be:

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dagster import op, job, resource, get_dagster_logger
import psycopg2
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib


@resource(config_schema={
    "host": str,
    "dbname": str,
    "user": str,
    "password": str,
})
def postgres_resource(context):
    return psycopg2.connect(
        host=context.resource_config["host"],
        dbname=context.resource_config["dbname"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
    )


@resource(config_schema={
    "host": str,
    "port": int,
    "username": str,
    "password": str,
})
def smtp_resource(context):
    smtp = smtplib.SMTP(context.resource_config["host"], context.resource_config["port"])
    smtp.starttls()
    smtp.login(context.resource_config["username"], context.resource_config["password"])
    return smtp


@op(required_resource_keys={"postgres"}, config_schema={"execution_date": str})
def query_sales_data(context):
    logger = get_dagster_logger()
    execution_date = context.op_config["execution_date"]
    conn = context.resources.postgres
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product
            """,
            (execution_date,)
        )
        results = cursor.fetchall()
    logger.info(f"Retrieved {len(results)} sales records for {execution_date}")
    return results


@op
def transform_to_csv(context, sales_data):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"CSV report generated at {csv_path}")
    return csv_path


@op
def generate_pdf_chart(context, sales_data, _csv_path):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    plt.figure()
    df.plot.bar(x="product", y="total_sales")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info(f"PDF chart generated at {pdf_path}")
    return pdf_path


@op(required_resource_keys={"smtp"})
def email_sales_report(context, csv_path, pdf_path):
    logger = get_dagster_logger()
    smtp = context.resources.smtp
    msg = MIMEMultipart()
    msg["From"] = "reports@company.com"
    msg["To"] = "management@company.com"
    msg["Subject"] = "Daily Sales Report"

    msg.attach(MIMEText("Please find attached the daily sales report.", "plain"))

    for path in [csv_path, pdf_path]:
        with open(path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
            msg.attach(part)

    smtp.send_message(msg)
    logger.info("Sales report email sent")


@job(
    resource_defs={
        "postgres": postgres_resource.configured({
            "host": "localhost",
            "dbname": "sales",
            "user": "postgres",
            "password": "postgres",
        }),
        "smtp": smtp_resource.configured({
            "host": "smtp.example.com",
            "port": 587,
            "username": "user",
            "password": "password",
        }),
    }
)
def sales_report_job():
    data = query_sales_data()
    csv_file = transform_to_csv(data)
    pdf_file = generate_pdf_chart(data, csv_file)
    email_sales_report(csv_file, pdf_file)


if __name__ == "__main__":
    result = sales_report_job.execute_in_process(
        run_config={
            "ops": {
                "query_sales_data": {
                    "config": {
                        "execution_date": "2024-01-01"
                    }
                }
            }
        }
    )

This code should be PEP 8 compliant, includes all necessary imports, resources, ops, and the job with dependencies. The resources are configured with example values, and the main block executes the job with a sample execution date. The generate_pdf_chart op takes both sales_data and csv_path to enforce the linear execution order as per the pipeline description.
</think>

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dagster import op, job, resource, get_dagster_logger
import psycopg2
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib


@resource(config_schema={
    "host": str,
    "dbname": str,
    "user": str,
    "password": str,
})
def postgres_resource(context):
    return psycopg2.connect(
        host=context.resource_config["host"],
        dbname=context.resource_config["dbname"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
    )


@resource(config_schema={
    "host": str,
    "port": int,
    "username": str,
    "password": str,
})
def smtp_resource(context):
    smtp = smtplib.SMTP(context.resource_config["host"], context.resource_config["port"])
    smtp.starttls()
    smtp.login(context.resource_config["username"], context.resource_config["password"])
    return smtp


@op(required_resource_keys={"postgres"}, config_schema={"execution_date": str})
def query_sales_data(context):
    logger = get_dagster_logger()
    execution_date = context.op_config["execution_date"]
    conn = context.resources.postgres
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT product, SUM(amount) as total_sales
            FROM sales
            WHERE date = %s
            GROUP BY product
            """,
            (execution_date,)
        )
        results = cursor.fetchall()
    logger.info(f"Retrieved {len(results)} sales records for {execution_date}")
    return results


@op
def transform_to_csv(context, sales_data):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"CSV report generated at {csv_path}")
    return csv_path


@op
def generate_pdf_chart(context, sales_data, _csv_path):
    logger = get_dagster_logger()
    df = pd.DataFrame(sales_data, columns=["product", "total_sales"])
    plt.figure()
    df.plot.bar(x="product", y="total_sales")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()
    logger.info(f"PDF chart generated at {pdf_path}")
    return pdf_path


@op(required_resource_keys={"smtp"})
def email_sales_report(context, csv_path, pdf_path):
    logger = get_dagster_logger()
    smtp = context.resources.smtp
    msg = MIMEMultipart()
    msg["From"] = "reports@company.com"
    msg["To"] = "management@company.com"
    msg["Subject"] = "Daily Sales Report"

    msg.attach(MIMEText("Please find attached the daily sales report.", "plain"))

    for path in [csv_path, pdf_path]:
        with open(path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(path))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'
            msg.attach(part)

    smtp.send_message(msg)
    logger.info("Sales report email sent")


@job(
    resource_defs={
        "postgres": postgres_resource.configured({
            "host": "localhost",
            "dbname": "sales",
            "user": "postgres",
            "password": "postgres",
        }),
        "smtp": smtp_resource.configured({
            "host": "smtp.example.com",
            "port": 587,
            "username": "user",
            "password": "password",
        }),
    }
)
def sales_report_j