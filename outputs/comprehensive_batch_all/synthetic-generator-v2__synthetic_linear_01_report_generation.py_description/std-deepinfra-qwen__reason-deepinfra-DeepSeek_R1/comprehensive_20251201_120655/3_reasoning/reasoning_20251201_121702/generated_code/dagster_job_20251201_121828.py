<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline is linear with four steps: querying sales data, transforming to CSV, generating a PDF chart, and emailing the report. Each step depends on the previous one, so the dependencies are straightforward.

First, I'll create each step as a Dagster op using the @op decorator. The job will tie these ops together in sequence. The user mentioned using resources for PostgreSQL and SMTP, so I should include stubs for these. Resources in Dagster are typically defined with @resource, but the user wants simplified examples as comments or minimal stubs. Maybe I can define a PostgresResource class and an SMTPResource class as placeholders.

For the query_sales_data op, it needs a PostgreSQL connection. I'll add a context parameter to access resources. The transform_to_csv op takes the data and writes a CSV. The generate_pdf_chart op uses matplotlib to create a chart and save it as PDF. The email_sales_report op sends the email with attachments.

The user also mentioned a daily schedule starting Jan 1, 2024, with catchup disabled. I'll include the schedule definition using ScheduleDefinition, specifying the cron schedule as "0 0 * * *" for daily. The retry policy is 2 retries with 5-minute delays, so I'll add that to the job decorator.

Failure handling via email notifications isn't directly addressed in the code, but maybe that's handled externally or via resources. Since the user said to provide minimal stubs, I'll leave that as a comment.

I need to make sure each op's output is passed to the next. The query_sales_data returns data which transform_to_csv uses. The CSV path is returned and passed to generate_pdf_chart, which returns the PDF path. Both paths are inputs to email_sales_report.

Testing the code structure: each op is defined, dependencies are set in the job, resources are stubbed, and the schedule is included. The main block executes the job in process.

Wait, the email_sales_report op needs both CSV and PDF paths. So in the job definition, after generate_pdf_chart, both the CSV and PDF paths should be available. But since it's a linear sequence, maybe the transform_to_csv returns the CSV path, then generate_pdf_chart takes the data (or maybe just the CSV path?) and generates the PDF. Hmm, the original description says the data flows from PostgreSQL to CSV to PDF. So maybe the data is passed through each step. Or perhaps each step writes to a file and passes the file path.

The problem states that the data flow is PostgreSQL → CSV file → PDF chart → Email attachments. So the CSV is created from the query results, then the PDF is generated from the same data? Or does the PDF use the CSV? The description says "transforms it into CSV and PDF chart formats", which might imply that both the CSV and PDF are generated from the query results. Wait, the steps are:

- query_sales_data: extracts data
- transform_to_csv: transforms to CSV
- generate_pdf_chart: creates PDF from the data
- email: sends both files.

So maybe the data from query_sales_data is passed to both transform_to_csv and generate_pdf_chart. But the steps are sequential, so perhaps transform_to_csv runs first, then generate_pdf_chart uses the same data. So the dependencies would be query -> transform -> generate -> email. But the data from query is needed by both transform and generate. Wait, no, the steps are linear: each task depends on the previous. So transform_to_csv depends on query, generate_pdf depends on transform, email depends on generate. But that doesn't make sense because generate_pdf would need the data from query, not from transform. Hmm, maybe the initial approach is incorrect.

Wait the pipeline description says "executes sequentially with no parallelization", so each step depends on the previous. So the data flow is query -> transform (creates CSV) -> generate (creates PDF from CSV?) -> email. But the description says the PDF is generated from the query results. So perhaps the data is passed through each step. Let me re-examine the steps:

- query_sales_data: extracts data (returns data)
- transform_to_csv: takes data, writes CSV, returns CSV path
- generate_pdf_chart: takes data, writes PDF, returns PDF path
- email: takes CSV path and PDF path.

But in a linear sequence, how does generate_pdf_chart get the data? Because transform_to_csv would have to pass the data along. So maybe transform_to_csv returns both the data and the CSV path? Or perhaps the data is stored in a file, and each subsequent step reads from the file. But the user's code example should pass data between ops.

Alternatively, maybe the data is passed from query to transform, which writes CSV and returns the data. Then generate_pdf uses the data to create the PDF. Then email uses the CSV and PDF paths. But the steps are linear, so dependencies are query -> transform -> generate -> email.

Wait, the original steps are:

1. query_sales_data
2. transform_to_csv (using query's output)
3. generate_pdf_chart (using query's output?)
4. email (using both CSV and PDF)

But if the dependencies are linear, each step depends on the previous. So transform depends on query, generate depends on transform, email depends on generate. But that would mean generate doesn't have access to the original data. So perhaps the data needs to be passed through each step.

Alternatively, maybe the data is processed in each step. For example:

- query returns data
- transform takes data, writes CSV, returns data (or CSV path)
- generate takes data (or CSV path), writes PDF, returns PDF path
- email takes CSV path and PDF path.

But in a linear dependency chain, each op's output is the input to the next. So transform's output (CSV path) is passed to generate, which then creates the PDF and passes its path to email. But how would generate_pdf create the PDF from the CSV? Or does it need the original data?

This is a bit ambiguous. The pipeline description says "transforms it into CSV and PDF chart formats", which suggests that both CSV and PDF are generated from the same data. So perhaps the steps should be:

query_sales_data -> data

transform_to_csv (data) -> csv_path

generate_pdf_chart (data) -> pdf_path

email (csv_path, pdf_path)

But in a linear sequence, how can generate_pdf run after transform_to_csv, but both depend on query? That would require parallel steps, but the pipeline is described as strictly linear with no parallelism. So maybe the user intended that the data is passed through each step. So:

query returns data

transform takes data, writes CSV, returns data (or csv_path)

generate takes data (or csv_path), writes PDF, returns pdf_path

email takes pdf_path and csv_path.

But in a linear dependency chain, each op depends on the previous. So:

transform depends on query

generate depends on transform

email depends on generate

But then email would only receive the pdf_path from generate. How does it get the csv_path? Unless each op passes along the necessary paths.

Alternatively, maybe each op returns both the data and the path, but that complicates things. Alternatively, the CSV and PDF are written to known paths (/tmp/sales_report.csv and /tmp/sales_chart.pdf), so the email op can reference those paths directly without needing them as inputs. But that's not ideal because it introduces hidden dependencies.

The problem states that the data flows from PostgreSQL to CSV to PDF to email. So perhaps the CSV is generated first, then the PDF is generated from the CSV, then both are emailed. But that would mean generate_pdf depends on the CSV, which is the output of transform_to_csv. Then email depends on both the CSV and PDF paths.

But in a linear sequence, each step depends on the previous. So:

query -> transform -> generate -> email

But email needs both CSV and PDF paths. So generate must pass both paths? Or perhaps the email op takes the CSV path from transform and the PDF path from generate. But in a linear chain, email can only receive the output from generate. So this suggests that the dependencies are not linear, but the problem states that the execution order is a strict linear sequence. This is conflicting.

Wait the pipeline description says "executes sequentially with no parallelization, branching, or conditional logic." So each task depends on the previous. Therefore, the email task can only depend on generate_pdf, which depends on transform_to_csv, which depends on query. Therefore, the email task can't directly access the CSV path from transform_to_csv unless it's passed through generate_pdf.

So perhaps the generate_pdf op takes the CSV path as input, generates the PDF, and returns both paths. Then email can take those. But that would require generate_pdf to have the CSV path as input, which would mean that transform_to_csv passes its output (CSV path) to generate_pdf, which then passes both to email.

But according to the pipeline steps, generate_pdf is supposed to create the PDF from the sales data, not from the CSV. So maybe the data is passed through each op. So:

query returns data

transform takes data, writes CSV, returns data and csv_path

generate takes data, writes PDF, returns data, csv_path, pdf_path

email takes csv_path and pdf_path

But in a linear sequence, each op's output is the next op's input. So:

@job
def sales_reporting_job():
    data = query_sales_data()
    csv_data = transform_to_csv(data)
    pdf_data = generate_pdf_chart(csv_data)
    email_sales_report(pdf_data)

But in this case, each op would need to pass along the necessary data. For example, transform_to_csv would return the csv_path, generate_pdf would take csv_path and return pdf_path, and email would take pdf_path. But then how does email get the csv_path? It seems like the email op needs both paths, but in a linear chain, it can only receive the output from generate_pdf.

This suggests that the initial approach is incorrect. Maybe the data from query is passed through all ops. So:

def query_sales_data(context):
    # get data
    return data

@op
def transform_to_csv(context, data):
    # write to CSV, get path
    return (data, csv_path)

@op
def generate_pdf_chart(context, data_csv):
    data, csv_path = data_csv
    # generate PDF, get pdf_path
    return (data, csv_path, pdf_path)

@op
def email_sales_report(context, data_pdf):
    data, csv_path, pdf_path = data_pdf
    # email both files

But this complicates the data passing. Alternatively, each op could write to a known path, and the email op reads both paths. But that's not ideal as it's less explicit.

Alternatively, perhaps the email op doesn't need the CSV and PDF paths as inputs, but instead reads them from the file system. But the problem states that data flows through the tasks, so inputs and outputs should be explicit.

Hmm. The user's pipeline steps are:

1. query_sales_data: outputs data
2. transform_to_csv: takes data, outputs csv_path
3. generate_pdf_chart: takes data, outputs pdf_path
4. email_sales_report: takes csv_path and pdf_path

But in a linear sequence, each op depends on the previous. So transform depends on query, generate depends on transform, email depends on generate. Therefore, generate can't get the data from query unless it's passed through transform.

So perhaps transform_to_csv takes the data, writes CSV, returns the data and csv_path. Then generate_pdf takes that, writes PDF, returns data, csv_path, pdf_path. Then email takes that and uses csv_path and pdf_path.

But this requires each subsequent op to pass along the data and paths. That's possible but may not be efficient. However, for the sake of the example, it's manageable.

Alternatively, maybe the data is not needed after the CSV is generated. The generate_pdf could read the CSV file. But the problem states that the PDF is generated from the query results, implying that it uses the same data as the CSV.

This is a bit confusing. To proceed, I'll assume that each op passes along the necessary data. So:

- query_sales_data returns the data.
- transform_to_csv takes the data, writes CSV, returns the csv_path.
- generate_pdf_chart takes the data (from query), writes PDF, returns pdf_path.
- email_sales_report takes both csv_path and pdf_path.

But in a linear sequence, generate_pdf can't receive the data from query unless it's passed through transform_to_csv. So transform_to_csv would need to return both the csv_path and the data. Then generate_pdf takes that, uses the data to generate PDF, returns pdf_path. Then email takes both csv_path and pdf_path.

So the code would look like:

@op
def query_sales_data(context):
    # query data
    return data

@op
def transform_to_csv(context, data):
    # write to CSV, return (data, csv_path)
    return csv_path

Wait, but if transform_to_csv returns only the csv_path, then generate_pdf_chart would need the data from query_sales_data. But in a linear sequence, generate_pdf depends on transform_to_csv, so it can't directly access the output of query_sales_data. Therefore, the data must be passed through transform_to_csv to generate_pdf.

So transform_to_csv would take the data, write CSV, and return the data along with the csv_path. Then generate_pdf can use the data to create the PDF.

But that complicates the return values. Alternatively, perhaps the data is stored in a file by transform_to_csv, and generate_pdf reads it from there. But the problem states that the data flows through the tasks.

Alternatively, maybe the generate_pdf op can take the csv_path as input, read the CSV, and generate the PDF from that. But the original description says the PDF is generated from the query data, not the CSV. However, for the sake of the example, perhaps it's acceptable.

Given the ambiguity, I'll proceed with the following approach:

- query_sales_data returns the data.
- transform_to_csv takes the data, writes CSV, returns csv_path.
- generate_pdf_chart takes the data (from query), writes PDF, returns pdf_path.
- email_sales_report takes both csv_path and pdf_path.

But in a linear sequence, generate_pdf can't directly receive the data from query unless it's passed through transform_to_csv. Therefore, transform_to_csv must pass along the data along with the csv_path.

So:

@op
def transform_to_csv(context, data):
    # write data to CSV, get csv_path
    context.log.info("Writing CSV to /tmp/sales_report.csv")
    return {"data": data, "csv_path": "/tmp/sales_report.csv"}

Then generate_pdf_chart can take this output, extract the data, generate the PDF, and return the pdf_path along with the csv_path.

But this requires each op to pass along the necessary information. So:

@op
def generate_pdf_chart(context, transformed_data):
    data = transformed_data["data"]
    csv_path = transformed_data["csv_path"]
    # generate PDF, get pdf_path
    pdf_path = "/tmp/sales_chart.pdf"
    return {"csv_path": csv_path, "pdf_path": pdf_path}

Then email_sales_report takes this output and uses both paths.

But this way, each op is passing along the necessary data. However, this may not be the most efficient, but it satisfies the linear dependency requirement.

Alternatively, perhaps the data is not needed after the CSV is generated, and the PDF is generated from the CSV. In that case, generate_pdf_chart would take the csv_path as input, read the CSV, and generate the PDF. Then the code would be:

@op
def query_sales_data(context):
    # query data
    return data

@op
def transform_to_csv(context, data):
    # write to CSV
    csv_path = "/tmp/sales_report.csv"
    return csv_path

@op
def generate_pdf_chart(context, csv_path):
    # read CSV and generate PDF
    pdf_path = "/tmp/sales_chart.pdf"
    return pdf_path

@op
def email_sales_report(context, csv_path, pdf_path):
    # send email with both paths

But in this case, generate_pdf depends on transform_to_csv (receives csv_path), and email depends on generate_pdf (pdf_path) and transform_to_csv (csv_path). However, in a linear sequence, email can't have two dependencies. The pipeline description says strict linear sequence, so each task depends on the previous. Therefore, email can only depend on generate_pdf, which depends on transform_to_csv, which depends on query.

So how does email get the csv_path? It must be passed through generate_pdf. So generate_pdf would take csv_path, generate pdf_path, and return both. Then email takes the output of generate_pdf, which includes both paths.

So:

@op
def generate_pdf_chart(context, csv_path):
    # generate PDF from CSV data
    pdf_path = "/tmp/sales_chart.pdf"
    return {"csv_path": csv_path, "pdf_path": pdf_path}

@op
def email_sales_report(context, paths):
    csv_path = paths["csv_path"]
    pdf_path = paths["pdf_path"]
    # send email

Then the job definition would be:

@job
def sales_reporting_job():
    data = query_sales_data()
    csv_path = transform_to_csv(data)
    paths = generate_pdf_chart(csv_path)
    email_sales_report(paths)

This way, each step depends on the previous, forming a linear chain. The email op receives both paths from the generate_pdf step.

This seems to fit the pipeline description. Now, let's code this.

For the query_sales_data op, it uses a PostgreSQL resource. So I'll create a PostgresResource class as a stub. The op uses context.resources.postgres to get the connection.

The transform_to_csv op uses pandas to write the CSV. The generate_pdf_chart uses matplotlib to create a bar chart and save as PDF.

The email_sales_report uses smtp_resource to send the email.

Now, including resources in the job. The job should have a resource_defs parameter. But in the code, resources are usually provided via a configuration. However, the user wants minimal stubs, so I'll define the resources as classes and include them in the job.

Putting it all together:

- Define PostgresResource and SMTPResource as stubs.
- Use them in the ops via context.resources.
- Set up the job with resource definitions.
- Add the schedule with the daily interval.

The retry policy is added to the job decorator: @job(retry_policy=RetryPolicy(max_retries=2, delay=300)).

Testing the code structure:

- All ops are defined with proper parameters.
- The job links them in sequence.
- Resources are stubbed.
- Schedule is defined.

I think this covers all the requirements. Now, writing the code.
</think>

from dagster import op, job, ScheduleDefinition, RetryPolicy, resource

# --- Resources ---
class PostgresResource:
    def execute_query(self, query):
        # Stub for PostgreSQL connection
        return [{"product": "A", "sales": 100}, {"product": "B", "sales": 200}]

class SMTPResource:
    def send_email(self, to, subject, body, attachments):
        # Stub for email sending
        print(f"Email to {to} with attachments {attachments}")

# --- Ops ---
@op(required_resource_keys={"postgres"})
def query_sales_data(context):
    """Query daily sales data from PostgreSQL database"""
    query = "SELECT product, SUM(amount) as sales FROM sales WHERE date = %s GROUP BY product"
    context.log.info(f"Executing query: {query}")
    return context.resources.postgres.execute_query(query)

@op
def transform_to_csv(context, data):
    """Transform sales data to CSV file"""
    import pandas as pd
    df = pd.DataFrame(data)
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    context.log.info(f"CSV saved to {csv_path}")
    return csv_path

@op
def generate_pdf_chart(context, csv_path):
    """Generate PDF chart from sales data"""
    import matplotlib.pyplot as plt
    import pandas as pd
    
    df = pd.read_csv(csv_path)
    plt.figure(figsize=(10,6))
    plt.bar(df['product'], df['sales'])
    plt.title("Daily Sales by Product")
    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path)
    plt.close()
    context.log.info(f"PDF saved to {pdf_path}")
    return {"csv_path": csv_path, "pdf_path": pdf_path}

@op(required_resource_keys={"smtp"})
def email_sales_report(context, paths):
    """Send email with sales report attachments"""
    body = "Daily sales report attached"
    context.resources.smtp.send_email(
        to="management@company.com",
        subject="Daily Sales Report",
        body=body,
        attachments=[paths["csv_path"], paths["pdf_path"]]
    )
    context.log.info("Sales report email sent")

# --- Job Definition ---
@job(
    resource_defs={
        "postgres": PostgresResource(),
        "smtp": SMTPResource()
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def sales_reporting_job():
    data = query_sales_data()
    csv_path = transform_to_csv(data)
    paths = generate_pdf_chart(csv_path)
    email_sales_report(paths)

# --- Schedule ---
daily_schedule = ScheduleDefinition(
    job=sales_reporting_job,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC"
)

if __name__ == "__main__":
    result = sales_reporting_job.execute_in_process()