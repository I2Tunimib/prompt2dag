<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three stages: Extract, Transform, Load. 

First, the Extract stage has three parallel ops: ingest_vendor_a, ingest_vendor_b, and ingest_vendor_c. These should be defined as separate @op-decorated functions. Since they run in parallel, their outputs will all be dependencies for the next stage.

Next, the Transform stage has a single cleanse_data op that depends on all three extract ops. So in Dagster, the cleanse_data op will take the outputs of the three ingest ops as inputs. But wait, each ingest op returns a DataFrame, so cleanse_data needs to accept all three. The description says it processes all three datasets together, so maybe the inputs are vendor_a_df, vendor_b_df, vendor_c_df, and then the function concatenates them or processes them together.

Then the Load stage has two ops: load_to_db and send_summary_email. load_to_db depends on cleanse_data, and send_summary_email depends on load_to_db. So the dependencies are sequential here.

Now, for the job structure. The @job will define the dependencies. The three ingest ops have no dependencies among themselves, so they can be run in parallel. Then cleanse_data depends on all three. Then load_to_db depends on cleanse_data, and send_summary_email depends on load_to_db.

I need to make sure that the ops are connected correctly. For example, the output of each ingest op is passed to cleanse_data. Since cleanse_data takes three inputs, the function definition should have parameters for each vendor's data.

Regarding resources, the user mentioned PostgreSQL and email. So I should include a resource for the database and maybe one for email. But the user said to provide simplified resource examples as comments or stubs. So I can define a database_resource and email_resource as stubs using the @resource decorator, and then include them in the job's config.

The launch pattern at the end should execute the job in process. Also, the retry configuration and schedule are mentioned, but since the code is about the job module, maybe the schedule isn't part of this code. The retries can be handled via op configuration with retry_policy, but the user might just add a comment about it.

Wait, the user's constraints say if resources/config are needed, provide simplified examples as comments or minimal stubs. So I should define the resources but maybe as empty classes or functions. For example, the database_resource could be a stub that connects to PostgreSQL, but in code, it's just a placeholder.

Also, the send_summary_email op needs to send an email, which would require some email configuration. So the email_resource could be another stub.

In the job, the resources are specified via the config. So the job would have a config_schema that includes these resources, or the resources are provided when executing. But in Dagster, resources are usually provided via resource definitions, not config. So perhaps the job is configured with resources like 'database' and 'email', which are passed to the ops that need them.

Looking at the ops: load_to_db uses the database resource, and send_summary_email uses the email resource. So those ops will require the resources as parameters.

So, the ops that need resources should have them specified in their parameters. For example, the load_to_db op would have context and cleansed_data as parameters, and the context would have the resources. Or wait, resources are accessed via context.resources. So maybe the op uses context to get the database connection.

Wait, in Dagster, resources are made available to ops via the context object. So in the op, you can access context.resources.database to get the database resource. Therefore, the op doesn't need to take the resource as a parameter, but instead accesses it through the context.

So, for the load_to_db op, the function would have context and cleansed_data as parameters. Then inside the function, use context.resources.database to get the connection.

Similarly, send_summary_email would take context and maybe some data from load_to_db, but according to the description, the email includes processing date, record counts, etc. So perhaps the load_to_db returns some metadata, like the count of records loaded, which is then passed to send_summary_email.

Wait, the load_to_db upserts records, so maybe it returns the count, which is then used by the email. So the load_to_db op's output is passed to send_summary_email.

But according to the pipeline steps, the email includes processing date, record counts, and data quality metrics. The data quality metrics might come from the cleanse_data op. Hmm, maybe cleanse_data outputs some metadata along with the cleansed data. Or perhaps the load_to_db op returns the count of records loaded, which is then used in the email.

Alternatively, the cleanse_data op could output both the cleansed data and some metrics, which are then passed through load_to_db to send_summary_email.

This requires some design. Let's see:

- cleanse_data processes the data and outputs the cleansed data (maybe a DataFrame) and some metrics (like total records, invalid records count, etc.). But in the current description, cleanse_data is followed by load_to_db, which needs the cleansed data. Then send_summary_email needs metrics from both cleanse_data and load_to_db.

But in Dagster, each op can only have one output. Unless we use multiple outputs, which would require using DynamicOutput or tuples. Alternatively, we can structure the outputs as a dictionary or a named tuple.

Alternatively, the cleanse_data op could output the cleansed data and some metrics, which are then passed along. But the load_to_db op would take the cleansed data, and the send_summary_email would take the metrics from cleanse_data and maybe the load result.

But this complicates the dependencies. Alternatively, maybe the metrics are stored in the context's log or somewhere else, but that's not ideal.

Alternatively, the send_summary_email op could depend on both cleanse_data and load_to_db. But according to the pipeline description, the email must be sent after the database load completes. So the send_summary_email op depends on load_to_db, which in turn depends on cleanse_data. So perhaps the metrics needed for the email are generated in cleanse_data and passed through load_to_db.

Wait, perhaps the load_to_db op returns some metadata, like the number of records loaded, and the send_summary_email op takes that as input along with the metrics from cleanse_data. But that would require send_summary_email to depend on both cleanse_data and load_to_db, which might not be allowed since load_to_db already depends on cleanse_data.

Alternatively, the cleanse_data op could output a tuple of (cleansed_data, metrics), and then load_to_db takes the cleansed_data, processes it, and outputs some load_metadata. Then send_summary_email takes both the metrics from cleanse_data and the load_metadata from load_to_db.

But in Dagster, each op can have multiple outputs by using the Out and OutputDefinition. Let me think: if cleanse_data has two outputs, one for the data and one for the metrics, then load_to_db can depend on the data output, and send_summary_email can depend on both the metrics output and the load_to_db output.

But how to structure that in code. Let's see:

@op(out={'cleansed_data': Out(), 'metrics': Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # process data
    cleansed_data = ...
    metrics = ...
    return cleansed_data, metrics

Then, load_to_db would take cleansed_data as input, and send_summary_email would take metrics and load_to_db's output.

But in the current pipeline description, the send_summary_email depends on load_to_db, which depends on cleanse_data. So perhaps the metrics are passed through load_to_db to send_summary_email. Alternatively, send_summary_email could take both the metrics from cleanse_data and the result from load_to_db.

But that would require send_summary_email to depend on both cleanse_data and load_to_db. However, according to the pipeline steps, the email is sent after the database load completes. So the send_summary_email must come after load_to_db, which is after cleanse_data. So the dependencies would be:

cleanse_data → load_to_db → send_summary_email

But if send_summary_email needs data from cleanse_data (metrics) and load_to_db (load metadata), then load_to_db must pass along the metrics. So perhaps load_to_db takes cleansed_data and metrics as inputs, processes the data, and then outputs the metrics along with load metadata. But that complicates the op.

Alternatively, maybe the metrics are stored in the context's resources or some other way, but that's not ideal.

Alternatively, perhaps the metrics are part of the cleansed_data output. For example, cleanse_data outputs a tuple containing both the data and the metrics. Then load_to_db takes the data part, and send_summary_email takes the metrics part. But how to split that.

Wait, perhaps the cleanse_data op returns a dictionary with 'data' and 'metrics' keys. Then load_to_db can take cleansed_data['data'], and send_summary_email can take cleansed_data['metrics'] and load_to_db's output.

But in Dagster, each output is a separate stream. So if cleanse_data has two outputs, then load_to_db can depend on one, and send_summary_email can depend on the other and on load_to_db's output.

But structuring that would require defining multiple outputs in the cleanse_data op.

Let me try to outline the code:

@op(out={'cleansed_data': Out(), 'metrics': Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # process data
    combined = pd.concat([vendor_a, vendor_b, vendor_c])
    # normalization steps...
    cleansed_data = ...
    metrics = {
        'total_records': len(cleansed_data),
        'invalid_records': ...,
    }
    return cleansed_data, metrics

Then, load_to_db would take cleansed_data as input:

@op(required_resource_keys={'database'})
def load_to_db(context, cleansed_data):
    # use context.resources.database to load data
    record_count = len(cleansed_data)
    context.resources.database.upsert(cleansed_data)
    return {'record_count': record_count}

Then send_summary_email would take both the metrics from cleanse_data and the load_metadata from load_to_db:

@op(required_resource_keys={'email'})
def send_summary_email(context, metrics, load_metadata):
    email_body = f"""
    Processing Date: {datetime.now()}
    Records Processed: {metrics['total_records']}
    Invalid Records: {metrics['invalid_records']}
    Records Loaded: {load_metadata['record_count']}
    """
    context.resources.email.send(
        to="supply-chain-team@company.com",
        subject="Shipment Data Summary",
        body=email_body
    )

But in this case, send_summary_email depends on both the 'metrics' output from cleanse_data and the output from load_to_db. However, in Dagster, each op can only depend on outputs from previous ops. So the dependencies would need to be set up correctly.

In the job definition, the dependencies would be:

cleanse_data_data = cleanse_data.cleansed_data
cleanse_data_metrics = cleanse_data.metrics

load_to_db = load_to_db(cleanse_data_data)
send_summary_email = send_summary_email(cleanse_data_metrics, load_to_db)

But how to represent multiple outputs in the job. Alternatively, when an op has multiple outputs, you can reference them using .alias() or by name.

Wait, in Dagster, when an op returns multiple outputs, you can access them by name. For example, if cleanse_data has two outputs named 'cleansed_data' and 'metrics', then other ops can depend on those outputs.

So in the job, the code would be:

@job(resource_defs={
    'database': database_resource,
    'email': email_resource,
})
def shipment_etl_job():
    vendor_a = ingest_vendor_a()
    vendor_b = ingest_vendor_b()
    vendor_c = ingest_vendor_c()
    cleansed_data, metrics = cleanse_data(vendor_a, vendor_b, vendor_c)
    load_metadata = load_to_db(cleansed_data)
    send_summary_email(metrics, load_metadata)

This way, cleanse_data is called with the three vendor data, and returns two outputs. Then load_to_db takes cleansed_data, and send_summary_email takes metrics and load_metadata.

But wait, the send_summary_email op is supposed to run after load_to_db. In this setup, send_summary_email depends on both metrics (from cleanse_data) and load_metadata (from load_to_db). Since load_to_db depends on cleansed_data, which is an output of cleanse_data, the send_summary_email will run after both metrics and load_metadata are available, which ensures it runs after load_to_db.

Yes, because load_metadata is the output of load_to_db, which depends on cleansed_data. So the send_summary_email op will only run after load_to_db has completed, because one of its inputs (load_metadata) is the output of load_to_db.

This satisfies the requirement that the email is sent after the database load completes.

Now, for the resources, the user mentioned PostgreSQL and email. So I need to define stubs for these resources. For example:

@resource(config_schema={'connection_string': str})
def database_resource(context):
    # Stub database connection
    class DatabaseConnection:
        def upsert(self, data):
            print(f"UPSERTING {len(data)} records into inventory_shipments")
    
    return DatabaseConnection()

@resource(config_schema={'smtp_server': str, 'port': int})
def email_resource(context):
    # Stub email sender
    class EmailSender:
        def send(self, to, subject, body):
            print(f"Sending email to {to}: {subject}\n{body}")
    
    return EmailSender()

These resources are then added to the job's resource_defs.

Putting it all together:

The ingest_vendor ops would read CSV files. For simplicity, they can return DataFrames. For example:

@op
def ingest_vendor_a():
    # Ingest Vendor A CSV
    return pd.read_csv("vendor_a.csv")

Similarly for B and C.

The cleanse_data op takes the three DataFrames, processes them, and returns cleansed data and metrics.

The load_to_db op uses the database resource to upsert the data.

The send_summary_email op uses the email resource to send the email.

Now, the job's resource definitions would include these stubbed resources. The user might need to configure them with actual connection strings or SMTP details, but for the code to be executable, the stubs just print messages.

Finally, the launch pattern at the end:

if __name__ == "__main__":
    result = shipment_etl_job.execute_in_process(run_config={
        'resources': {
            'database': {'config': {'connection_string': 'postgresql://user:pass@host/db'}},
            'email': {'config': {'smtp_server': 'smtp.example.com', 'port': 587}},
        }
    })

This allows the job to be run with the stub resources.

Regarding retries and schedules: The user mentioned retry configuration (2 retries with 5-minute delay), but in Dagster, retries are handled via op retry policies. So we can add a retry_policy to each op, or to the job. However, the user's constraints say to include minimal code. So perhaps adding a comment about retry policies, but since the code needs to be executable, maybe not. Alternatively, include the retry_policy in the op decorators.

But the user's example doesn't mention retry policies in the code, so perhaps it's handled via configuration elsewhere. Since the code must be executable, maybe omit the retry policies here and assume they are configured via the Dagster instance.

Similarly, scheduling is mentioned but the code doesn't include a schedule definition. The user's constraints say to generate the job module, not the schedule. So the schedule can be defined elsewhere.

Putting all this together, the final code would look like:

Import necessary modules (pandas, Dagster's op, job, resource, etc.).

Define the resources as stubs.

Define the ops with appropriate inputs and outputs.

Define the job with dependencies.

Add the main block to execute the job.

Now, check for PEP 8 compliance, correct function names, explicit dependencies, etc.

Each ingest op is named correctly, and they run in parallel. The cleanse_data op depends on all three. The load_to_db depends on cleanse_data, and send_summary_email depends on load_to_db and the metrics from cleanse_data.

Yes, this should satisfy the pipeline description.
</think>

from dagster import op, job, resource

# --- Resources ---
@resource(config_schema={"connection_string": str})
def database_resource(context):
    """Stub PostgreSQL database resource"""
    class DatabaseConnection:
        def upsert(self, data):
            print(f"UPSERTING {len(data)} records to inventory_shipments")
    
    return DatabaseConnection()

@resource(config_schema={"smtp_server": str, "port": int})
def email_resource(context):
    """Stub email notification resource"""
    class EmailSender:
        def send(self, to, subject, body):
            print(f"Email to {to}: {subject}\n{body}")
    
    return EmailSender()

# --- Operations ---
@op
def ingest_vendor_a():
    """Ingest Vendor A CSV data (1,250 records)"""
    return "vendor_a_data"  # Simplified for example

@op
def ingest_vendor_b():
    """Ingest Vendor B CSV data (980 records)"""
    return "vendor_b_data"

@op
def ingest_vendor_c():
    """Ingest Vendor C CSV data (1,750 records)"""
    return "vendor_c_data"

@op
def cleanse_data(vendor_a, vendor_b, vendor_c):
    """Clean and normalize combined vendor data"""
    combined_data = f"{vendor_a}+{vendor_b}+{vendor_c}"
    print(f"Cleansing {combined_data}")
    return {
        "cleansed_data": "processed_data",
        "metrics": {"total": 3850, "invalid": 42}
    }

@op(required_resource_keys={"database"})
def load_to_db(context, cleansed_data):
    """Load processed data to PostgreSQL database"""
    context.resources.database.upsert(cleansed_data["cleansed_data"])
    return {"loaded_records": cleansed_data["metrics"]["total"]}

@op(required_resource_keys={"email"})
def send_summary_email(context, metrics, load_result):
    """Send processing summary email"""
    email_body = f"""
    Processing Summary:
    - Total Records: {metrics['total']}
    - Invalid Records: {metrics['invalid']}
    - Loaded Records: {load_result['loaded_records']}
    """
    context.resources.email.send(
        to="supply-chain-team@company.com",
        subject="Daily Shipment Processing Summary",
        body=email_body
    )

# --- Job Definition ---
@job(resource_defs={
    "database": database_resource,
    "email": email_resource
})
def shipment_etl_job():
    a = ingest_vendor_a()
    b = ingest_vendor_b()
    c = ingest_vendor_c()
    
    processed = cleanse_data(a, b, c)
    
    load_result = load_to_db(processed["cleansed_data"])
    
    send_summary_email(processed["metrics"], load_result)

# --- Execution ---
if __name__ == "__main__":
    shipment_etl_job.execute_in_process(run_config={
        "resources": {
            "database": {"config": {"connection_string": "postgresql://user:pass@host/db"}},
            "email": {"config": {"smtp_server": "smtp.example.com", "port": 587}}
        }
    })