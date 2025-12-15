<think>
Okay, I need to convert this pipeline description into a Dagster job. Let's start by breaking down the requirements.

First, the pipeline has three stages: Extract, Transform, Load. Each stage has specific tasks. The Extract stage has three parallel ops for vendors A, B, and C. Then Transform depends on all three, and Load has two sequential steps: load_to_db and send_summary_email, where the email depends on the database load.

So in Dagster, I'll use @op for each task and @job to define the dependencies. The Extract ops (ingest_vendor_a, etc.) can run in parallel, so their outputs will all be inputs to the cleanse_data op. That's a fan-in pattern.

For the Transform stage, cleanse_data takes all three vendor data. Then load_to_db depends on cleanse_data, and send_summary_email depends on load_to_db. Wait, no—the description says load_to_db must complete before the email is sent. So the email op should depend on load_to_db.

Now, the ops need to pass data. Since the example uses XCom (from Airflow?), but Dagster uses op outputs and inputs. So each ingest op returns data, which cleanse_data takes as inputs. Then cleanse_data returns processed data, which load_to_db takes, and after that, send_summary_email runs.

But the send_summary_email might need some data from the previous steps, like record counts. Hmm. The email includes processing date, record counts, and metrics. So maybe the load_to_db op returns some metadata, or the cleanse_data op does, which is passed through.

Wait, the load_to_db upserts records, so maybe the count is determined after cleansing. So cleanse_data could output the count, which is then passed to load_to_db and then to send_summary_email. Or perhaps the load_to_db also returns a count, but the problem states that the email includes data quality metrics which might come from the cleansing step.

Alternatively, maybe the send_summary_email op takes the output from cleanse_data to get the record count and metrics. But the email is sent after loading, so the dependencies are: cleanse_data → load_to_db → send_summary_email. So perhaps load_to_db passes along the necessary data for the email.

Alternatively, the send_summary_email could depend on both load_to_db and cleanse_data, but that might complicate the dependencies. But according to the description, the database load must complete before the email is sent, so the email should only run after load_to_db. So maybe the data needed for the email is passed from cleanse_data to load_to_db, and then load_to_db passes it to send_summary_email.

But how to structure that. Let's think about the data flow:

ingest_vendor_a → output to cleanse_data
ingest_vendor_b → output to cleanse_data
ingest_vendor_c → output to cleanse_data

cleanse_data processes all three, outputs cleansed data and maybe some metrics. Then load_to_db takes the cleansed data, does the upsert, and perhaps returns a success status or some metadata. Then send_summary_email uses the metrics from cleanse_data and the success status from load_to_db.

Wait, but in Dagster, each op's output is passed to the next op. So if send_summary_email needs data from cleanse_data, it would have to receive it as an input. But since send_summary_email comes after load_to_db, which depends on cleanse_data, how can it get the data from cleanse_data?

One approach is to have load_to_db return both the load status and the metrics, but that might not make sense. Alternatively, the cleanse_data op could output a tuple containing both the cleansed data and the metrics. Then load_to_db takes the cleansed data, and send_summary_email takes the metrics part.

Alternatively, maybe the metrics are stored in a context object or resource, but the problem states to use standard Dagster features. So perhaps:

cleanse_data outputs a dictionary with 'cleaned_data' and 'metrics'.
load_to_db takes 'cleaned_data' and processes it, then returns a success flag.
send_summary_email takes 'metrics' from cleanse_data and 'success' from load_to_db.

But in Dagster, each op can only depend on the outputs of previous ops. So if send_summary_email needs both metrics from cleanse_data and the result from load_to_db, it would have to take both as inputs. But load_to_db depends on cleanse_data, so the data flow would be:

cleanse_data → load_to_db (using cleaned_data)
cleanse_data → send_summary_email (using metrics)
load_to_db → send_summary_email (using success status)

But that would require send_summary_email to have two inputs: one from cleanse_data (metrics) and one from load_to_db (success). However, in Dagster, the dependencies are determined by the inputs. So if send_summary_email requires an input from load_to_db, then it will run after load_to_db. But it can also take an input from cleanse_data as long as that's available.

Wait, but in Dagster, if an op has two inputs from two different ops, those two ops must have completed before this op can run. But in this case, cleanse_data is a predecessor to load_to_db. So the send_summary_email op would have inputs from both cleanse_data and load_to_db. But since load_to_db depends on cleanse_data, the send_summary_email would only run after load_to_db completes, which is after cleanse_data. So that's acceptable.

But according to the pipeline description, the email should be sent only after the database load completes. So the send_summary_email should depend on load_to_db. But if it also depends on cleanse_data, that's okay because cleanse_data is upstream of load_to_db.

Alternatively, perhaps the metrics are passed through load_to_db. For example, cleanse_data produces metrics, which are passed to load_to_db, which then passes them along to send_summary_email. That way, send_summary_email only depends on load_to_db.

Let me think. The email includes processing date, record counts, and data quality metrics. The record counts are after cleansing (3,850 records), which is from the cleanse_data step. The data quality metrics might also come from cleanse_data. The load_to_db step upserts those records. So the email needs info from cleanse_data, but must run after load_to_db.

So the send_summary_email op needs to receive the metrics from cleanse_data, but only after load_to_db has completed. How to achieve that.

In Dagster, if send_summary_email takes an input from cleanse_data and another from load_to_db, then it will wait for both. But since load_to_db depends on cleanse_data, the send_summary_email will only run after load_to_db completes, which is correct.

So the code would look like:

@op
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # process data, compute metrics
    return {'cleaned_data': ..., 'metrics': ...}

@op
def load_to_db(cleaned_data):
    # load data, return success or some status
    return 'success'

@op
def send_summary_email(metrics, status):
    # send email using metrics and status

Then, in the job definition:

defs = Definitions(
    job=define_job(
        op_defs=[ingest_vendor_a, ingest_vendor_b, ingest_vendor_c, cleanse_data, load_to_db, send_summary_email],
        dependencies={
            'cleanse_data': {
                'vendor_a': DependencyDefinition('ingest_vendor_a'),
                'vendor_b': DependencyDefinition('ingest_vendor_b'),
                'vendor_c': DependencyDefinition('ingest_vendor_c'),
            },
            'load_to_db': {
                'cleaned_data': DependencyDefinition('cleanse_data', output='cleaned_data'),
            },
            'send_summary_email': {
                'metrics': DependencyDefinition('cleanse_data', output='metrics'),
                'status': DependencyDefinition('load_to_db'),
            },
        },
    ),
)

Wait, but in Dagster, when you have multiple outputs, you need to specify them. So maybe the cleanse_data op has two outputs: cleaned_data and metrics. Then, load_to_db depends on the cleaned_data output, and send_summary_email depends on the metrics output and the load_to_db output.

So the code would be:

@op(out={"cleaned_data": Out(), "metrics": Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # process data
    cleaned_data = ...
    metrics = ...
    return cleaned_data, metrics

Then, load_to_db takes cleaned_data as input:

@op
def load_to_db(cleaned_data):
    # load data
    return "success"

Then, send_summary_email takes metrics from cleanse_data and the result from load_to_db:

@op
def send_summary_email(metrics, load_status):
    # send email using metrics and load_status

In the job dependencies, the send_summary_email op depends on both the 'metrics' output of cleanse_data and the output of load_to_db.

But how to structure the dependencies in the job. Let's see:

The job would have:

ingest_vendor_a, ingest_vendor_b, ingest_vendor_c → cleanse_data → load_to_db
cleanse_data → send_summary_email (for metrics)
load_to_db → send_summary_email (for load_status)

But in Dagster, each op's dependencies are defined based on their inputs. So for send_summary_email to have both metrics and load_status as inputs, it must depend on both cleanse_data and load_to_db. But since load_to_db already depends on cleanse_data, the send_summary_email will run after load_to_db completes, which is correct.

So the code would be structured with:

- The three ingest ops run in parallel.
- Their outputs are passed to cleanse_data.
- Cleanse_data produces two outputs: cleaned_data and metrics.
- Load_to_db takes cleaned_data.
- Send_summary_email takes metrics from cleanse_data and the output from load_to_db.

So the dependencies are:

cleanse_data depends on all three ingest ops.

load_to_db depends on cleanse_data's cleaned_data.

send_summary_email depends on cleanse_data's metrics and load_to_db's output.

In code, the job would be defined with:

@job
def supply_chain_etl():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleaned_data, metrics = cleanse_data(va, vb, vc)
    load_status = load_to_db(cleaned_data)
    send_summary_email(metrics, load_status)

This way, the send_summary_email op runs after load_to_db, since load_status is an input to it. And it also receives the metrics from cleanse_data.

Now, for the actual code:

Each ingest op returns some data. For simplicity, maybe they return the number of records, but in reality, they would return the data. But since this is a minimal example, perhaps they return a list of records or a count.

The cleanse_data op takes the three vendor data, processes them, and returns cleaned data and metrics. The load_to_db op takes the cleaned data and returns a status. The send_summary_email op uses metrics and status to send the email.

Now, implementing the ops:

For the ingest ops, they could read CSV files, but since the problem says to provide simplified examples, maybe they just return dummy data. For example:

@op
def ingest_vendor_a():
    # Ingest Vendor A CSV (1,250 records)
    return ["vendor_a_data"] * 1250

Similarly for B and C.

The cleanse_data op would process these:

@op(out={"cleaned_data": Out(), "metrics": Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # Combine data
    combined = vendor_a + vendor_b + vendor_c
    # Example cleansing steps
    cleaned_data = [record for record in combined if validate(record)]  # hypothetical validation
    metrics = {
        "total_records": len(combined),
        "cleaned_records": len(cleaned_data),
        "invalid_count": len(combined) - len(cleaned_data),
    }
    return cleaned_data, metrics

Then load_to_db:

@op
def load_to_db(cleaned_data):
    # Load to PostgreSQL
    # Example: upsert records
    print(f"Loading {len(cleaned_data)} records to database")
    return {"status": "success", "records_loaded": len(cleaned_data)}

send_summary_email:

@op
def send_summary_email(metrics, load_result):
    # Send email with metrics and load status
    email_body = f"""
    Processing Date: {datetime.date.today()}
    Total Records Processed: {metrics['total_records']}
    Cleaned Records: {metrics['cleaned_records']}
    Invalid Records Filtered: {metrics['invalid_count']}
    Database Load Status: {load_result['status']}
    Records Loaded: {load_result['records_loaded']}
    """
    print(f"Sending email to supply-chain-team@company.com:\n{email_body}")

But since the problem says to avoid placeholders and use safe defaults, perhaps the email is just printed.

Now, putting it all together.

Also, the pipeline mentions retry configuration: 2 retries with 5-minute delay. In Dagster, this can be set with op retry policies. So adding a retry policy to the job or individual ops.

But the user constraints say to include minimal resource/config examples as comments or stubs. So perhaps adding a comment about retries.

For example:

from dagster import op, job, RetryPolicy

default_retry_policy = RetryPolicy(max_retries=2, delay=300)  # 5 minutes in seconds

Then, in the job definition, include the retry policy:

@job(retry_policy=default_retry_policy)
def supply_chain_etl():
    ...

But according to the problem, the retry configuration is 2 retries with 5-minute delay. So adding that.

Also, scheduling: daily execution starting Jan 1, 2024, no catchup. But the user constraints say to provide a minimal launch pattern, so maybe the schedule is beyond the current scope, but the code should include the job with the execute_in_process part.

The user constraints say to include the if __name__ == "__main__" block.

Putting all together:

The code would look like:

from dagster import op, job, RetryPolicy

default_retry_policy = RetryPolicy(max_retries=2, delay=300)

@op
def ingest_vendor_a():
    # Simulate ingesting Vendor A CSV
    return ["vendor_a_data"] * 1250

@op
def ingest_vendor_b():
    # Simulate ingesting Vendor B CSV
    return ["vendor_b_data"] * 980

@op
def ingest_vendor_c():
    # Simulate ingesting Vendor C CSV
    return ["vendor_c_data"] * 1750

@op(out={"cleaned_data": Out(), "metrics": Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    # Combine and cleanse data
    combined = vendor_a + vendor_b + vendor_c
    # Example validation (filter out 10% as invalid)
    cleaned_data = combined[:int(len(combined) * 0.9)]
    metrics = {
        "total_records": len(combined),
        "cleaned_records": len(cleaned_data),
        "invalid_count": len(combined) - len(cleaned_data),
    }
    return cleaned_data, metrics

@op
def load_to_db(cleaned_data):
    # Simulate database load
    print(f"Loaded {len(cleaned_data)} records into PostgreSQL")
    return {"status": "success", "records_loaded": len(cleaned_data)}

@op
def send_summary_email(context, metrics, load_result):
    # Simulate sending email
    email_body = f"""
    Supply Chain Processing Summary
    Date: {context.run.run_time.date() if context.run else 'N/A'}
    Total Records Received: {metrics['total_records']}
    Cleaned Records: {metrics['cleaned_records']}
    Invalid Records Removed: {metrics['invalid_count']}
    Database Load Status: {load_result['status']}
    Records Successfully Loaded: {load_result['records_loaded']}
    """
    print("Sending email to supply-chain-team@company.com:")
    print(email_body)

@job(retry_policy=default_retry_policy)
def supply_chain_etl():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleaned_data, metrics = cleanse_data(va, vb, vc)
    load_result = load_to_db(cleaned_data)
    send_summary_email(metrics, load_result)

if __name__ == "__main__":
    result = supply_chain_etl.execute_in_process()

Wait, but the send_summary_email op is defined with three parameters: context, metrics, load_result. But in the job, it's called with metrics and load_result. So the context is automatically provided by Dagster if the op has a context argument. So the op should have context as the first parameter.

But in the code above, when defining send_summary_email, the parameters are context, metrics, load_result. But in the job, when we call send_summary_email(metrics, load_result), that would pass metrics as the first argument, which would be the context. That's incorrect.

So the correct way is to have the context as the first parameter, then the other inputs. So the op should be defined as:

@op
def send_summary_email(context, metrics, load_result):
    ...

And in the job, when wiring the dependencies, the inputs are metrics and load_result. The context is automatically injected by Dagster when the op is executed.

So in the job definition, the send_summary_email is called with metrics and load_result as inputs, which correspond to the op's parameters after context.

That should be correct.

Testing this code: when the job runs, the three ingest ops run in parallel, then cleanse_data runs, then load_to_db, then send_summary_email.

The RetryPolicy is applied to the entire job. The problem states that the retry configuration is 2 retries with 5-minute delay, which matches the policy.

The send_summary_email uses the context to get the run time, which is the processing date. The context.run.run_time would be the time of the execution. In the execute_in_process case, it's the time when the job is run.

The email is printed to the console, which is a minimal implementation.

This code should be PEP 8 compliant, uses explicit dependencies, and includes all required steps.
</think>

from dagster import op, job, RetryPolicy, Out

default_retry_policy = RetryPolicy(max_retries=2, delay=300)

@op
def ingest_vendor_a():
    """Simulate ingesting Vendor A CSV data (1,250 records)"""
    return [{"vendor": "A", "data": f"record_{i}"} for i in range(1250)]

@op
def ingest_vendor_b():
    """Simulate ingesting Vendor B CSV data (980 records)"""
    return [{"vendor": "B", "data": f"record_{i}"} for i in range(980)]

@op
def ingest_vendor_c():
    """Simulate ingesting Vendor C CSV data (1,750 records)"""
    return [{"vendor": "C", "data": f"record_{i}"} for i in range(1750)]

@op(out={"cleaned_data": Out(), "metrics": Out()})
def cleanse_data(vendor_a, vendor_b, vendor_c):
    """Clean and normalize combined vendor data"""
    combined = vendor_a + vendor_b + vendor_c
    valid_records = [rec for rec in combined if "record" in rec["data"]]
    metrics = {
        "total_received": len(combined),
        "valid_count": len(valid_records),
        "invalid_count": len(combined) - len(valid_records)
    }
    return valid_records, metrics

@op
def load_to_db(cleaned_data):
    """Simulate loading cleaned data to PostgreSQL database"""
    print(f"Upserting {len(cleaned_data)} records to inventory_shipments table")
    return {"status": "success", "loaded_count": len(cleaned_data)}

@op
def send_summary_email(context, metrics, load_result):
    """Simulate sending summary email notification"""
    email_content = f"""
    Supply Chain Processing Report - {context.run.run_time.date()}
    =====================================================
    Total Records Received: {metrics['total_received']}
    Valid Records Processed: {metrics['valid_count']}
    Invalid Records Filtered: {metrics['invalid_count']}
    Database Load Status: {load_result['status']}
    Records Successfully Loaded: {load_result['loaded_count']}
    """
    print(f"Sending email to supply-chain-team@company.com:\n{email_content}")

@job(retry_policy=default_retry_policy)
def supply_chain_etl():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleaned_data, metrics = cleanse_data(va, vb, vc)
    load_result = load_to_db(cleaned_data)
    send_summary_email(metrics, load_result)

if __name__ == "__main__":
    supply_chain_etl.execute_in_process()