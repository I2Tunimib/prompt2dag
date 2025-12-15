<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three stages: Extract, Transform, Load. The Extract stage has three parallel ops for vendors A, B, and C. Then Transform depends on all three extract ops, and Load has two sequential steps where load_to_db must complete before send_summary_email.

First, I'll create the three extract ops. Since they run in parallel, they don't depend on each other. Each ingest_vendor function will return some data, maybe a list of records. But since it's a stub, I'll just return a placeholder with the record count.

Next, the cleanse_data op needs to take all three vendor data as inputs. So in Dagster, the op will have parameters for each vendor's output. The function will process them together, maybe combine the data. Then return the cleansed data count.

Then, the load_to_db op depends on cleanse_data. It takes the cleansed data and "loads" it into PostgreSQL. Again, a stub implementation here.

After loading, send_summary_email should run. It needs the cleansed data count to include in the email. So it depends on load_to_db, but also needs the data from cleanse_data. Wait, no—the email sends after the load is done. So maybe the load_to_db op passes along the data, or the send_summary_email can take both the load result and the cleansed data. Alternatively, maybe the cleansed data is passed through. Hmm. Let me think. The load_to_db might not return anything, but the send_summary_email needs the count from cleanse_data. So perhaps the send_summary_email op depends on both cleanse_data and load_to_db, ensuring that load_to_db completes first. But how to set the dependencies? In Dagster, if an op requires the output of another, it will wait for that op. So if send_summary_email requires load_to_db's output, but also needs cleanse_data's output, then the dependencies would be: cleanse_data → load_to_db, and cleanse_data → send_summary_email, load_to_db → send_summary_email. Wait, no. Let me structure it correctly.

Actually, the load_to_db must complete before send_summary_email. So send_summary_email should depend on load_to_db. But send_summary_email also needs the data from cleanse_data. So the cleanse_data op's output is used by both load_to_db and send_summary_email. But load_to_db must complete before send_summary_email. So in the job definition, the send_summary_email op will have a dependency on load_to_db, and also take the output from cleanse_data. But since load_to_db already depends on cleanse_data, the data will be available. Wait, no. Because in Dagster, each op's inputs are explicit. So send_summary_email needs to take the output from cleanse_data and the output from load_to_db. But load_to_db's output might not be necessary for the email. Maybe the email only needs the cleansed data count, which comes from cleanse_data. But the email should only be sent after the load is done. So the send_summary_email op should depend on both cleanse_data and load_to_db, but only use the output from cleanse_data. However, in Dagster, if an op depends on another op, it's because it uses its output. So perhaps the load_to_db op should pass along the cleansed data, or the send_summary_email can take the cleansed data as input, and also depend on load_to_db. But how to structure that dependency.

Alternatively, maybe the load_to_db op returns the cleansed data so that send_summary_email can take it from there. That way, send_summary_email only depends on load_to_db. Let me adjust that. So cleanse_data passes data to load_to_db, which then returns it, allowing send_summary_email to take it from load_to_db's output. That way, send_summary_email only needs to depend on load_to_db, ensuring that it runs after load_to_db completes. That might be a cleaner approach. So:

cleanse_data → load_to_db → send_summary_email

But send_summary_email would then get the data from load_to_db's output. So in code:

@op
def load_to_db(context, cleansed_data):
    # load data
    context.log.info("Loaded to DB")
    return cleansed_data  # pass through

@op
def send_summary_email(context, cleansed_data):
    # use cleansed_data to get count, etc.

Then in the job, the dependencies would be:

ingest_vendor_a, ingest_vendor_b, ingest_vendor_c → cleanse_data → load_to_db → send_summary_email

That way, send_summary_email only depends on load_to_db, which depends on cleanse_data. So the execution order is correct.

Now, for the job definition. The three ingest ops are all inputs to cleanse_data. So in the job, cleanse_data will have all three as upstream dependencies.

Then, the job would look like:

@job
def supply_chain_etl_job():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleansed = cleanse_data(va, vb, vc)
    loaded = load_to_db(cleansed)
    send_summary_email(loaded)

This sets up the dependencies correctly. The three ingest ops run in parallel, then cleanse_data runs after all three, then load_to_db runs after cleanse_data, then send_summary_email after load_to_db.

Now, the ops themselves. Each ingest op would return some data. For stubs, they can return a dictionary with a record count. For example:

@op
def ingest_vendor_a():
    return {"vendor": "A", "records": 1250}

Similarly for B and C.

The cleanse_data op takes all three vendor data, combines them, processes, and returns the cleansed data. The example says 3850 records after cleansing. So:

@op
def cleanse_data(context, vendor_a, vendor_b, vendor_c):
    total = vendor_a['records'] + vendor_b['records'] + vendor_c['records']
    # processing steps...
    context.log.info(f"Cleansed {total} records")
    return {"cleansed_records": 3850, "details": "..."}

Then load_to_db takes the cleansed data and does the upsert:

@op
def load_to_db(context, cleansed_data):
    context.log.info(f"Loaded {cleansed_data['cleansed_records']} records to DB")
    # return cleansed_data to pass through
    return cleansed_data

Finally, send_summary_email uses the cleansed data to send the email:

@op
def send_summary_email(context, cleansed_data):
    count = cleansed_data['cleansed_records']
    context.log.info(f"Sent email for {count} records")
    # email sending logic here

Now, the resources. The problem mentions PostgreSQL and email, but the code should have minimal stubs. Since the user said to provide simplified resource or config examples as comments or minimal stubs, perhaps we can add a config schema for email and database, but not implement them fully. Alternatively, since the code needs to be executable, maybe just log instead of actually sending emails or connecting to a DB.

The retry configuration and schedule are part of the orchestration. The user mentioned retry configuration: 2 retries with 5-minute delay. In Dagster, this can be set via op retries. So adding @op(retry_policy=RetryPolicy(max_retries=2, delay=300)) to each op. Wait, but the user said retry configuration applies to the entire job? Or per op? The problem says "Retry configuration: 2 retries with 5-minute delay between attempts". So perhaps each op has this retry policy. But in Dagster, retry policies can be set at the op level or job level. Since all ops should have the same retry policy, maybe we can define a retry_policy and apply it to all ops.

But the user's code constraints say to use @op and @job. So perhaps adding the retry_policy to each @op. Let's import RetryPolicy from dagster.

Wait, the user's code must be executable. So I need to make sure that all imports are correct. So:

from dagster import op, job, RetryPolicy

Then, each op can have the retry_policy parameter.

But looking at the problem statement, the retry configuration is part of the orchestration, so perhaps all ops have the same retry policy. So adding retry_policy=RetryPolicy(max_retries=2, delay=300) to each @op.

But that would be repetitive. Alternatively, can we define a default retry policy for the job? Not directly; in Dagster, retry policies are set per op. So each op needs to have the retry policy specified.

So modifying each @op decorator to include retry_policy=RetryPolicy(max_retries=2, delay=300).

But wait, the problem says "retry configuration: 2 retries with 5-minute delay between attempts". So delay of 5 minutes is 300 seconds. So the RetryPolicy would have delay=300.

So each op will have that retry policy.

Now, the schedule. The user mentioned a daily schedule starting Jan 1, 2024, no catchup. But the code only needs to include the job, not the schedule definition, unless specified. The user's constraints say to include a minimal launch pattern, which is the if __name__ block.

So the code will have the job and the ops, with retry policies, and the main block to execute.

Putting it all together:

Imports needed: from dagster import op, job, RetryPolicy

Each op has the retry policy.

Now, code:

from dagster import op, job, RetryPolicy

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_a():
    # In real implementation, read CSV from Vendor A
    return {"vendor": "A", "records": 1250}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_b():
    return {"vendor": "B", "records": 980}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_c():
    return {"vendor": "C", "records": 1750}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_data(context, vendor_a, vendor_b, vendor_c):
    total = vendor_a["records"] + vendor_b["records"] + vendor_c["records"]
    context.log.info(f"Processing {total} raw records")
    # Normalize SKU, validate dates, enrich locations
    cleansed_count = 3850  # After processing
    return {"cleansed_records": cleansed_count, "metrics": {"valid": 3850, "invalid": total - 3850}}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def load_to_db(context, cleansed_data):
    # Stub database load - real implementation would use PostgreSQL resource
    context.log.info(f"Loading {cleansed_data['cleansed_records']} records into inventory_shipments")
    return cleansed_data  # Pass through for email

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def send_summary_email(context, cleansed_data):
    # Stub email sending - real implementation would use email resource
    context.log.info(
        f"Sending summary email for {cleansed_data['cleansed_records']} records"
        f" with metrics: {cleansed_data['metrics']}"
    )

@job
def supply_chain_etl_job():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleansed = cleanse_data(va, vb, vc)
    loaded = load_to_db(cleansed)
    send_summary_email(loaded)

if __name__ == "__main__":
    result = supply_chain_etl_job.execute_in_process()

This code should satisfy all the constraints. Each op has the retry policy, dependencies are set correctly, and the main block allows execution. The data flows from the three ingest ops into cleanse_data, then to load_to_db, then to send_summary_email. The send_summary_email depends on load_to_db completing, which ensures the correct order.

I need to check if the send_summary_email is correctly taking the output from load_to_db, which is the cleansed_data. Since load_to_db returns the cleansed_data, send_summary_email receives it as input. That's correct because the email needs the cleansed data metrics. The load_to_db doesn't modify the data, just passes it through, which is acceptable for this example.

Also, the three ingest ops are all inputs to cleanse_data, so they run in parallel, as required.

The code is PEP 8 compliant, uses explicit dependencies, and includes minimal docstrings (though none are added here, but the user didn't require them unless helpful). The stubs are safe and executable.
</think>

from dagster import op, job, RetryPolicy

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_a():
    return {"vendor": "A", "records": 1250}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_b():
    return {"vendor": "B", "records": 980}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def ingest_vendor_c():
    return {"vendor": "C", "records": 1750}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def cleanse_data(context, vendor_a, vendor_b, vendor_c):
    total = vendor_a["records"] + vendor_b["records"] + vendor_c["records"]
    context.log.info(f"Processing {total} raw records")
    return {"cleansed_records": 3850, "metrics": {"valid": 3850, "invalid": total - 3850}}

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def load_to_db(context, cleansed_data):
    context.log.info(f"Loaded {cleansed_data['cleansed_records']} records to inventory_shipments")
    return cleansed_data

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def send_summary_email(context, cleansed_data):
    context.log.info(
        f"Sent email summary: {cleansed_data['cleansed_records']} records processed. "
        f"Metrics: {cleansed_data['metrics']}"
    )

@job
def supply_chain_etl_job():
    va = ingest_vendor_a()
    vb = ingest_vendor_b()
    vc = ingest_vendor_c()
    cleansed = cleanse_data(va, vb, vc)
    loaded = load_to_db(cleansed)
    send_summary_email(loaded)

if __name__ == "__main__":
    supply_chain_etl_job.execute_in_process()