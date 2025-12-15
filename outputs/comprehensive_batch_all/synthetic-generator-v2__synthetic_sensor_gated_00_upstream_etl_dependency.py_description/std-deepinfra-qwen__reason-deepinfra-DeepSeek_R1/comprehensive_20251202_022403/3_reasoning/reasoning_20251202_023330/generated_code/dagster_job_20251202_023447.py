<think>
Okay, I need to convert the given pipeline description into a Dagster job. Let's start by understanding the requirements. The pipeline has three steps: a sensor waiting for an upstream process, loading CSV data, and generating a dashboard. 

First, Dagster uses ops and jobs instead of Airflow's operators and DAGs. The sensor in Dagster is a separate concept, so I'll need to create a sensor that triggers the job when the upstream completes. But the user wants the job code, so maybe the sensor is part of the job? Wait, no. In Dagster, sensors are outside of jobs. They detect events and can trigger job runs. So the job itself will consist of the three ops, but the sensor is a separate component. However, the user's constraints mention using ops and a job with dependencies, so the sensor might need to be modeled as part of the job. Hmm, but in Dagster, sensors are not part of the job definition. They are part of the repository. Wait, the original pipeline description mentions the sensor as the first step. But in Dagster, sensors are not steps within a job. So perhaps the first op is a sensor, but that's not how Dagster works. Sensors are standalone and launch job runs when certain conditions are met. So maybe the job is triggered by a sensor, but the job itself has the three ops. But the user's pipeline steps include the sensor as the first step. This might be a misunderstanding. Let me think again.

The original pipeline's first step is an ExternalTaskSensor that monitors another DAG. In Airflow, that sensor is a task in the DAG. In Dagster, sensors are separate from jobs. So the sensor would be defined outside the job and trigger the job when the upstream completes. But the user's constraints say to model each step as an op. So perhaps the first op is a sensor-like wait, but Dagster doesn't have ops that wait like that. Alternatively, maybe the sensor is part of the job's triggering mechanism, and the job starts with the load_sales_csv op. But the pipeline steps include the sensor as the first step. This is conflicting.

Wait, the user's constraints mention "If you need resources/config, provide simplified resource or config examples as comments or minimal stubs." So maybe the sensor is a separate entity, but the job is structured with the three ops. However, in Dagster, the sensor would be a separate definition that triggers the job when the upstream completes. But the user's pipeline steps list the sensor as the first task. So perhaps the job is supposed to start with an op that acts as a sensor. But that's not how Dagster works. Ops are computational steps, not sensors. Sensors are part of the Dagster framework that monitor external state and trigger job runs.

This is a problem. The original pipeline's first step is a sensor that waits for another DAG to complete. In Dagster, this would be a sensor that watches for the completion of the upstream job (daily_sales_aggregation) and then triggers this job. So the job itself doesn't include the sensor as an op. The job would consist of the load_sales_csv and generate_dashboard ops. But the user's pipeline description includes the sensor as the first step. So perhaps the user expects the sensor to be part of the job. But in Dagster, that's not possible. So maybe the answer should model the sensor as a separate component, but the job code only includes the load and generate steps. However, the user's constraints specify to use ops and a job with dependencies. So perhaps the sensor is not part of the job code but is a separate sensor definition. But the user's question is to convert the pipeline into a Dagster job module. So maybe the answer should include the job with the three steps, but how to model the sensor as an op?

Alternatively, perhaps the first op is a dummy op that represents the sensor's completion. But that's not accurate. Alternatively, maybe the sensor is part of the job's execution by using a sensor that starts the job, and the job's first op is the loading. But the original pipeline's steps include the sensor as the first task. So perhaps the user expects the sensor to be part of the job's structure. But in Dagster, that's not feasible. So this is a misunderstanding in the original pipeline's translation to Dagster.

Given this confusion, perhaps the correct approach is to model the sensor as a Dagster sensor that triggers the job when the upstream completes. Then the job itself includes the load and generate steps. But the original pipeline's steps include the sensor as the first task. So maybe the answer should create a sensor that triggers the job, and the job has the two ops. But the user's code example expects three ops. Alternatively, perhaps the wait_for_sales_aggregation is an op that checks for the upstream completion. But how to implement that in Dagster?

Wait, in Dagster, you can have ops that check external state. So perhaps the first op is a function that polls the upstream system to see if the daily_sales_aggregation has completed. But that's not using Dagster's sensor functionality. The user's constraints mention using ops and jobs, so maybe the first op is a sensor-like op that waits. But that's not how sensors work in Dagster. Sensors are for triggering job runs, not for within a job.

This is a bit of a dilemma. Let me re-read the pipeline description. The first step is an ExternalTaskSensor that monitors the daily_sales_aggregation DAG. So in Airflow, this sensor is part of the DAG's tasks. In Dagster, the equivalent would be a sensor that watches for the completion of the upstream job and triggers this job. So the sensor is not part of the job's ops but is a separate entity. Therefore, the job would consist of the load_sales_csv and generate_dashboard ops, with a sensor that triggers the job when the upstream completes. However, the user's pipeline steps include the sensor as the first step. So perhaps the answer should include a sensor as part of the job, but that's not possible. Therefore, the answer should model the sensor outside the job and the job includes the other two steps. But the user's code example expects three ops. So maybe the user made a mistake in the pipeline description, expecting the sensor to be an op. Alternatively, perhaps the answer should include a sensor definition in the same module, but the job itself has the two ops. Let's check the constraints again.

The constraints say: "Use Dagster ops (@op) to represent each step and a job (@job) that sets dependencies." So each step in the pipeline description must be an op. Therefore, the wait_for_sales_aggregation step must be an op. But how to model an op that waits for an external task? That's not typical in Dagster. Ops are supposed to perform computation, not wait. So perhaps the first op is a no-op, but the actual waiting is handled by a sensor. But the user's code must include three ops. Hmm.

Alternatively, perhaps the sensor is part of the job's execution by using a Dagster sensor that starts the job, and the job's first op is the load_sales_csv. But then the job would have two ops, not three. The original pipeline has three steps, including the sensor. So this is conflicting.

Wait, maybe the user is mistaken in considering the sensor as a step in the pipeline. In Airflow, sensors are tasks, but in Dagster, sensors are separate. So perhaps the correct approach is to model the sensor as a Dagster sensor that triggers the job, and the job has the two ops: load and generate. But the pipeline description lists three steps. So perhaps the user expects three ops. Therefore, maybe the answer should include a dummy op for the sensor, but that's not accurate. Alternatively, perhaps the first op is a check that the upstream has completed, perhaps by checking a file or a database. But the original sensor is an ExternalTaskSensor monitoring another DAG. So perhaps the first op in the job checks if the upstream process has completed, perhaps by checking a file's existence or a database entry. But that's different from the Airflow sensor.

Given the constraints, perhaps the answer should include three ops, with the first op being a check for the upstream completion. Let's proceed with that approach. So:

- wait_for_sales_aggregation op: checks if the upstream process has completed. But how? Maybe by checking a file exists (assuming the upstream process writes a file). Or perhaps using a Dagster asset sensor. But the user wants ops and a job. So perhaps the first op is a function that checks for the existence of the CSV file, which is the output of the upstream process. But that's not exactly the same as monitoring another DAG's completion. However, given the constraints, this might be the best approximation.

So the three ops would be:

1. wait_for_sales_aggregation: checks that the upstream CSV is available.
2. load_sales_csv: loads the CSV.
3. generate_dashboard: generates the dashboard.

But in reality, the first op would need to wait/poll until the CSV is available, which isn't how ops work. Ops are supposed to run and complete, not block. So this approach might not be feasible. Alternatively, perhaps the sensor is a separate Dagster sensor that triggers the job when the upstream completes, and the job has the two ops. But the user's pipeline steps include three steps, so the answer must have three ops. Therefore, perhaps the first op is a placeholder, and the actual waiting is handled via a sensor. But how to model that.

Alternatively, perhaps the sensor is part of the job's configuration. But I'm not sure. Given the time constraints, perhaps the best approach is to model the three ops as per the pipeline steps, with the first op being a sensor-like op that checks for the upstream completion. However, in practice, this would require the op to poll or wait, which is not ideal. But for the sake of the exercise, perhaps the answer includes an op that simulates waiting, perhaps with a comment indicating that in a real scenario, a Dagster sensor would be used.

Wait, the user's note says: "If branching/conditional paths appear, model them using standard Python branching within ops or via fan-in patterns." So perhaps the first op is a sensor-like op that uses Dagster's context to check for upstream completion. But I'm not sure how to implement that. Alternatively, perhaps the first op is a no-op, and the actual sensor is defined elsewhere. But the user's code must include three ops.

Alternatively, perhaps the first op is a dummy op that just passes, and the actual sensor is a separate component. But the user's code needs to have three ops. So perhaps the answer includes three ops, with the first being a no-op, and comments indicating that a sensor would be used to trigger the job. But that's not following the user's constraints.

Alternatively, perhaps the first op is a sensor implemented as an op using Dagster's APIs. But I don't think that's possible. Sensors are part of the framework, not ops.

This is tricky. Let me think again. The original pipeline's first step is a sensor that waits for another DAG to complete. In Dagster, the equivalent would be a sensor that monitors the completion of the upstream job (daily_sales_aggregation) and triggers this job. So the sensor is not part of the job's ops. The job would then consist of the load and generate steps. But the user's pipeline steps include three steps, so the answer must have three ops. Therefore, perhaps the answer includes a dummy op for the sensor step, but that's not correct. Alternatively, perhaps the answer models the sensor as an op that uses Dagster's context to check for the upstream completion. For example, using the Dagster API to check if the upstream job has completed. But I'm not sure how to do that.

Alternatively, perhaps the first op is a check that the required input file exists. So the wait_for_sales_aggregation op checks if the CSV file exists. If not, it raises an error, and the retry mechanism would handle retrying. But that's not exactly the same as a sensor polling every 60 seconds. However, with retries configured, perhaps this approximates the behavior. For example, if the op fails because the file isn't present, it retries after a delay. The retry configuration in the original pipeline is 2 attempts with 5-minute delays. So setting the op to retry with a delay could simulate polling.

So the wait_for_sales_aggregation op would check for the existence of the CSV file. If it's not there, it raises a Failure, and the retry mechanism would retry after 5 minutes, up to 2 times. But the original sensor has a 1-hour timeout. So 2 retries with 5 minutes each would only cover 10 minutes, which is less than an hour. Hmm. Maybe adjust the retries. But the original pipeline's retry config is 2 attempts with 5-minute delays. So perhaps the op's retry policy is set to 2 retries with 5-minute delay. But the sensor's timeout is 1 hour. So this approach may not cover the full hour. But given the constraints, this might be the best approximation.

So the code would have three ops:

1. wait_for_sales_aggregation: checks if the CSV file exists. If not, raises an exception.
2. load_sales_csv: loads the CSV.
3. generate_dashboard: generates the dashboard.

The job sets dependencies: wait → load → generate.

The retries are configured via the op's retry policy.

Additionally, the sensor's reschedule mode and polling are not directly modeled, but perhaps the retry policy approximates this.

So, putting this together:

- Use @op decorators for each step.
- The wait_for_sales_aggregation op checks for the file.
- The load op reads the file.
- The generate op creates the dashboard.
- The job has linear dependencies.
- Configure retries with retry_policy.
- Add minimal config, like the file path.

Now, code outline:

from dagster import op, job, RetryPolicy

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_sales_aggregation(context):
    # Check if the upstream CSV exists
    csv_path = context.op_config["csv_path"]
    if not os.path.exists(csv_path):
        raise Exception("Sales data not available")
    context.log.info("Sales aggregation complete")

@op
def load_sales_csv(context, wait_for_sales_aggregation):
    csv_path = context.op_config["csv_path"]
    # Load and validate CSV
    df = pd.read_csv(csv_path)
    # Validation logic here
    return df

@op
def generate_dashboard(context, load_sales_csv):
    # Generate dashboard using df
    pass

@job(
    config={
        "ops": {
            "wait_for_sales_aggregation": {"config": {"csv_path": "sales.csv"}},
            "load_sales_csv": {"config": {"csv_path": "sales.csv"}},
        }
    },
    tags={"email": "alerts@example.com"},
)
def executive_sales_dashboard_job():
    load_sales_csv(wait_for_sales_aggregation())
    generate_dashboard(load_sales_csv())

if __name__ == "__main__":
    result = executive_sales_dashboard_job.execute_in_process(run_config={...})

But wait, the dependencies are set correctly? The generate_dashboard depends on load_sales_csv, which depends on wait_for_sales_aggregation.

But in the code above, the job is defined as:

def executive_sales_dashboard_job():
    load_sales_csv(wait_for_sales_aggregation())
    generate_dashboard(load_sales_csv())

But this would create two calls to load_sales_csv: one with wait_for_sales_aggregation as input, and another as input to generate_dashboard. That's incorrect. The correct way is to chain the ops:

def executive_sales_dashboard_job():
    data = load_sales_csv(wait_for_sales_aggregation())
    generate_dashboard(data)

So each op's output is passed to the next. But in this case, wait_for_sales_aggregation doesn't produce an output, it's just a check. So perhaps the load_sales_csv doesn't take the output of wait_for_sales_aggregation, but depends on it. So the dependency is set by invoking load_sales_csv after wait_for_sales_aggregation.

In Dagster, dependencies are determined by the order of execution and input passing. So if load_sales_csv requires the output of wait_for_sales_aggregation, but in this case, the wait op doesn't produce an output. So perhaps the load op doesn't take the wait op's output as an input, but the job structure enforces the order.

Wait, no. In Dagster, if two ops are connected in the job definition without data dependencies, their order is not enforced unless specified via inputs. So to enforce the order, the later op must depend on the earlier op's output. But if the earlier op doesn't produce an output, you can have it return a value (like a dummy) that the next op takes as input. Alternatively, use the nothing type.

For example:

@op
def wait_for_sales_aggregation(context):
    # check file exists
    return None

@op
def load_sales_csv(context, _):
    # load csv

Then, in the job:

def executive_sales_dashboard_job():
    signal = wait_for_sales_aggregation()
    data = load_sales_csv(signal)
    generate_dashboard(data)

This way, the load op depends on the wait op's output (even though it's None), enforcing the order.

Alternatively, use the Nothing type for the output to indicate no data is passed but the dependency is required.

But for simplicity, perhaps the wait op returns a dummy value, and the load op takes it as an input but doesn't use it.

So adjusting the code:

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_sales_aggregation(context):
    csv_path = context.op_config["csv_path"]
    if not os.path.exists(csv_path):
        raise Exception("Sales data not available")
    context.log.info("Sales aggregation complete")
    return True  # Dummy output

@op
def load_sales_csv(context, wait_signal):
    csv_path = context.op_config["csv_path"]
    df = pd.read_csv(csv_path)
    # validation
    return df

@op
def generate_dashboard(context, df):
    # generate dashboard
    pass

@job(...)
def executive_sales_dashboard_job():
    signal = wait_for_sales_aggregation()
    df = load_sales_csv(signal)
    generate_dashboard(df)

This ensures the order: wait → load → generate.

Now, the config for the ops. The wait and load ops both need the csv_path. So in the job's config, each op's config has csv_path.

But in the code above, both ops have their own config. Alternatively, use a shared resource for the file path.

But the user's constraints say to provide simplified resource/config examples as comments or minimal stubs. So perhaps using op config.

So the job's config includes the csv_path for both ops.

But in the code, the load_sales_csv op's config also needs csv_path. So the job's config would have:

config={
    "ops": {
        "wait_for_sales_aggregation": {
            "config": {
                "csv_path": "default/path.csv"
            }
        },
        "load_sales_csv": {
            "config": {
                "csv_path": "default/path.csv"
            }
        }
    }
}

But this duplicates the path. Alternatively, use a resource for the file path, but the user's constraints allow minimal stubs. So perhaps proceed with op config.

Now, email notifications: the original pipeline has email notifications on task failure. In Dagster, this can be handled via event hooks or by configuring the executor/launcher. But the user's constraints say to include minimal stubs. So perhaps add a tag with the email address and a comment.

The job's tags include an email for alerts.

Putting it all together:

The code will import necessary modules, define the three ops with retry policies where needed, set up the job with dependencies, and include a main block for execution.

Also, need to handle imports for os and pandas, but the user's code must be executable. So include import os and pandas as pd.

So the final code would look like:

import os
import pandas as pd
from dagster import op, job, RetryPolicy

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_sales_aggregation(context):
    csv_path = context.op_config["csv_path"]
    if not os.path.exists(csv_path):
        raise Exception("Sales data not available")
    context.log.info("Sales aggregation complete")
    return True

@op
def load_sales_csv(context, wait_signal):
    csv_path = context.op_config["csv_path"]
    df = pd.read_csv(csv_path)
    # Validate data format and completeness
    if df.empty or 'revenue' not in df.columns:
        raise ValueError("Invalid sales data format")
    context.log.info(f"Loaded {len(df)} sales records")
    return df

@op
def generate_dashboard(context, df):
    # Generate visualizations and metrics
    dashboard_data = {
        'revenue_trend': df['revenue'].sum(),
        'top_product': df['product'].value_counts().idxmax(),
        'regional_sales': df.groupby('region')['revenue'].sum().to_dict()
    }
    # Save or send dashboard (stub implementation)
    context.log.info("Dashboard generated with data: %s", dashboard_data)

@job(
    config={
        "ops": {
            "wait_for_sales_aggregation": {"config": {"csv_path": "sales.csv"}},
            "load_sales_csv": {"config": {"csv_path": "sales.csv"}}
        }
    },
    tags={"email": "alerts@example.com"}
)
def executive_sales_dashboard_job():
    signal = wait_for_sales_aggregation()
    df = load_sales_csv(signal)
    generate_dashboard(df)

if __name__ == "__main__":
    result = executive_sales_dashboard_job.execute_in_process()

This code includes three ops with dependencies, retry policies, config for file paths, and a main block. The wait op checks for the CSV file's existence, the load op reads it, and the generate op creates a dashboard. The job is configured with the necessary paths and email tags. Retries are set for the wait op as per the original pipeline's constraints.
</think>

import os
import pandas as pd
from dagster import op, job, RetryPolicy

@op(retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_for_sales_aggregation(context):
    csv_path = context.op_config["csv_path"]
    if not os.path.exists(csv_path):
        raise Exception("Sales data not available")
    context.log.info("Sales aggregation complete")
    return True

@op
def load_sales_csv(context, wait_signal):
    csv_path = context.op_config["csv_path"]
    df = pd.read_csv(csv_path)
    if df.empty or 'revenue' not in df.columns:
        raise ValueError("Invalid sales data format")
    context.log.info(f"Loaded {len(df)} sales records")
    return df

@op
def generate_dashboard(context, df):
    dashboard_data = {
        'revenue_trend': df['revenue'].sum(),
        'top_product': df['product'].value_counts().idxmax(),
        'regional_sales': df.groupby('region')['revenue'].sum().to_dict()
    }
    context.log.info("Dashboard generated with data: %s", dashboard_data)

@job(
    config={
        "ops": {
            "wait_for_sales_aggregation": {"config": {"csv_path": "sales.csv"}},
            "load_sales_csv": {"config": {"csv_path": "sales.csv"}}
        }
    },
    tags={"email": "alerts@example.com"}
)
def executive_sales_dashboard_job():
    signal = wait_for_sales_aggregation()
    df = load_sales_csv(signal)
    generate_dashboard(df)

if __name__ == "__main__":
    result = executive_sales_dashboard_job.execute_in_process()