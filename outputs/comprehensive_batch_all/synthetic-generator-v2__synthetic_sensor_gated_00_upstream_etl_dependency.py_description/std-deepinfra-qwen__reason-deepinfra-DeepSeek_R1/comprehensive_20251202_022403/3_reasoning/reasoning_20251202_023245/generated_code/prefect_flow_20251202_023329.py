<think>
Okay, I need to convert the given pipeline description into a Prefect 2.x flow. Let me start by understanding the requirements.

First, the pipeline has three steps: wait_for_sales_aggregation, load_sales_csv, and generate_dashboard. These should be tasks in a Prefect flow. The flow needs to run them sequentially.

The wait_for_sales_aggregation step is an ExternalTaskSensor in Airflow, but in Prefect, I'll need to use a sensor. Prefect has the wait_for_task or wait_for_run functionality, but since it's an external DAG, maybe I need to use a custom solution. Wait, the user mentioned using a sensor-gated pattern. Prefect's equivalent might be using a wait_for function with retries. Alternatively, perhaps using a task that polls until the upstream process is complete. Since the example uses ExternalTaskSensor with reschedule mode, maybe I can model this with a task that sleeps and retries until a condition is met. But how to check if the daily_sales_aggregation DAG has completed? Since Prefect doesn't have DAGs in the same way as Airflow, maybe the upstream process is another flow run. So perhaps the wait_for_sales_aggregation task would check for the completion of a specific flow run. However, the problem states to assume required packages are installed, so maybe I can use a placeholder with a comment indicating that the actual implementation would check the upstream process.

Next, the load_sales_csv task loads a CSV, validates format and completeness. So this task would read the CSV file, perform some checks, maybe return the data or a path.

Generate_dashboard task creates visualizations. This could involve generating plots and saving them or compiling a report.

The orchestration is linear, so each task is called sequentially. No branching or parallelism, so no need for .submit() here. But the user mentioned for parallel steps use .submit(), but here all steps are sequential.

The scheduling part: the flow should run daily, aligned with the upstream process. In Prefect, scheduling can be done via deployments. The user says to include a comment about deployment/schedule configuration if schedules are mentioned. So I'll add a comment indicating the schedule setup.

Retries: the pipeline has retry configuration with 2 attempts and 5-minute delays. In Prefect, tasks can have retries set with retries and retry_delay_seconds parameters in the @task decorator.

Email notifications: Prefect has notifications via hooks or in the deployment setup. Since the code needs to be self-contained, maybe add a comment about configuring notifications in the deployment, or use a task failure handler. But the user says to include code that's executable, so perhaps the tasks have on_failure hooks. However, implementing email notifications would require more setup. The user might just want a comment indicating that email notifications are configured, perhaps in the deployment YAML.

Now, putting it all together.

First, import flow and task from prefect.

Define each task as @task decorated functions. The wait_for_sales_aggregation task needs to poll until the upstream process completes. Since it's a sensor with reschedule mode, perhaps this task will sleep and retry. But in Prefect, tasks can't be rescheduled in the same way as Airflow. So maybe this task uses a loop with time.sleep and checks for the existence of the upstream completion. However, to avoid blocking the worker, maybe use asynchronous checks or Prefect's built-in waiting mechanisms. Alternatively, use a subflow that retries. But perhaps for simplicity, the task will have retries configured with a delay, so that it retries every 60 seconds, up to a maximum of 1 hour (which is 60 retries). Wait, the timeout is 1 hour, so 60 retries * 60 seconds each = 3600 seconds = 1 hour. So set the task's retries to 60, retry_delay_seconds to 60. But wait, retries are for failures. The sensor is waiting for a condition, not failing. So maybe the task should have a while loop that checks the condition each time, and exits when the condition is met or the timeout occurs. But how to handle that in Prefect?

Alternatively, use a task that raises an exception until the upstream process is complete, and use retries to keep trying. For example, the task checks if the upstream is done. If not, raises a Prefect exception to trigger a retry. The retries would be set to 60, with a delay of 60 seconds. That way, the task will retry every minute for up to an hour. If after 60 retries it's still not done, the task fails. That matches the timeout of 1 hour.

So the wait_for_sales_aggregation task would look something like this:

@task(retries=60, retry_delay_seconds=60)
def wait_for_sales_aggregation():
    # Check if upstream process is complete
    if not check_upstream_completion():
        raise Exception("Upstream not complete yet")
    return True

But check_upstream_completion() is a placeholder function. Since the user said to avoid placeholders, maybe add a comment indicating that the actual implementation would check the upstream DAG's status. But for the code to be executable, perhaps simulate it by always returning True after a certain condition. Alternatively, use a mock. But the user wants executable code. Hmm. Maybe in the example, the task just waits for 60 seconds and then proceeds, but that's not accurate. Alternatively, use a dummy check that always passes, but include a comment.

Alternatively, since the user mentioned that the code must be executable, perhaps simulate the check. For example, check for the existence of a file that the upstream process creates. But the problem states that the input is a CSV from the upstream process. So maybe the wait_for_sales_aggregation task checks if the CSV file exists and is recent. But the problem says the sensor waits for the upstream DAG completion, not the file. So perhaps the CSV is generated by the upstream DAG, and once the DAG completes, the CSV is available. So the wait task could check for the existence of the CSV file. But that's conflating two steps: waiting for the DAG to complete and then loading the CSV. However, in the pipeline steps, the first step is to wait for the DAG completion, then load the CSV. So perhaps the wait task is checking the DAG's status (via some API), and the load task reads the CSV.

But since Prefect doesn't have direct integration with Airflow's DAGs, perhaps the wait task is omitted, and instead the flow is scheduled after the upstream flow. But the problem says to model the sensor-gated pattern. So perhaps the wait task is a placeholder that simulates waiting.

Alternatively, use Prefect's built-in capabilities to wait for another flow run. For example, if the upstream process is a Prefect flow, then the wait_for_sales_aggregation task could use the Prefect API to check if the latest run of that flow has succeeded. But that requires authentication and API calls. Since the user wants the code to be executable, perhaps mock this with a function that returns True after some time. But that's not reliable. Alternatively, use a simple time-based wait for the example.

But given the constraints, perhaps proceed with a task that has retries and simulates waiting. For the sake of the example, the task will succeed on the first try but include a comment indicating that the actual implementation would check the upstream DAG.

So, code outline:

from prefect import flow, task
from prefect.filesystems import LocalFileSystem
import pandas as pd
import time

@task(
    retries=60,
    retry_delay_seconds=60,
    name="Wait for Sales Aggregation",
    description="Polls for completion of upstream daily sales aggregation process",
)
def wait_for_sales_aggregation():
    # Implementation would check external system for upstream completion
    # Simulated check that always passes after a brief delay
    time.sleep(1)
    return True

@task(
    retries=2,
    retry_delay_seconds=300,
    name="Load Sales CSV",
    description="Loads and validates aggregated sales data from CSV",
)
def load_sales_csv():
    # Assuming CSV path is provided by upstream process
    csv_path = "daily_sales_aggregation.csv"
    df = pd.read_csv(csv_path)
    # Perform validation checks
    if df.empty:
        raise ValueError("Empty sales data")
    required_columns = ["revenue", "product", "region"]
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Missing required columns")
    return df

@task(
    name="Generate Dashboard",
    description="Creates executive dashboard with visualizations",
)
def generate_dashboard(df):
    # Generate visualizations and save dashboard
    # Example: Save a simple HTML report
    dashboard_html = "<h1>Sales Dashboard</h1>"
    with open("sales_dashboard.html", "w") as f:
        f.write(dashboard_html)
    return "sales_dashboard.html"

@flow(
    name="Executive Sales Dashboard Pipeline",
    description="Daily executive sales dashboard generation flow",
    retries=0,  # No flow-level retries, task-level retries configured
)
def executive_sales_dashboard_pipeline():
    # Sensor-gated execution pattern
    wait_result = wait_for_sales_aggregation()
    
    # Linear task sequence
    sales_data = load_sales_csv()
    dashboard_path = generate_dashboard(sales_data)
    
    return dashboard_path

if __name__ == "__main__":
    # Local execution
    executive_sales_dashboard_pipeline()

Now, check constraints:

- Uses @task and @flow correctly.
- Sequential execution: tasks are called in order.
- wait_for_sales_aggregation uses retries to simulate polling every 60 seconds for up to 60 retries (1 hour).
- load_sales_csv has retries=2 and retry_delay_seconds=300 (5 minutes).
- generate_dashboard is a normal task.
- The flow has a main guard.
- Comments indicate where integrations would be.
- The code is PEP 8 compliant (as per formatters).
- The sensor uses reschedule mode via retries with delay, which in Prefect is handled by task retries.
- Email notifications: the user mentioned they are configured for task failures. Prefect allows task-level notifications via hooks, but setting that up requires more code. Since the code must be executable, perhaps omit and add a comment. But the user says to include code that's executable. Alternatively, add a comment in the task decorator about notifications. However, the example code doesn't include notification setup, so perhaps it's handled via Prefect's deployment configuration. So include a comment in the deployment section.

The schedule is daily. Add a comment indicating the deployment configuration.

So, in the code, after the flow definition, add a comment:

# To configure daily schedule and email notifications:
# Create deployment with:
# prefect deployment build ./module.py:executive_sales_dashboard_pipeline -n sales-dashboard-daily --schedule "0 0 * * *" --cron

But the user said to include a brief comment indicating deployment/schedule configuration. So perhaps add:

# Note: Configure daily schedule via deployment with cron "0 0 * * *"

Putting it all together, the code should meet the requirements.
</think>

from prefect import flow, task
import pandas as pd
import time
from pathlib import Path


@task(
    retries=60,
    retry_delay_seconds=60,
    name="Wait for Sales Aggregation",
    description="Polls upstream DAG completion with 1-hour timeout",
)
def wait_for_sales_aggregation():
    # Implementation would check external system for upstream completion
    # Simulated wait with timeout logic
    start_time = time.time()
    while time.time() - start_time < 3600:
        if Path("daily_sales_aggregation.csv").exists():  # Simplified check
            return True
        time.sleep(60)
    raise TimeoutError("Upstream sales aggregation timed out")


@task(
    retries=2,
    retry_delay_seconds=300,
    name="Load Sales CSV",
    description="Loads and validates aggregated sales data",
)
def load_sales_csv():
    df = pd.read_csv("daily_sales_aggregation.csv")
    if df.empty or {"date", "amount", "region"}.difference(df.columns):
        raise ValueError("Invalid or incomplete sales data")
    return df


@task(
    name="Generate Dashboard",
    description="Creates executive dashboard with key metrics",
)
def generate_dashboard(sales_data):
    # Simplified dashboard generation
    dashboard = f"""
    Daily Sales Dashboard
    ---------------------
    Total Sales: ${sales_data['amount'].sum():,}
    Top Region: {sales_data['region'].value_counts().idxmax()}
    """
    Path("sales_dashboard.txt").write_text(dashboard)
    return "sales_dashboard.txt"


@flow(
    name="Executive Sales Dashboard Pipeline",
    description="Generates daily executive sales reports",
    log_prints=True,
)
def executive_sales_dashboard_pipeline():
    # Sensor-gated execution pattern
    wait_for_sales_aggregation()
    
    # Sequential processing
    sales_data = load_sales_csv()
    report_path = generate_dashboard(sales_data)
    
    return report_path


if __name__ == "__main__":
    # To configure daily schedule:
    # prefect deployment build ... --schedule "0 0 * * *"
    executive_sales_dashboard_pipeline()