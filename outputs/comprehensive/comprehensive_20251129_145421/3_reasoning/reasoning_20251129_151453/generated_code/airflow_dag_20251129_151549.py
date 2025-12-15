<think>
Okay, I need to convert this pipeline description into an Apache Airflow DAG. Let's start by understanding the structure and requirements.

First, the DAG has several steps. The initial steps are Check_PCD_SFTP_Folder and Check_PCD_Shared_Folder, both using KubernetesJobOperator. Then there's an EmptyOperator to synchronize before parallel HTTP tasks. The parallel extraction involves 18 tasks, all HttpOperators. After that, another EmptyOperator Start_PCD_Extract_2 depends on Status_Tracker. Then the PCD_file_upload KubernetesJobOperator, and finally a notification PythonOperator that runs regardless of success or failure.

The schedule comes from an Airflow Variable "pcd_etl_schedule", defaulting to None. Start date is January 1, 2021, catchup is False, and a 60-minute timeout.

I need to use Airflow 2.x imports. So, importing DAG, KubernetesJobOperator from airflow.providers.cncf.kubernetes, HttpOperator from airflow.providers.http, PythonOperator, EmptyOperator, etc.

Default_args should include the start_date, email settings, retries, etc. The email_on_failure and email_on_retry are probably False since the notification task handles emails.

For the KubernetesJobOperator tasks, they each use a job_template_file from variables. The HttpOperator tasks will need http_conn_id, endpoint, method POST, and a response check. Wait, the response_check is a function that checks if the status code is 200. So I'll need to define a function for that.

The parallel HTTP tasks are 18 in total. The description lists 18 names, but when I count the list under step 4, there are 18 entries. Wait, let me check: Financial_Expense, UPCC_Financial_Reportingr (typo?), CHC_Financial_reporting, PCN_Financial_Reporting, NPPCC_Financial_Reporting, Fiscal_Year_Reporting_Dates, UPCC_Primary_Care_Patient_Services, CHC_Primary_Care_Patient_Services, Practitioner_Role_Mapping, Status_Tracker, HR_Records, Provincial_Risk_Tracking, Decision_Log, HA_Hierarchy, UPPC_Budget, CHC_Budget, PCN_Budget, NPPCC_Budget. Wait, that's 18? Let me count again:

1. Financial_Expense
2. UPCC_Financial_Reportingr
3. CHC_Financial_reporting
4. PCN_Financial_Reporting
5. NPPCC_Financial_Reporting
6. Fiscal_Year_Reporting_Dates
7. UPCC_Primary_Care_Patient_Services
8. CHC_Primary_Care_Patient_Services
9. Practitioner_Role_Mapping
10. Status_Tracker
11. HR_Records
12. Provincial_Risk_Tracking
13. Decision_Log
14. HA_Hierarchy
15. UPPC_Budget
16. CHC_Budget
17. PCN_Budget
18. NPPCC_Budget

Yes, 18. So I need to create 18 HttpOperator tasks. Each will have a task_id based on their name, maybe replacing spaces with underscores and making lowercase? Or as per the given names. Wait, the names in the list have some typos, like UPCC_Financial_Reportingr (extra 'r' at the end). But I should use the exact names provided.

Each HttpOperator will use the endpoint URL from Airflow Variables, like 'pcd_financial_expense_url', etc. The method is POST, and they need a response check. So I'll write a helper function to check the status code is 200.

The notification task is a PythonOperator that sends emails. The function for this operator needs to determine success or failure, generate HTML content, and send emails to different lists based on the outcome. It should have access to the context, including the dag_run's success or failure status and any failed tasks.

The dependencies: Check_PCD_SFTP_Folder >> Check_PCD_Shared_Folder >> Start_PCD_Extract_1. Then Start_PCD_Extract_1 triggers all 18 parallel HTTP tasks. However, Start_PCD_Extract_2 depends on Status_Tracker (one of the HTTP tasks) completing. Then PCD_file_upload depends on all HTTP tasks and Start_PCD_Extract_2. Finally, ETL_Notification depends on PCD_file_upload, but with trigger_rule all_done so it runs regardless of upstream success.

Wait, the description says ETL_Notification's input is "All upstream task completion (success or failure)", so it should be a downstream of all tasks. But in the steps, PCD_file_upload comes after the HTTP tasks and Start_PCD_Extract_2. So the notification task should be downstream of PCD_file_upload? Or is it downstream of all tasks?

Looking back: Step 7 says "Input: All upstream task completion (success or failure)". So ETL_Notification should be dependent on all previous tasks. But in the pipeline steps, after the parallel HTTP tasks and Start_PCD_Extract_2, the PCD_file_upload runs, then ETL_Notification. So ETL_Notification depends on PCD_file_upload. But according to the input description, it should depend on all tasks. So perhaps the notification task is set as a downstream of PCD_file_upload, but with trigger_rule set to all_done. However, in Airflow, if a task is downstream of another, it will only run if the upstream task is done (success or failure, depending on trigger_rule). But to capture all tasks, maybe the notification task should be a downstream of all previous tasks. But that's complicated with 18 HTTP tasks plus others.

Alternatively, since the notification is the last step, perhaps it's set to be downstream of PCD_file_upload, but with trigger_rule all_done. But that might not capture failures in earlier tasks. Wait, no. Because if any task before PCD_file_upload fails, the PCD_file_upload might not run, and the DAG run would fail. The notification task needs to trigger regardless of the success or failure of all tasks. So the correct approach is to set the notification task as the final step, with trigger_rule "all_done", and have it depend on all previous tasks. But how to set dependencies for that.

Alternatively, since the notification is supposed to run after all tasks, regardless of their success, perhaps set it as a downstream of all other tasks. But with 20+ tasks, that's cumbersome. Alternatively, use a list of all tasks and set each to precede the notification. But that's not practical. Another approach is to make the notification task the last in the sequence, but using trigger_rule "all_done" and dependencies such that it's triggered after all tasks. However, in Airflow, if a task has no upstream dependencies, it will run immediately. So perhaps the correct way is to have the notification task be downstream of all other tasks. But how to do that.

Wait, the steps are:

Check_SFTP -> Check_Shared -> Start1 -> parallel HTTP tasks.

Start_PCD_Extract_2 depends on Status_Tracker (one HTTP task) and then PCD_file_upload depends on all HTTP tasks and Start2.

Then notification depends on PCD_file_upload. But if any HTTP task fails, the PCD_file_upload might not run. So the notification task would not be triggered unless it's set to have trigger_rule all_done and depends on all previous tasks.

Alternatively, perhaps the notification task should be a downstream of all tasks. But how to set that up.

Alternatively, since the notification is supposed to run regardless of the success of all upstream tasks, perhaps set it as a downstream of the last task (PCD_file_upload) but with trigger_rule "all_done". However, if PCD_file_upload is skipped due to upstream failure, the trigger_rule "all_done" would still trigger the notification. But in that case, the notification would run after PCD_file_upload is either successful, failed, or skipped. But does that cover all possible upstream tasks? Not exactly. Because if any task before PCD_file_upload fails, the DAG run would fail, and the notification task would still need to run.

Wait, the notification task's trigger_rule is set to "all_done", which means it will run as long as all its upstream tasks are done, regardless of their status. So if the notification task is downstream of PCD_file_upload, then if PCD_file_upload is skipped (due to upstream failure), the notification task would still run. But what about tasks that are upstream of PCD_file_upload but not directly upstream of the notification task? For example, if one of the HTTP tasks fails, then PCD_file_upload is not executed, but the notification task would still run because it's downstream of PCD_file_upload (which is in a failed state?), but I'm not sure. Maybe the trigger_rule "all_done" applies to all tasks in the DAG, not just upstream. Wait, no. The trigger_rule applies to the task's direct upstream tasks. So if the notification task is downstream of PCD_file_upload, then it will run when PCD_file_upload is done (success, failed, skipped, etc.), but other tasks (like HTTP tasks) may have failed, but the notification task's trigger_rule is only based on its direct upstream (PCD_file_upload). So this approach might not capture all tasks' statuses.

The correct way is to have the notification task depend on all other tasks. But how to do that. One way is to collect all tasks into a list and set them as upstream of the notification task. For example:

etl_notification_task = PythonOperator(...)

all_tasks = [check_sftp, check_shared, start1, http_tasks..., start2, pcd_upload]

etl_notification_task << all_tasks

But with 20+ tasks, this could be tedious. Alternatively, after defining all tasks, collect them into a list and set them as upstream.

But in the pipeline steps, the notification task is step 7, which comes after PCD_file_upload (step 6). So perhaps the notification task is downstream of PCD_file_upload, but also needs to depend on all other tasks? Or maybe the notification task is the very last task, and all other tasks are upstream of it.

Alternatively, perhaps the notification task is set to trigger regardless of the entire DAG's status. But Airflow doesn't have a built-in way for a task to depend on the entire DAG. The best approach is to have the notification task depend on all leaf nodes. In this case, the leaf nodes would be the PCD_file_upload and all HTTP tasks that don't have downstreams. Wait, no. The HTTP tasks are all upstream of PCD_file_upload via Start_PCD_Extract_2 and the parallel tasks. Wait, the PCD_file_upload depends on all HTTP tasks and Start_PCD_Extract_2. So the HTTP tasks are upstream of PCD_file_upload. So the notification task is downstream of PCD_file_upload, which is downstream of all HTTP tasks. Therefore, if the notification task is downstream of PCD_file_upload and has trigger_rule "all_done", then it will run when PCD_file_upload is done, but if any HTTP task fails, PCD_file_upload may not run, and thus the notification task may not run. Wait, no. Because if an HTTP task fails, the PCD_file_upload task would be in an upstream_failed state, and the notification task's trigger_rule is "all_done", which would still run. Wait, Airflow's trigger rules: "all_done" means all upstream tasks are done, regardless of their state. So if the notification task is downstream of PCD_file_upload, and PCD_file_upload is upstream_failed (because one of its dependencies failed), then the notification task would run because PCD_file_upload is in a done state (failed or upstream_failed). But wait, if PCD_file_upload is not executed because its upstream tasks failed, then the notification task's upstream is PCD_file_upload, which would be in a state of upstream_failed or skipped. So the trigger_rule "all_done" would still trigger the notification task because the upstream task (PCD_file_upload) is in a done state. Therefore, setting the notification task downstream of PCD_file_upload with trigger_rule "all_done" would ensure it runs after PCD_file_upload is done, regardless of success or failure. However, this would not capture failures in tasks that are not upstream of PCD_file_upload. For example, if Check_PCD_SFTP_Folder fails, then Check_PCD_Shared_Folder and all subsequent tasks are skipped. In that case, PCD_file_upload would not be executed, and the notification task would not run because it's downstream of PCD_file_upload. But according to the pipeline description, the notification should trigger regardless of any upstream task's success or failure. So the notification task must be a downstream of all tasks, not just PCD_file_upload.

Therefore, the correct approach is to have the notification task depend on all tasks in the DAG. But how to implement that. One way is to collect all task IDs and set them as upstream. Alternatively, use a list of all tasks and set them as upstream of the notification task.

But in Airflow, you can set multiple upstream dependencies. So perhaps after defining all tasks, collect all the task instances and set them as upstream of the notification task.

Alternatively, use the DAG's tasks and set the notification task to be downstream of all. But how to do that programmatically.

Alternatively, since the notification task is the last step, perhaps after defining all tasks, set the notification task as downstream of all other tasks. For example:

etl_notification_task.set_upstream([check_sftp, check_shared, start1, start2, pcd_upload] + http_tasks)

But with 18 HTTP tasks, this could be tedious. So perhaps collect all the HTTP tasks into a list when creating them.

So, during the creation of the HTTP tasks, append them to a list like http_tasks = [].

Then, after creating all tasks, set the notification task's upstream to be:

etl_notification_task.set_upstream([check_sftp, check_shared, start1, start2, pcd_upload] + http_tasks)

But also, the Start_PCD_Extract_2 task depends on Status_Tracker (one of the HTTP tasks). So Start_PCD_Extract_2 is downstream of Status_Tracker, and PCD_file_upload is downstream of all HTTP tasks and Start_PCD_Extract_2.

Wait, the pipeline steps say:

Start_PCD_Extract_2 depends on Status_Tracker completion. Then PCD_file_upload depends on all parallel HTTP tasks and Start_PCD_Extract_2.

So, the dependencies are:

Check_SFTP >> Check_Shared >> Start1 >> all HTTP tasks.

Start_PCD_Extract_2 is triggered by Status_Tracker (one HTTP task) completing. Then PCD_file_upload depends on all HTTP tasks and Start_PCD_Extract_2.

Wait, how to model that. Let's see:

The HTTP tasks are all triggered by Start1. Then, Start_PCD_Extract_2 is triggered by Status_Tracker (one of the HTTP tasks). Then, PCD_file_upload depends on all HTTP tasks (using a join) and Start_PCD_Extract_2.

But how to set dependencies where PCD_file_upload depends on all HTTP tasks and Start_PCD_Extract_2.

One approach is to have PCD_file_upload be downstream of both Start_PCD_Extract_2 and all HTTP tasks. But since Start_PCD_Extract_2 is downstream of Status_Tracker, which is one of the HTTP tasks, perhaps:

http_tasks >> pcd_file_upload

start_pcd_extract_2 >> pcd_file_upload

But since Start_PCD_Extract_2 is downstream of Status_Tracker, which is part of http_tasks, then http_tasks includes Status_Tracker. So when http_tasks >> pcd_file_upload, that includes Status_Tracker. Then Start_PCD_Extract_2 is downstream of Status_Tracker, and Start_PCD_Extract_2 >> pcd_file_upload. So pcd_file_upload would depend on all http_tasks and Start_PCD_Extract_2. But since Start_PCD_Extract_2 is downstream of Status_Tracker, which is part of http_tasks, the dependencies would be:

http_tasks >> pcd_file_upload

start_pcd_extract_2 >> pcd_file_upload

But that would create a situation where pcd_file_upload depends on all http_tasks and also on start_pcd_extract_2, which is redundant because start_pcd_extract_2 is already downstream of Status_Tracker (an HTTP task). However, according to the pipeline description, PCD_file_upload must wait for all HTTP tasks and Start_PCD_Extract_2. So perhaps Start_PCD_Extract_2 is a separate dependency.

So, the correct dependencies would be:

all_http_tasks >> pcd_file_upload

start_pcd_extract_2 >> pcd_file_upload

But how to represent all_http_tasks as a list. So, after creating all 18 HttpOperator tasks, collect them into a list called http_tasks. Then:

pcd_file_upload << http_tasks

pcd_file_upload << start_pcd_extract_2

This way, pcd_file_upload depends on all HTTP tasks and Start_PCD_Extract_2.

But then, the notification task needs to depend on pcd_file_upload and all other tasks. Wait, no. Because if any task before pcd_file_upload fails, the notification should still trigger. So the notification task must depend on all tasks in the DAG. So perhaps:

notification_task.set_upstream([check_sftp, check_shared, start1, start2, pcd_file_upload] + http_tasks)

But that's a lot. Alternatively, use a dummy operator that is downstream of all tasks, and then notification depends on that. But that's more complex.

Alternatively, use the fact that the notification task is supposed to run after all tasks, regardless of their state, so set it as the last task and use trigger_rule "all_done". But how to make it depend on all tasks.

Alternatively, since the notification task is a PythonOperator that uses the context to get the overall status, perhaps it's sufficient to set it as downstream of the last task (pcd_file_upload) and use trigger_rule "all_done". But this may not capture failures in earlier tasks. For example, if Check_SFTP fails, then Check_Shared and Start1 are skipped, and all HTTP tasks are skipped. Then pcd_file_upload is skipped. The notification task would depend on pcd_file_upload, which is skipped. With trigger_rule "all_done", the notification task would run because its upstream (pcd_file_upload) is done (skipped). But in this case, the notification task would still run, which is correct. However, the notification task's function would need to check the entire DAG run's status, not just the upstream tasks.

So, perhaps the notification task can be downstream of pcd_file_upload, and in its Python function, it can access the DagRun object to determine the overall status and collect failed tasks. This way, even if pcd_file_upload is skipped or failed, the notification task runs and checks the entire DAG's status.

Yes, that's possible. The Python function can use the context to get the dag_run and check its state, as well as iterate through all task instances to find failed ones.

So, the notification task's Python function would look something like:

def send_etl_notification(**context):
    dag_run = context['dag_run']
    failed_tasks = [task.task_id for task in dag_run.get_task_instances() if task.state == 'failed']
    success = dag_run.state == 'success'
    # generate email content based on success and failed_tasks
    # send email using send_email method

Then, the notification task is downstream of pcd_file_upload, with trigger_rule "all_done". This way, even if pcd_file_upload is skipped or failed, the notification task runs. And in the function, it checks the entire DAG run's status.

This approach simplifies the dependencies, as the notification task only needs to be downstream of the last task (pcd_file_upload), but uses the trigger_rule to run regardless of that task's state, and the function checks the entire DAG's status.

Now, putting this all together.

First, default_args:

default_args = {
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

The email settings are handled by the notification task, so email_on_failure is False.

The DAG parameters:

schedule_interval = Variable.get("pcd_etl_schedule", default_var=None),
catchup = False,
dagrun_timeout = timedelta(minutes=60),
tags = ["etl", "pcd"],

Now, the KubernetesJobOperator tasks. The job_template_file is from variables. The parameters are:

Check_PCD_SFTP_Folder uses 'pcd_emtysftp_job'

Check_PCD_Shared_Folder uses 'pcd_emtydir_job'

PCD_file_upload uses 'pcd_job'

The HttpOperator tasks need to have their endpoints configured via variables. For example, Financial_Expense uses 'pcd_financial_expense_url'.

Each HttpOperator task will have:

method='POST',
data=json.dumps({"health_authority_data": "standardized_payload"}),
response_check=check_http_response,

Where check_http_response is a function that checks if the response status code is 200.

def check_http_response(response):
    return response.status_code == 200

Now, creating the 18 HttpOperator tasks. Let's list their task_ids and variable names.

The list from step 4:

Financial_Expense: variable pcd_financial_expense_url

UPCC_Financial_Reportingr: variable pcd_upcc_financial_reportingr_url (note the typo in the task name)

CHC_Financial_reporting: pcd_chc_financial_reporting_url

PCN_Financial_Reporting: pcd_pcn_financial_reporting_url

NPPCC_Financial_Reporting: pcd_nppcc_financial_reporting_url

Fiscal_Year_Reporting_Dates: pcd_fiscal_year_reporting_dates_url

UPCC_Primary_Care_Patient_Services: pcd_upcc_primary_care_patient_services_url

CHC_Primary_Care_Patient_Services: pcd_chc_primary_care_patient_services_url

Practitioner_Role_Mapping: pcd_practitioner_role_mapping_url

Status_Tracker: pcd_status_tracker_url

HR_Records: pcd_hr_records_url

Provincial_Risk_Tracking: pcd_provincial_risk_tracking_url

Decision_Log: pcd_decision_log_url

HA_Hierarchy: pcd_ha_hierarchy_url

UPPC_Budget: pcd_uppc_budget_url (note typo in task name UPPC vs UPCC?)

CHC_Budget: pcd_chc_budget_url

PCN_Budget: pcd_pcn_budget_url

NPPCC_Budget: pcd_nppcc_budget_url

Wait, the task names have some inconsistencies. For example, UPCC_Financial_Reportingr (extra 'r'), UPPC_Budget (maybe a typo for UPCC?), etc. But according to the pipeline description, I should use the exact names provided.

So, the task_ids will be as per the names given, even with typos. For example, 'UPCC_Financial_Reportingr'.

Each HttpOperator will have:

task_id='Financial_Expense',
http_conn_id='http_default',  # assuming a connection is set up; or maybe from variable?
endpoint=Variable.get("pcd_financial_expense_url"),
method='POST',
data=json.dumps({"health_authority_data": "standardized_payload"}),
response_check=check_http_response,

But wait, the http_conn_id should be the connection ID for the HTTP endpoint. The endpoint variable would be the URL path, or the full URL? Wait, the HttpOperator's http_conn_id defines the base URL, and the endpoint is appended. Alternatively, if the Airflow variable contains the full URL, then http_conn_id can be omitted, and the endpoint is the full URL. But that's not standard. Alternatively, set up an HTTP connection with the base URL, and the endpoint is the path. But given that the URLs are different for each task, perhaps each task's http_conn_id is configured separately, or the endpoint is the full URL from the variable.

But the problem is that the HttpOperator requires an http_conn_id. So perhaps each task's http_conn_id is set to a connection that has the base URL, and the endpoint is the specific path. Or, if the variable contains the full URL, then the http_conn_id can be set to None, and the endpoint is the full URL. But according to the HttpOperator documentation, the http_conn_id is required. So perhaps the solution is to have a single HTTP connection that's a placeholder, and the endpoint is the full URL retrieved from the variable.

For example, create an HTTP connection called 'pcd_http_conn' with host 'https://example.com' (or any placeholder), and then in the operator, override the endpoint with the full URL from the variable.

But the HttpOperator's 'endpoint' parameter is relative to the connection's host. So if the variable contains the full URL, perhaps we can split it into host and endpoint.

Alternatively, perhaps the variables store just the path, and the connection has the base URL. But given that each task may have a different base URL, this approach isn't feasible. Therefore, the variables must store the full URL, and the http_conn_id is set to a connection that allows overriding the base URL. Alternatively, use a connection without a host, and specify the full URL in the endpoint.

But the HttpOperator's code (airflow.providers.http.operators.http) uses the connection's host as the base, and appends the endpoint. So if the variable contains the full URL, perhaps the http_conn_id can be set to a connection that has an empty host, and the endpoint is the full URL. Or, perhaps the http_conn_id is not required if the endpoint is a full URL. But according to the operator's code, the http_conn_id is required.

This is a potential problem. To handle this, perhaps create a single HTTP connection (e.g., 'pcd_api_connection') with a dummy host, and then in each HttpOperator, set the endpoint to the full URL from the variable. But the HttpOperator will combine the connection's host and the endpoint. So if the connection's host is 'http://dummy', and the endpoint is 'https://real-url.com/path', then the resulting URL would be 'http://dummyhttps://real-url.com/path', which is invalid.

Therefore, the correct approach is to have the http_conn_id specify the base URL, and the endpoint is the path. But if each task's URL is different, this isn't feasible unless each has its own connection. Which is impractical.

Alternative solution: Use the SimpleHttpOperator, which allows specifying the full URL in the endpoint. Wait, no—the HttpOperator is the same as SimpleHttpOperator in Airflow 2.0+. Wait, checking the Airflow documentation: The HttpOperator is part of the http provider, and it can take a full URL in the 'endpoint' parameter if the connection is set up with an empty host. For example, create an HTTP connection with Connection ID 'http_default', host empty, and extra {"host": ""}. Then, in the HttpOperator, set the endpoint to the full URL from the variable.

Alternatively, perhaps the HttpOperator allows the endpoint to be a full URL if the connection's host is empty. Let's test this scenario.

Assuming that the http_conn_id is set to a connection that has an empty host, then the endpoint can be a full URL. So, in the code:

http_conn_id='pcd_http_conn',

Where 'pcd_http_conn' is an Airflow connection of type HTTP, with host left empty, and the endpoint in the operator is set to Variable.get("pcd_financial_expense_url").

This should work. So each HttpOperator uses the same connection ID, which allows the endpoint to be a full URL.

So, the HttpOperators are created with:

HttpOperator(
    task_id='Financial_Expense',
    http_conn_id='pcd_http_conn',
    endpoint=Variable.get('pcd_financial_expense_url'),
    method='POST',
    data=json.dumps({'health_authority_data': 'standardized_payload'}),
    response_check=check_http_response,
    dag=dag,
)

Now, the KubernetesJobOperator tasks. The KubernetesJobOperator requires a job_template_file parameter, which is a YAML file defining the job. The job_template_file is retrieved from Airflow variables.

For example:

KubernetesJobOperator(
    task_id='Check_PCD_SFTP_Folder',
    job_template_file=Variable.get('pcd_emtysftp_job'),
    wait_until_job_complete=True,
    dag=dag,
)

Same for Check_PCD_Shared_Folder and PCD_file_upload.

Now, the notification task. The PythonOperator calls a function that sends emails. The function uses the Airflow email backend and variables for recipient lists.

The function needs to generate HTML content with success/failure details, including failed tasks. It uses variables for the email lists: PCD_ETL_email_list_success and ETL_email_list_alerts.

The function can access the context's 'dag_run' to get the state and failed task instances.

def send_etl_notification(**context):
    dag_run = context['dag_run']
    ti = context['ti']
    failed_tasks = dag_run.get_task_instances(state='failed')
    failed_task_ids = [task.task_id for task in failed_tasks]
    success = dag_run.state == 'success'
    environment = Variable.get('Environment', default_var='DEV')
    email_list_success = Variable.get('PCD_ETL_email_list_success', default_var='default_success@example.com')
    email_list_alerts = Variable.get('ETL_email_list_alerts', default_var='default_alerts@example.com')
    
    subject = f"PCD ETL {'Success' if success else 'Failure'} - {environment}"
    recipient = email_list_success if success else email_list_alerts
    
    html_content = f"""
    <h3>PCD ETL {'Completed Successfully' if success else 'Failed'} - Environment: {environment}</h3>
    <p>DAG Run ID: {dag_run.run_id}</p>
    <p>Execution Time: {dag_run.execution_date}</p>
    """
    if not success:
        html_content += f"<p>Failed Tasks: {', '.join(failed_task_ids)}</p>"
    
    send_email(
        to=recipient,
        subject=subject,
        html_content=html_content,
    )

But wait, the send_email function is part of Airflow's utils. So we need to import it:

from airflow.utils.email import send_email

Then, in the function, call send_email with the parameters.

Now, putting all together.

Imports needed:

from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email

Wait, but the KubernetesJobOperator is from airflow.providers.cncf.kubernetes.operators.kubernetes_job. Wait, Airflow's KubernetesJobOperator is part of the cncf provider. So the correct import is:

from airflow.providers.cncf.kubernetes.operators.kubernetes_job import KubernetesJobOperator

But I need to check the exact import path. Assuming that the provider is installed, the import is correct.

So, the imports:

from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_job import KubernetesJobOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email

Now, the DAG definition.

default_args = {
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='pcd_etl',
    default_args=default_args,
    schedule_interval=Variable.get('pcd_etl_schedule', default_var=None),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['etl', 'pcd'],
) as dag:

    # Define tasks

    check_sftp = KubernetesJobOperator(
        task_id='Check_PCD_SFTP_Folder',
        job_template_file=Variable.get('pcd_emtysftp_job'),
        wait_until_job_complete=True,
    )

    check_shared = KubernetesJobOperator(
        task_id='Check_PCD_Shared_Folder',
        job_template_file=Variable.get('pcd_emtydir_job'),
        wait_until_job_complete=True,
    )

    start_extract_1 = EmptyOperator(task_id='Start_PCD_Extract_1')

    # Define HTTP tasks
    http_tasks = []

    # List of HTTP task IDs and corresponding variable names
    http_task_configs = [
        ('Financial_Expense', 'pcd_financial_expense_url'),
        ('UPCC_Financial_Reportingr', 'pcd_upcc_financial_reportingr_url'),
        ('CHC_Financial_reporting', 'pcd_chc_financial_reporting_url'),
        ('PCN_Financial_Reporting', 'pcd_pcn_financial_reporting_url'),
        ('NPPCC_Financial_Reporting', 'pcd_nppcc_financial_reporting_url'),
        ('Fiscal_Year_Reporting_Dates', 'pcd_fiscal_year_reporting_dates_url'),
        ('UPCC_Primary_Care_Patient_Services', 'pcd_upcc_primary_care_patient_services_url'),
        ('CHC_Primary_Care_Patient_Services', 'pcd_chc_primary_care_patient_services_url'),
        ('Practitioner_Role_Mapping', 'pcd_practitioner_role_mapping_url'),
        ('Status_Tracker', 'pcd_status_tracker_url'),
        ('HR_Records', 'pcd_hr_records_url'),
        ('Provincial_Risk_Tracking', 'pcd_provincial_risk_tracking_url'),
        ('Decision_Log', 'pcd_decision_log_url'),
        ('HA_Hierarchy', 'pcd_ha_hierarchy_url'),
        ('UPPC_Budget', 'pcd_uppc_budget_url'),
        ('CHC_Budget', 'pcd_chc_budget_url'),
        ('PCN_Budget', 'pcd_pcn_budget_url'),
        ('NPPCC_Budget', 'pcd_nppcc_budget_url'),
    ]

    def check_http_response(response):
        return response.status_code == 200

    for task_id, url_var in http_task_configs:
        http_task = HttpOperator(
            task_id=task_id,
            http_conn_id='pcd_http_conn',
            endpoint=Variable.get(url_var),
            method='POST',
            data=json.dumps({'health_authority_data': 'standardized_payload'}),
            response_check=check_http_response,
        )
        http_tasks.append(http_task)

    start_extract_2 = EmptyOperator(task_id='Start_PCD_Extract_2')

    pcd_upload = KubernetesJobOperator(
        task_id='PCD_file_upload',
        job_template_file=Variable.get