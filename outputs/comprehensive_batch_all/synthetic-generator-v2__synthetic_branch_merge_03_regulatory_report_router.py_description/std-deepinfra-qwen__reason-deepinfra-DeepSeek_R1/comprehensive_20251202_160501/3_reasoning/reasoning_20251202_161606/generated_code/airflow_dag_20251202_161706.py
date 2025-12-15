<think>
Okay, I need to create an Apache Airflow DAG based on the provided pipeline description. Let me start by understanding the requirements.

First, the pipeline involves reading a CSV, checking account types, branching to either FATCA or IRS processing, and then merging and archiving. The DAG should have daily execution, no catch-up, retries with 2 attempts and 5-minute delays. Also, use Airflow 2.x style imports and explicit dependencies.

Starting with the imports. The user mentioned using Airflow 2.x imports, so I'll import DAG and PythonOperator from the appropriate modules. Since there's branching, I'll need the BranchPythonOperator. No sensors are mentioned, so no need for those.

Next, define default_args with the retry parameters, start date, and other settings. The DAG should have schedule_interval as daily, so maybe '@daily', and catchup=False.

The tasks are read_csv, account_check (which branches), route_to_fatca, route_to_irs, and archive_reports. The account_check task will determine which branch to follow. Both branches need to run in parallel and then converge before archive_reports.

For XCom usage, the read_csv task will push the data, which account_check will pull. Then each route task will process their respective data and push results, which archive_reports will merge.

Each PythonOperator will have a python_callable function. The account_check function should return the task_id of the next task(s) to execute. Since it's a branch, using BranchPythonOperator here.

Wait, the BranchPythonOperator expects the callable to return a single task_id or a list of task_ids. In this case, account_check needs to decide whether to route to FATCA, IRS, or both? Wait, the description says "dynamic routing based on account type". So maybe the account_check checks each transaction's account type and decides which branches to trigger. But in Airflow, branching typically selects one or more subsequent tasks. However, the pipeline says "parallel execution paths with maximum width of 2", implying both branches can run in parallel if needed. So perhaps the account_check determines which of the two routes to execute. For example, if some accounts are international and some domestic, both routes might need to run. But how to handle that in Airflow's branching?

Hmm, maybe the account_check returns both route_to_fatca and route_to_irs task IDs if there are both types, or just one. But how to handle that dynamically. Alternatively, maybe the BranchPythonOperator is not the right approach here. Wait, the description says "conditional branching creates parallel execution paths with maximum width of 2". So perhaps the account_check decides which branches to trigger, and both can be triggered if needed. So the BranchPythonOperator's function would return a list of task_ids to follow. Then, both routes can be executed in parallel.

So, in the account_check function, after analyzing the data, if there are international accounts, include 'route_to_fatca', if domestic, include 'route_to_irs'. Then, the function returns a list of the relevant tasks. That way, both can be triggered if needed.

But how to pass that data? The function would need to read the data from XCom, check the account types, and decide which tasks to trigger. For example, if the data has both types, both routes are triggered. If only one type, only that route is triggered. Then, archive_reports runs after both routes complete, regardless of which ones were executed.

Wait, but in Airflow, if a task is not part of the branch, it will be skipped. So if account_check returns ['route_to_fatca'], then route_to_irs will be skipped. But in the pipeline description, it's possible that some days have only international or domestic accounts, so the corresponding route task should run, and the other is skipped. However, the archive task needs to wait for both routes if they were executed, but if one is skipped, how does that work?

Wait, no. If the account_check returns both task IDs, then both are executed. If it returns only one, then the other is skipped. So the archive task should depend on both route tasks, but if one is skipped, the dependency still holds because Airflow's trigger rules can handle this. The default trigger rule is 'all_success', but if a task is skipped, the 'all_success' would not trigger. So perhaps the archive task should have a trigger rule of 'none_failed'. Or use a join task that is triggered if either of the routes completes.

Alternatively, use a dummy operator after the branch to join the paths. But the user's pipeline says "branch merge pattern ensures both reporting paths complete before archival". Wait, but if one path is skipped, how can both complete? Maybe the description implies that both paths are always triggered, but that contradicts the conditional routing. Hmm, perhaps the example assumes that there are both types of accounts every day, but realistically, the pipeline should handle cases where only one type exists.

This is a bit tricky. To handle this correctly, perhaps the account_check will always trigger both routes, but each route checks if there's data to process. Alternatively, the BranchPythonOperator decides which routes to trigger based on the data. Then, the archive task depends on both routes, but if a route is skipped, the trigger rule for the archive task needs to be adjusted.

Alternatively, use the trigger rule 'none_failed' for the archive task so that it runs as long as none of the upstream tasks failed, even if some were skipped. Let me check Airflow's trigger rules. The 'all_done' trigger rule would run the task when all upstream tasks are done, regardless of their status. But in this case, the archive task should run after both routes if they were executed, or after whichever were executed. So setting the archive task's trigger_rule to 'none_failed' might work, but perhaps 'all_done' is better. Alternatively, use a Join node.

Wait, perhaps the correct approach is to have the archive task depend on both route_to_fatca and route_to_irs, and set its trigger_rule to 'none_failed'. That way, even if one is skipped, the archive task runs as long as the others succeeded.

But how to set the trigger rule in the PythonOperator. Let me think. When defining the archive_reports task, set trigger_rule='none_failed' or 'all_done'.

In the code, after defining the tasks, set the dependencies:

read_csv >> account_check
account_check >> [route_to_fatca, route_to_irs]
[route_to_fatca, route_to_irs] >> archive_reports

But if account_check returns only one of the routes, the other is skipped. Then, the archive task's upstream tasks are route_to_fatca and route_to_irs. If one is skipped, then by default, the archive task would not run because the trigger rule is 'all_success'. So changing the trigger rule to 'all_done' would make it run regardless of whether the upstream tasks were successful or skipped. But we need the archive task to run only if the executed routes succeeded.

Alternatively, use 'none_failed' which means the task runs as long as no upstream tasks failed (i.e., they succeeded or were skipped). That seems appropriate here.

So, in the archive_reports task, set trigger_rule='none_failed'.

Now, writing the functions.

The read_csv function reads the CSV and pushes it to XCom. The account_check function pulls the data, checks account types, and returns the appropriate task IDs. For example, if there are international accounts, return 'route_to_fatca'; if domestic, return 'route_to_irs'; if both, return both. Then, the BranchPythonOperator will trigger those tasks.

Wait, the BranchPythonOperator's callable should return the task_id(s) to follow. So if the data has both account types, the function returns ['route_to_fatca', 'route_to_irs'], and both tasks are executed. If only one, returns that task_id. Then, the archive task depends on both, but if one is skipped, the trigger_rule allows it to proceed.

But how to implement that in the account_check function. Let's say the function checks the data and determines which routes are needed. For example:

def account_check_func(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_csv')
    # Assume data is a list of transactions, each with 'account_type'
    account_types = set(t['account_type'] for t in data)
    branches = []
    if 'international' in account_types:
        branches.append('route_to_fatca')
    if 'domestic' in account_types:
        branches.append('route_to_irs')
    return branches

Then, the BranchPythonOperator will trigger those branches. If both are present, both tasks are executed. If none, perhaps the function returns a default? But according to the pipeline description, there's a branch-merge pattern, so maybe there's always at least one account type. But to handle cases where there are none, maybe add a dummy task or raise an error. However, the problem statement doesn't mention that, so perhaps assume that there's at least one.

In the code, the account_check_func returns the list of task_ids to trigger. Then, the BranchPythonOperator is set with task_id='account_check' and the callable as account_check_func.

Each route task processes their respective data. For example, route_to_fatca processes international accounts. These tasks can pull the data from XCom, filter the relevant accounts, process them, and push the results.

The archive_reports task then pulls the results from both routes (if they ran) and merges them.

But how to handle cases where a route didn't run (e.g., no domestic accounts). In such cases, the XCom from that route task may not exist. So the archive task needs to check if the XCom exists for each route and handle accordingly.

Alternatively, each route task pushes its results to XCom only if there was data to process. Then, the archive task can pull from both, and if one is missing, it just processes the available data.

But in the code examples, perhaps the functions for route_to_fatca and route_to_irs will process the data and push their results. If there are no accounts of their type, they might push nothing or a flag. However, in Airflow, if a task is skipped, it doesn't execute its code, so the XCom wouldn't be pushed. Therefore, the archive task needs to handle cases where XCom from a route is missing.

Alternatively, ensure that even if a route is skipped, the archive task can proceed. For example, in the archive function, try to pull XCom from both routes, and handle None cases.

But this adds complexity. Maybe the route tasks are designed to only run when there's data, so their XCom will be present if they ran. If they were skipped, their XCom isn't there. So the archive function can check for the presence.

But how to implement that. Let's outline the functions:

def read_csv_func(**kwargs):
    # Read CSV file (assuming a path; but the problem doesn't specify, so mock data)
    # For example, mock data
    data = [
        {'transaction_id': 1, 'account_type': 'international'},
        {'transaction_id': 2, 'account_type': 'domestic'},
    ]
    return data

def account_check_func(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_csv')
    account_types = set(t['account_type'] for t in data)
    branches = []
    if 'international' in account_types:
        branches.append('route_to_fatca')
    if 'domestic' in account_types:
        branches.append('route_to_irs')
    return branches

def route_to_fatca_func(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_csv')
    international_transactions = [t for t in data if t['account_type'] == 'international']
    # Process and generate XML reports (mock)
    processed_data = f"FATCA Report for {len(international_transactions)} transactions"
    ti.xcom_push(key='fatca_report', value=processed_data)

def route_to_irs_func(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_csv')
    domestic_transactions = [t for t in data if t['account_type'] == 'domestic']
    # Process and generate Form 1099 data (mock)
    processed_data = f"IRS Report for {len(domestic_transactions)} transactions"
    ti.xcom_push(key='irs_report', value=processed_data)

def archive_reports_func(**kwargs):
    ti = kwargs['ti']
    fatca_report = ti.xcom_pull(task_ids='route_to_fatca', key='fatca_report')
    irs_report = ti.xcom_pull(task_ids='route_to_irs', key='irs_report')
    # Merge and archive (mock)
    combined = f"Archived Reports:\n- {fatca_report}\n- {irs_report}"
    print(combined)

Now, the tasks:

read_csv = PythonOperator(task_id='read_csv', python_callable=read_csv_func)

account_check = BranchPythonOperator(task_id='account_check', python_callable=account_check_func)

route_to_fatca = PythonOperator(task_id='route_to_fatca', python_callable=route_to_fatca_func)

route_to_irs = PythonOperator(task_id='route_to_irs', python_callable=route_to_irs_func)

archive_reports = PythonOperator(
    task_id='archive_reports',
    python_callable=archive_reports_func,
    trigger_rule='none_failed'
)

Then dependencies:

read_csv >> account_check
account_check >> [route_to_fatca, route_to_irs]
[route_to_fatca, route_to_irs] >> archive_reports

Wait, but if account_check returns only one task, say route_to_fatca, then route_to_irs is skipped. Then, archive_reports depends on both, but route_to_irs is skipped. With trigger_rule='none_failed', archive_reports will run as long as none of the upstream tasks failed. Since route_to_irs is skipped, and route_to_fatca succeeded, 'none_failed' is satisfied.

Yes, that should work.

Now, putting it all together with the DAG definition.

Also, need to set the start_date. The user says daily execution, no catchup. So default_args can include 'start_date': days_ago(1), 'retries': 2, 'retry_delay': timedelta(minutes=5), etc.

Putting all together:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='regulatory_report_router',
    default_args=default_args,
    description='Routes financial transactions to appropriate regulatory systems',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'compliance'],
) as dag:

    def read_csv_func(**kwargs):
        # Mock data: replace with actual CSV reading logic
        data = [
            {'transaction_id': 1, 'account_type': 'international'},
            {'transaction_id': 2, 'account_type': 'domestic'},
        ]
        return data

    def account_check_func(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_csv')
        account_types = {t['account_type'] for t in data}
        branches = []
        if 'international' in account_types:
            branches.append('route_to_fatca')
        if 'domestic' in account_types:
            branches.append('route_to_irs')
        return branches

    def route_to_fatca_func(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_csv')
        international = [t for t in data if t['account_type'] == 'international']
        # Mock processing
        ti.xcom_push(key='fatca_report', value=f'FATCA: {len(international)} transactions')

    def route_to_irs_func(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='read_csv')
        domestic = [t for t in data if t['account_type'] == 'domestic']
        # Mock processing
        ti.xcom_push(key='irs_report', value=f'IRS: {len(domestic)} transactions')

    def archive_reports_func(**kwargs):
        ti = kwargs['ti']
        fatca = ti.xcom_pull(task_ids='route_to_fatca', key='fatca_report', default='No FATCA report')
        irs = ti.xcom_pull(task_ids='route_to_irs', key='irs_report', default='No IRS report')
        # Mock archiving
        print(f'Archiving reports:\n{fatca}\n{irs}')

    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv_func,
    )

    account_check = BranchPythonOperator(
        task_id='account_check',
        python_callable=account_check_func,
    )

    route_to_fatca = PythonOperator(
        task_id='route_to_fatca',
        python_callable=route_to_fatca_func,
    )

    route_to_irs = PythonOperator(
        task_id='route_to_irs',
        python_callable=route_to_irs_func,
    )

    archive_reports = PythonOperator(
        task_id='archive_reports',
        python_callable=archive_reports_func,
        trigger_rule='none_failed',
    )

    read_csv >> account_check
    account_check >> [route_to_fatca, route_to_irs]
    [route_to_fatca, route_to_irs] >> archive_reports

This should handle the branching correctly. The archive task uses trigger_rule='none_failed' to ensure it runs even if one of the routes is skipped. The XCom pulls in the archive function have default values in case a route was skipped (so the XCom isn't present). The mock data includes both account types, but if the CSV has only one type, the corresponding route is triggered and the other is skipped.

I think this meets all the requirements: correct imports, branching, dependencies, retries, schedule, etc. PEP 8 compliance is handled with proper formatting and line lengths.
</think>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='regulatory_report_router',
    default_args=default_args,
    description='Financial transaction regulatory reporting pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['compliance', 'finance'],
) as dag:

    def read_csv(**kwargs):
        """Mock CSV reading function with sample data"""
        return [
            {'txn_id': 1, 'account_type': 'international', 'amount': 5000},
            {'txn_id': 2, 'account_type': 'domestic', 'amount': 2500}
        ]

    def account_check(**kwargs):
        """Determine routing based on account types present in data"""
        ti = kwargs['ti']
        transactions = ti.xcom_pull(task_ids='read_csv')
        account_types = {t['account_type'] for t in transactions}
        
        branches = []
        if 'international' in account_types:
            branches.append('route_to_fatca')
        if 'domestic' in account_types:
            branches.append('route_to_irs')
        return branches

    def route_to_fatca(**kwargs):
        """Mock FATCA reporting system integration"""
        ti = kwargs['ti']
        transactions = ti.xcom_pull(task_ids='read_csv')
        international_txns = [t for t in transactions if t['account_type'] == 'international']
        print(f"Processing {len(international_txns)} FATCA transactions")
        return 'fatca_report.xml'

    def route_to_irs(**kwargs):
        """Mock IRS reporting system integration"""
        ti = kwargs['ti']
        transactions = ti.xcom_pull(task_ids='read_csv')
        domestic_txns = [t for t in transactions if t['account_type'] == 'domestic']
        print(f"Processing {len(domestic_txns)} IRS transactions")
        return 'irs_1099_data.csv'

    def archive_reports(**kwargs):
        """Mock archiving process combining both report types"""
        ti = kwargs['ti']
        fatca = ti.xcom_pull(task_ids='route_to_fatca', default='No FATCA report')
        irs = ti.xcom_pull(task_ids='route_to_irs', default='No IRS report')
        print(f"Archiving combined reports:\nFATCA: {fatca}\nIRS: {irs}")

    read_csv_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
    )

    account_check_task = BranchPythonOperator(
        task_id='account_check',
        python_callable=account_check,
    )

    fatca_task = PythonOperator(
        task_id='route_to_fatca',
        python_callable=route_to_fatca,
    )

    irs_task = PythonOperator(
        task_id='route_to_irs',
        python_callable=route_to_irs,
    )

    archive_task = PythonOperator(
        task_id='archive_reports',
        python_callable=archive_reports,
        trigger_rule='none_failed'
    )

    read_csv_task >> account_check_task
    account_check_task >> [fatca_task, irs_task]
    [fatca_task, irs_task] >> archive_task