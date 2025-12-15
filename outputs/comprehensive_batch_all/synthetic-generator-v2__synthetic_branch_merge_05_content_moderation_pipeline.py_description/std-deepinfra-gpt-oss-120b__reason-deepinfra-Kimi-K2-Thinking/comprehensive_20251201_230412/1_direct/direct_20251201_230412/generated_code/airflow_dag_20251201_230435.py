import os
import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def scan_csv(**context):
    """
    Scan a CSV file containing user-generated content.
    Pushes the total number of items found to XCom.
    """
    csv_path = os.getenv("USER_CONTENT_CSV", "/tmp/user_content.csv")
    if not os.path.exists(csv_path):
        # Create a mock CSV if it does not exist
        df = pd.DataFrame(
            {
                "content_id": range(1, 6),
                "text": [
                    "I love this!",
                    "This is terrible.",
                    "Great job!",
                    "Awful experience.",
                    "Nice work.",
                ],
            }
        )
        df.to_csv(csv_path, index=False)

    df = pd.read_csv(csv_path)
    total_items = len(df)
    context["ti"].xcom_push(key="total_items", value=total_items)
    context["ti"].xcom_push(key="content_df", value=df.to_dict(orient="records"))
    return total_items


def decide_branch(**context):
    """
    Branching logic based on a mock toxicity score.
    Returns the task_id of the downstream path to follow.
    """
    # Retrieve content data (not used in mock scoring)
    content = context["ti"].xcom_pull(key="content_df", task_ids="scan_csv")
    # Mock toxicity score between 0 and 1
    toxicity_score = random.random()
    context["ti"].xcom_push(key="toxicity_score", value=toxicity_score)

    threshold = 0.7
    if toxicity_score >= threshold:
        return "remove_and_flag_content"
    return "publish_content"


def remove_and_flag_content(**context):
    """
    Handles removal of toxic content and flags the user.
    """
    score = context["ti"].xcom_pull(key="toxicity_score", task_ids="toxicity_check")
    result = {
        "action": "removed",
        "reason": f"Toxicity score {score:.2f} exceeds threshold",
    }
    context["ti"].xcom_push(key="action_result", value=result)
    return result


def publish_content(**context):
    """
    Publishes safe content to the platform.
    """
    score = context["ti"].xcom_pull(key="toxicity_score", task_ids="toxicity_check")
    result = {
        "action": "published",
        "reason": f"Toxicity score {score:.2f} below threshold",
    }
    context["ti"].xcom_push(key="action_result", value=result)
    return result


def audit_log(**context):
    """
    Consolidates information from previous tasks and creates an audit log entry.
    """
    total_items = context["ti"].xcom_pull(key="total_items", task_ids="scan_csv")
    toxicity_score = context["ti"].xcom_pull(key="toxicity_score", task_ids="toxicity_check")
    action_result = context["ti"].xcom_pull(key="action_result", task_ids=["remove_and_flag_content", "publish_content"])

    # action_result will be a list with a single dict (the executed branch)
    if isinstance(action_result, list):
        action_result = next((item for item in action_result if item), None)

    audit_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_items": total_items,
        "toxicity_score": toxicity_score,
        "action": action_result.get("action") if action_result else "none",
        "details": action_result.get("reason") if action_result else "No action taken",
    }

    # In a real implementation, this would be written to a persistent store.
    # Here we simply log it.
    print("Audit Log Entry:", audit_entry)
    return audit_entry


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="content_moderation_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["moderation", "example"],
) as dag:

    scan_csv_task = PythonOperator(
        task_id="scan_csv",
        python_callable=scan_csv,
        provide_context=True,
    )

    toxicity_check_task = BranchPythonOperator(
        task_id="toxicity_check",
        python_callable=decide_branch,
        provide_context=True,
    )

    remove_and_flag_task = PythonOperator(
        task_id="remove_and_flag_content",
        python_callable=remove_and_flag_content,
        provide_context=True,
    )

    publish_task = PythonOperator(
        task_id="publish_content",
        python_callable=publish_content,
        provide_context=True,
    )

    audit_log_task = PythonOperator(
        task_id="audit_log",
        python_callable=audit_log,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Define dependencies
    scan_csv_task >> toxicity_check_task
    toxicity_check_task >> [remove_and_flag_task, publish_task]
    [remove_and_flag_task, publish_task] >> audit_log_task