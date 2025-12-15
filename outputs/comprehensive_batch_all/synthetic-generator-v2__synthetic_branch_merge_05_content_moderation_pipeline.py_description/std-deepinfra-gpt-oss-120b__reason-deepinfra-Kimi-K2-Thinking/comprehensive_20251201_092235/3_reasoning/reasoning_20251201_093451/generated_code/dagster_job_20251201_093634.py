from dagster import (
    op, job, schedule, RetryPolicy, DefaultScheduleStatus,
    Out, Output, In, Nothing
)
from typing import Dict, Any, Optional
import random
import csv
import io
from datetime import datetime

# Mock CSV data for demonstration
MOCK_CSV_CONTENT = """content_id,text,user_id
1,"Great post about programming!",user_001
2,"Excellent work on the project",user_002
3,"Nice contribution to the discussion",user_003
"""


@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,  # 5 minutes in seconds
    ),
    description="Scans a CSV file for user-generated content"
)
def scan_csv(context) -> Dict[str, Any]:
    """
    Scans a CSV file and returns metadata about the content.
    In a real implementation, this would read from a file system or object store.
    """
    context.log.info("Scanning CSV file for user-generated content")
    
    # Mock CSV reading
    csv_data = io.StringIO(MOCK_CSV_CONTENT)
    reader = csv.DictReader(csv_data)
    
    content_items = []
    for row in reader:
        content_items.append({
            "content_id": row["content_id"],
            "text": row["text"],
            "user_id": row["user_id"]
        })
    
    total_items = len(content_items)
    context.log.info(f"Found {total_items} content items")
    
    return {
        "total_items": total_items,
        "content_items": content_items,
        "scan_timestamp": datetime.now().isoformat()
    }


@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    out={
        "remove_branch": Out(is_required=False),
        "publish_branch": Out(is_required=False)
    },
    description="Evaluates toxicity level and routes to appropriate branch"
)
def toxicity_check(context, scan_result: Dict[str, Any]):
    """
    Evaluates toxicity level of content and routes execution.
    Yields output to only one branch based on toxicity threshold of 0.7.
    """
    context.log.info(f"Evaluating toxicity for {scan_result['total_items']} items")
    
    # Mock toxicity scoring - in real implementation, this would call a model
    # For demonstration, we'll use a random score
    toxicity_score = random.random()
    context.log.info(f"Toxicity score: {toxicity_score:.2f}")
    
    # Route based on threshold
    if toxicity_score > 0.7:
        context.log.info("Toxicity above threshold (0.7) - routing to remove_and_flag_content branch")
        yield Output(scan_result, output_name="remove_branch")
    else:
        context.log.info("Toxicity below threshold (0.7) - routing to publish_content branch")
        yield Output(scan_result, output_name="publish_branch")


@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    description="Removes toxic content and flags user accounts"
)
def remove_and_flag_content(context, scan_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes toxic content from platform and flags associated users for review.
    """
    context.log.info("Executing remove_and_flag_content")
    item_ids = [item["content_id"] for item in scan_result["content_items"]]
    user_ids = [item["user_id"] for item in scan_result["content_items"]]
    
    context.log.info(f"Removing {len(item_ids)} toxic items: {item_ids}")
    context.log.info(f"Flagging users for review: {user_ids}")
    
    return {
        "action": "removed",
        "items_processed": len(item_ids),
        "item_ids": item_ids,
        "user_ids_flagged": user_ids,
        "timestamp": datetime.now().isoformat()
    }


@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    description="Publishes safe content to platform"
)
def publish_content(context, scan_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publishes safe content to the platform for user visibility.
    """
    context.log.info("Executing publish_content")
    item_ids = [item["content_id"] for item in scan_result["content_items"]]
    
    context.log.info(f"Publishing {len(item_ids)} safe items: {item_ids}")
    
    return {
        "action": "published",
        "items_processed": len(item_ids),
        "item_ids": item_ids,
        "timestamp": datetime.now().isoformat()
    }


@op(
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=300,
    ),
    description="Creates final audit log entry"
)
def audit_log(context, remove_result: Optional[Dict[str, Any]] = None, 
              publish_result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Creates audit log entry after either remove or publish path completes.
    Receives results from both branches but only one will have data.
    """
    context.log.info("Creating audit log entry")
    
    # Determine which branch executed
    if remove_result is not None:
        action_data = remove_result
        context.log.info("Audit log for removal action")
    elif publish_result is not None:
        action_data = publish_result
        context.log.info("Audit log for publish action")
    else:
        context.log.error("No result received from either branch")
        raise ValueError("No action results available for audit")
    
    audit_entry = {
        "timestamp": datetime.now().isoformat(),
        "action": action_data["action"],
        "items_processed": action_data["items_processed"],
        "item_ids": action_data["item_ids"],
        "status": "completed"
    }
    
    context.log.info(f"Audit entry created: {audit_entry}")
    return audit_entry


@job(
    description="Content moderation pipeline that scans, checks toxicity, and routes content"
)
def content_moderation_job():
    """
    Defines the content moderation pipeline with branch-merge pattern.
    """
    scan_data = scan_csv()
    remove_data, publish_data = toxicity_check(scan_data)
    
    remove_result = remove_and_flag_content(remove_data)
    publish_result = publish_content(publish_data)
    
    audit_log(remove_result=remove_result, publish_result=publish_result)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight
    job=content_moderation_job,
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily content moderation schedule"
)
def daily_content_moderation_schedule():
    """
    Schedule to run content moderation pipeline daily.
    """
    return {}


# Launch pattern for local execution
if __name__ == "__main__":
    result = content_moderation_job.execute_in_process()
    print(f"Job execution result: {result.success}")
    if result.success:
        print("Pipeline completed successfully")
    else:
        print("Pipeline failed")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")