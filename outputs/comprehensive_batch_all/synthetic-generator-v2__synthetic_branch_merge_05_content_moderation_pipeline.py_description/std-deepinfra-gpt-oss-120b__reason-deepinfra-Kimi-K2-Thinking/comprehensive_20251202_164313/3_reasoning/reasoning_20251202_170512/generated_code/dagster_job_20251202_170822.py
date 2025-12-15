from dagster import op, job, In, Out, Nothing, Output, RetryPolicy
import pandas as pd
from typing import List, Dict, Any, Tuple

# Mock toxicity scorer
def mock_toxicity_score(text: str) -> float:
    # Simple mock: longer text = higher toxicity
    return min(len(text) / 1000, 1.0)

@op(
    out={
        "content_items": Out(List[Dict[str, Any]]),
        "total_count": Out(int)
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 5 min delay
)
def scan_csv(context):
    """
    Scans a CSV file for user-generated content.
    Returns content items and total count.
    """
    # For demo purposes, create mock data
    # In real usage, this would read from a file path provided via config
    csv_path = "user_content.csv"  # Could be from resource/config
    
    try:
        # Mock CSV reading
        df = pd.DataFrame({
            'user_id': [1, 2, 3],
            'content': ['Safe post', 'Toxic post ' * 100, 'Another safe post'],
            'timestamp': ['2023-01-01'] * 3
        })
        
        content_items = df.to_dict('records')
        total_count = len(content_items)
        
        context.log.info(f"Scanned {total_count} content items from {csv_path}")
        return Output(content_items, "content_items"), Output(total_count, "total_count")
    except Exception as e:
        context.log.error(f"Failed to scan CSV: {e}")
        raise

@op(
    ins={
        "content_items": In(List[Dict[str, Any]]),
        "total_count": In(int)
    },
    out={
        "routing_decision": Out(str),  # 'high' or 'low'
        "scored_content": Out(List[Dict[str, Any]])
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def toxicity_check(context, content_items: List[Dict[str, Any]], total_count: int):
    """
    Evaluates toxicity level of content and routes based on threshold 0.7.
    Returns routing decision and scored content.
    """
    # Calculate average toxicity across all content
    toxicity_scores = [mock_toxicity_score(item['content']) for item in content_items]
    avg_toxicity = sum(toxicity_scores) / len(toxicity_scores) if toxicity_scores else 0
    
    # Add scores to content items
    scored_content = [
        {**item, 'toxicity_score': score} 
        for item, score in zip(content_items, toxicity_scores)
    ]
    
    # Routing decision based on threshold
    threshold = 0.7
    routing_decision = "high" if avg_toxicity > threshold else "low"
    
    context.log.info(
        f"Average toxicity: {avg_toxicity:.3f} - Routing to {routing_decision} path"
    )
    
    return Output(routing_decision, "routing_decision"), Output(scored_content, "scored_content")

@op(
    ins={
        "scored_content": In(List[Dict[str, Any]]),
        "routing_decision": In(str)
    },
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def remove_and_flag_content(context, scored_content: List[Dict[str, Any]], routing_decision: str):
    """
    Removes toxic content and flags users. Only acts if routing_decision is 'high'.
    """
    if routing_decision != "high":
        context.log.info("Routing decision is 'low', skipping removal")
        return Output(None)
    
    # Process high toxicity content
    toxic_items = [item for item in scored_content if item['toxicity_score'] > 0.7]
    
    for item in toxic_items:
        user_id = item['user_id']
        context.log.info(f"Removing content from user {user_id} and flagging account")
        # Mock removal and flagging
        # In real implementation: call platform API, update database, etc.
    
    context.log.info(f"Removed and flagged {len(toxic_items)} toxic content items")
    return Output(None)

@op(
    ins={
        "scored_content": In(List[Dict[str, Any]]),
        "routing_decision": In(str)
    },
    out=Out(Nothing),
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def publish_content(context, scored_content: List[Dict[str, Any]], routing_decision: str):
    """
    Publishes safe content. Only acts if routing_decision is 'low'.
    """
    if routing_decision != "low":
        context.log.info("Routing decision is 'high', skipping publishing")
        return Output(None)
    
    # Process low toxicity content
    safe_items = [item for item in scored_content if item['toxicity_score'] <= 0.7]
    
    for item in safe_items:
        user_id = item['user_id']
        context.log.info(f"Publishing content from user {user_id}")
        # Mock publishing
        # In real implementation: call platform API, update database, etc.
    
    context.log.info(f"Published {len(safe_items)} safe content items")
    return Output(None)

@op(
    ins={
        "remove_result": In(Nothing),
        "publish_result": In(Nothing),
        "routing_decision": In(str),
        "total_count": In(int)
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def audit_log(context, remove_result, publish_result, routing_decision: str, total_count: int):
    """
    Creates audit log entry after either remove or publish path completes.
    """
    context.log.info("Creating audit log entry")
    
    # Collect action based on routing decision
    action_taken = "remove_and_flag" if routing_decision == "high" else "publish"
    
    log_entry = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "total_items_processed": total_count,
        "routing_decision": routing_decision,
        "action_taken": action_taken,
        "status": "completed"
    }
    
    # Mock writing to audit log
    # In real implementation: write to database, file, or logging service
    context.log.info(f"Audit log: {log_entry}")
    
    # Return the log entry for potential downstream use
    return log_entry

@job(
    description="Content moderation pipeline with toxicity checking and conditional routing"
)
def content_moderation_pipeline():
    """
    Content moderation pipeline that scans CSV, checks toxicity, 
    routes to remove or publish, and creates audit log.
    """
    # Step 1: Scan CSV
    content_items, total_count = scan_csv()
    
    # Step 2: Toxicity check (branching)
    routing_decision, scored_content = toxicity_check(content_items, total_count)
    
    # Step 3: Parallel branches (fan-out)
    # Both ops receive the same inputs but only one will act based on routing_decision
    remove_result = remove_and_flag_content(scored_content, routing_decision)
    publish_result = publish_content(scored_content, routing_decision)
    
    # Step 4: Audit log (fan-in)
    # Wait for both branches to complete (they'll complete even if they no-op)
    audit_log(remove_result, publish_result, routing_decision, total_count)

# Schedule for daily execution
from dagster import ScheduleDefinition

content_moderation_schedule = ScheduleDefinition(
    job=content_moderation_pipeline,
    cron_schedule="0 0 * * *",  # Daily at midnight
)

# Minimal launch pattern
if __name__ == '__main__':
    # Execute the job in-process for testing
    result = content_moderation_pipeline.execute_in_process()
    assert result.success