from prefect import flow, task
import csv
import random
from datetime import datetime
from typing import Dict, Any


@task(retries=2, retry_delay_seconds=300)
def scan_csv(file_path: str) -> Dict[str, Any]:
    """Scans a CSV file for user-generated content and returns metadata."""
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            items = list(reader)
        
        total_items = len(items) - 1 if len(items) > 0 else 0
        return {
            "total_items": total_items,
            "file_path": file_path,
            "status": "scanned"
        }
    except FileNotFoundError:
        return {
            "total_items": 0,
            "file_path": file_path,
            "status": "file_not_found"
        }


@task(retries=2, retry_delay_seconds=300)
def toxicity_check(content_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluates toxicity level and returns routing decision."""
    toxicity_score = random.random()
    is_toxic = toxicity_score > 0.7
    
    return {
        "is_toxic": is_toxic,
        "toxicity_score": toxicity_score,
        "content_metadata": content_metadata
    }


@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(toxicity_result: Dict[str, Any]) -> Dict[str, Any]:
    """Removes toxic content and flags user account for review."""
    content_metadata = toxicity_result["content_metadata"]
    
    return {
        "action": "removed_and_flagged",
        "content_metadata": content_metadata,
        "toxicity_score": toxicity_result["toxicity_score"],
        "status": "completed"
    }


@task(retries=2, retry_delay_seconds=300)
def publish_content(toxicity_result: Dict[str, Any]) -> Dict[str, Any]:
    """Publishes safe content to the platform."""
    content_metadata = toxicity_result["content_metadata"]
    
    return {
        "action": "published",
        "content_metadata": content_metadata,
        "toxicity_score": toxicity_result["toxicity_score"],
        "status": "completed"
    }


@task(retries=2, retry_delay_seconds=300)
def audit_log(action_result: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a final audit log entry after either branch completes."""
    action = action_result["action"]
    content_metadata = action_result["content_metadata"]
    toxicity_score = action_result["toxicity_score"]
    
    log_entry = {
        "action": action,
        "total_items_processed": content_metadata["total_items"],
        "toxicity_score": round(toxicity_score, 3),
        "status": "logged",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return log_entry


@flow(name="content-moderation-pipeline")
def content_moderation_pipeline(csv_file_path: str = "user_content.csv"):
    """
    Content moderation pipeline with conditional branching.
    
    Schedule: Daily (configure at deployment with: 
    prefect deployment build --schedule "0 0 * * *")
    """
    scan_result = scan_csv(csv_file_path)
    
    toxicity_result = toxicity_check(scan_result)
    
    if toxicity_result["is_toxic"]:
        action_result = remove_and_flag_content(toxicity_result)
    else:
        action_result = publish_content(toxicity_result)
    
    audit_result = audit_log(action_result)
    
    return audit_result


if __name__ == "__main__":
    result = content_moderation_pipeline()
    print(f"Pipeline completed: {result}")