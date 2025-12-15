from prefect import flow, task
import pandas as pd
from typing import Dict, Any, List
import time
import random
from datetime import datetime

# Mock CSV path for demonstration
MOCK_CSV_PATH = "user_content.csv"


@task(retries=2, retry_delay_seconds=300)
def scan_csv(csv_path: str) -> Dict[str, Any]:
    """Scans a CSV file for user-generated content and returns metadata."""
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        # Create mock CSV with varied content if it doesn't exist
        mock_data = {
            "content_id": list(range(1, 11)),
            "user_id": [100 + i for i in range(10)],
            "content_text": [
                "Great post! Thanks for sharing.",
                "This is awful and toxic content, I hate it.",
                "Nice work everyone, keep it up!",
                "Terrible, hate this content and the users.",
                "Helpful information, much appreciated.",
                "You're all stupid and wrong.",
                "Excellent points, well made.",
                "Worst content ever, disgusting.",
                "Thanks for the useful info.",
                "Hate this, hate you all."
            ]
        }
        df = pd.DataFrame(mock_data)
        df.to_csv(csv_path, index=False)
    
    return {
        "data": df.to_dict("records"),
        "total_items": len(df),
        "csv_path": csv_path,
        "scan_timestamp": datetime.now().isoformat()
    }


@task(retries=2, retry_delay_seconds=300)
def toxicity_check(content_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluates content toxicity level and determines routing decision.
    Returns a dictionary with routing information.
    """
    data = content_data["data"]
    
    # Mock toxicity scoring based on keyword detection
    toxic_indicators = ["hate", "awful", "terrible", "stupid", "worst", "disgusting"]
    
    toxic_count = sum(
        1 for item in data 
        if any(word in item["content_text"].lower() for word in toxic_indicators)
    )
    
    # Calculate toxicity ratio for the batch
    toxicity_ratio = toxic_count / len(data) if data else 0
    toxicity_score = min(toxicity_ratio + random.uniform(0.0, 0.2), 0.95)
    threshold = 0.7
    
    return {
        "toxicity_score": round(toxicity_score, 3),
        "threshold": threshold,
        "route_to_removal": toxicity_score >= threshold,
        "content_data": content_data,
        "toxic_items_count": toxic_count
    }


@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(toxicity_result: Dict[str, Any]) -> Dict[str, Any]:
    """Removes toxic content from the platform and flags associated user accounts."""
    return {
        "action": "remove_and_flag",
        "content_removed": True,
        "items_removed": toxicity_result["toxic_items_count"],
        "users_flagged_count": toxicity_result["toxic_items_count"],
        "timestamp": datetime.now().isoformat(),
        "toxicity_score": toxicity_result["toxicity_score"],
        "status": "completed"
    }


@task(retries=2, retry_delay_seconds=300)
def publish_content(toxicity_result: Dict[str, Any]) -> Dict[str, Any]:
    """Publishes safe content to the platform for user visibility."""
    total_items = toxicity_result["content_data"]["total_items"]
    toxic_items = toxicity_result["toxic_items_count"]
    safe_items = total_items - toxic_items
    
    return {
        "action": "publish",
        "content_published": True,
        "items_published": safe_items,
        "timestamp": datetime.now().isoformat(),
        "toxicity_score": toxicity_result["toxicity_score"],
        "status": "completed"
    }


@task(retries=2, retry_delay_seconds=300)
def audit_log(action_result: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a unified audit log entry after either branch completes."""
    log_entry = {
        "audit_id": f"audit_{int(time.time())}",
        "action_taken": action_result["action"],
        "timestamp": datetime.now().isoformat(),
        "action_details": action_result,
        "status": "logged"
    }
    print(f"Audit log created: {log_entry['audit_id']}")
    return log_entry


@flow(name="content-moderation-pipeline")
def content_moderation_flow(csv_path: str = MOCK_CSV_PATH) -> Dict[str, Any]:
    """
    Content moderation pipeline with branch-merge pattern.
    Processes CSV content through toxicity check and conditional routing.
    """
    # Step 1: Scan CSV file
    scan_future = scan_csv.submit(csv_path)
    
    # Step 2: Evaluate toxicity and determine routing
    toxicity_future = toxicity_check.submit(scan_future)
    
    # Retrieve result to make branching decision
    toxicity_data = toxicity_future.result()
    
    # Step 3: Execute conditional branch (only one path runs)
    if toxicity_data["route_to_removal"]:
        action_future = remove_and_flag_content.submit(toxicity_data)
    else:
        action_future = publish_content.submit(toxicity_data)
    
    # Step 4: Merge path - audit log runs after either branch completes
    audit_future = audit_log.submit(action_future)
    
    return audit_future.result()


if __name__ == "__main__":
    # Execute flow locally
    # For daily scheduling, deploy with:
    # prefect deployment build content_moderation_flow.py:content_moderation_flow --name daily-moderation --cron "0 9 * * *"
    # prefect deployment apply content_moderation_flow-deployment.yaml
    result = content_moderation_flow()
    print(f"Flow completed with result: {result}")