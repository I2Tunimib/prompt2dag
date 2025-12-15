from __future__ import annotations

import csv
import datetime
import os
import random
from typing import Any, Dict, List, Optional

from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def scan_csv(csv_path: str = "user_content.csv") -> Dict[str, Any]:
    """
    Scan a CSV file for user-generated content.

    Returns a dictionary with total number of items and the content rows.
    """
    if not os.path.isfile(csv_path):
        return {"total_items": 0, "content": []}

    content: List[Dict[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            content.append(row)

    return {"total_items": len(content), "content": content}


@task(retries=2, retry_delay_seconds=300)
def toxicity_check(metadata: Dict[str, Any], threshold: float = 0.7) -> str:
    """
    Evaluate toxicity of the content.

    Returns "high" if any item exceeds the threshold, otherwise "low".
    """
    for item in metadata.get("content", []):
        # Mock toxicity score; replace with real model inference as needed
        score = random.random()
        if score > threshold:
            return "high"
    return "low"


@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove toxic content and flag the associated user account.
    """
    # Placeholder logic for removal and flagging
    removed_count = metadata.get("total_items", 0)
    return {"action": "removed", "count": removed_count, "timestamp": datetime.datetime.utcnow().isoformat()}


@task(retries=2, retry_delay_seconds=300)
def publish_content(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publish safe content to the platform.
    """
    # Placeholder logic for publishing
    published_count = metadata.get("total_items", 0)
    return {"action": "published", "count": published_count, "timestamp": datetime.datetime.utcnow().isoformat()}


@task(retries=2, retry_delay_seconds=300)
def audit_log(
    remove_result: Optional[Dict[str, Any]],
    publish_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Create a final audit log entry after either removal or publishing completes.
    """
    entry: Dict[str, Any] = {
        "audit_timestamp": datetime.datetime.utcnow().isoformat(),
        "details": {},
    }

    if remove_result:
        entry["details"] = {
            "action": remove_result["action"],
            "count": remove_result["count"],
            "source_timestamp": remove_result["timestamp"],
        }
    elif publish_result:
        entry["details"] = {
            "action": publish_result["action"],
            "count": publish_result["count"],
            "source_timestamp": publish_result["timestamp"],
        }
    else:
        entry["details"] = {"action": "none", "count": 0}

    # In a real implementation, write to a persistent store or logging system
    return entry


@flow(name="content_moderation_pipeline")
def content_moderation_flow(csv_path: str = "user_content.csv") -> Dict[str, Any]:
    """
    Orchestrates the content moderation pipeline.

    The flow is intended to be scheduled to run daily.
    """
    scan_result = scan_csv(csv_path)
    decision = toxicity_check(scan_result)

    if decision == "high":
        remove_future = remove_and_flag_content.submit(scan_result)
        remove_output = remove_future.result()
        audit = audit_log(remove_output, None)
    else:
        publish_future = publish_content.submit(scan_result)
        publish_output = publish_future.result()
        audit = audit_log(None, publish_output)

    return audit


if __name__ == "__main__":
    # Local execution entry point
    result = content_moderation_flow()
    print("Audit Log:", result)