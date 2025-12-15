import csv
import random
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import quote


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Read user‑generated content from a CSV file and return metadata.",
)
def scan_csv(csv_path: str) -> List[Dict[str, Any]]:
    """
    Scan a CSV file for user‑generated content.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        A list of dictionaries representing each row in the CSV.
        If the file does not exist, returns an empty list.
    """
    path = Path(csv_path)
    if not path.is_file():
        return []

    rows: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    return rows


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Evaluate toxicity of content and decide routing.",
)
def toxicity_check(content: List[Dict[str, Any]]) -> bool:
    """
    Mock toxicity evaluation.

    Generates a random toxicity score for each piece of content,
    computes the average, and decides if the overall batch is
    considered highly toxic.

    Args:
        content: List of content rows.

    Returns:
        True if average toxicity > 0.7 (high toxicity), else False.
    """
    if not content:
        return False

    scores = [random.random() for _ in content]
    average_score = sum(scores) / len(scores)
    return average_score > 0.7


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Remove toxic content and flag the user.",
)
def remove_and_flag_content(content: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simulate removal of toxic content and flagging of the associated user.

    Args:
        content: List of content rows.

    Returns:
        Summary dictionary of the removal action.
    """
    removed_count = len(content)
    # In a real implementation, removal and flagging logic would go here.
    result = {
        "action": "remove_and_flag",
        "removed_items": removed_count,
        "timestamp": datetime.utcnow().isoformat(),
    }
    return result


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Publish safe content to the platform.",
)
def publish_content(content: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simulate publishing of safe content.

    Args:
        content: List of content rows.

    Returns:
        Summary dictionary of the publishing action.
    """
    published_count = len(content)
    # In a real implementation, publishing logic would go here.
    result = {
        "action": "publish",
        "published_items": published_count,
        "timestamp": datetime.utcnow().isoformat(),
    }
    return result


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Create a unified audit log entry after processing.",
)
def audit_log(
    scan_result: List[Dict[str, Any]],
    action_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Consolidate results into an audit log entry.

    Args:
        scan_result: Original content rows from the CSV.
        action_result: Result from either removal or publishing task.

    Returns:
        A dictionary representing the audit log entry.
    """
    log_entry = {
        "total_items_scanned": len(scan_result),
        "action_taken": action_result.get("action"),
        "items_processed": (
            action_result.get("removed_items")
            or action_result.get("published_items")
            or 0
        ),
        "timestamp": datetime.utcnow().isoformat(),
    }
    # In a real implementation, this would be persisted to a logging system.
    return log_entry


@flow
def content_moderation_flow(csv_path: str = "user_content.csv") -> Dict[str, Any]:
    """
    Orchestrate the content moderation pipeline.

    The flow:
    1. Scan the CSV for content.
    2. Evaluate toxicity.
    3. Branch to either removal/flagging or publishing.
    4. Create a unified audit log.

    Args:
        csv_path: Path to the CSV file containing user content.

    Returns:
        The final audit log entry.
    """
    # Step 1: Scan CSV
    scanned_content = scan_csv(csv_path)

    # Step 2: Toxicity check
    high_toxic = toxicity_check(scanned_content)

    # Step 3: Conditional branch
    if high_toxic:
        action_outcome = remove_and_flag_content(scanned_content)
    else:
        action_outcome = publish_content(scanned_content)

    # Step 4: Merge and audit
    audit = audit_log(scanned_content, action_outcome)
    return audit


# The flow is intended to run daily. Deployment and schedule configuration
# should be defined separately using Prefect's deployment utilities.

if __name__ == "__main__":
    # Local execution for testing/debugging
    result = content_moderation_flow()
    print("Audit Log:", result)