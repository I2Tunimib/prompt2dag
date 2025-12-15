from pathlib import Path
import csv
import random
from typing import List, Dict, Any

from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def scan_csv(file_path: str = "user_content.csv") -> Dict[str, Any]:
    """Read a CSV file and return metadata and rows.

    Returns:
        dict: Contains ``total_items`` and ``rows`` (list of dicts).
    """
    path = Path(file_path)
    if not path.is_file():
        return {"total_items": 0, "rows": []}

    rows: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    return {"total_items": len(rows), "rows": rows}


@task(retries=2, retry_delay_seconds=300)
def toxicity_check(content_rows: List[Dict[str, Any]], threshold: float = 0.7) -> bool:
    """Mock toxicity scoring.

    Returns True if the average mock score exceeds the threshold.
    """
    if not content_rows:
        return False

    scores = [random.random() for _ in content_rows]
    avg_score = sum(scores) / len(scores)
    return avg_score >= threshold


@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Mock removal of toxic content and flagging of the user."""
    return {
        "action": "removed",
        "details": f"{metadata['total_items']} items removed due to high toxicity",
    }


@task(retries=2, retry_delay_seconds=300)
def publish_content(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Mock publishing of safe content."""
    return {
        "action": "published",
        "details": f"{metadata['total_items']} items published as safe content",
    }


@task(retries=2, retry_delay_seconds=300)
def audit_log(scan_result: Dict[str, Any], branch_result: Dict[str, Any]) -> None:
    """Create a unified audit log entry."""
    log_entry = {
        "total_items": scan_result["total_items"],
        "action": branch_result["action"],
        "details": branch_result["details"],
    }
    print("AUDIT LOG:", log_entry)


@flow
def content_moderation_flow(csv_path: str = "user_content.csv"):
    """Orchestrates the content moderation pipeline."""
    scan_result = scan_csv(csv_path)
    high_toxicity = toxicity_check(scan_result["rows"])

    if high_toxicity:
        branch_future = remove_and_flag_content.submit(scan_result)
    else:
        branch_future = publish_content.submit(scan_result)

    branch_result = branch_future.result()
    audit_log(scan_result, branch_result)


if __name__ == "__main__":
    # In production this flow would be deployed with a daily schedule.
    content_moderation_flow()