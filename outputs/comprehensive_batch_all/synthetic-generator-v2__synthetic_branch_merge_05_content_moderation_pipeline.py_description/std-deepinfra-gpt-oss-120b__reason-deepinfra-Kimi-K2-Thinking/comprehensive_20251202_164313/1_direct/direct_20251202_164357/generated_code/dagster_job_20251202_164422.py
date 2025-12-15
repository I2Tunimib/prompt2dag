from __future__ import annotations

import csv
import random
from typing import Any, Dict, List, Optional

from dagster import (
    In,
    Out,
    RetryPolicy,
    Config,
    OpExecutionContext,
    op,
    job,
)


class ScanCsvConfig(Config):
    """Configuration for the scan_csv op."""

    csv_path: str = "user_content.csv"


@op(
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema=ScanCsvConfig,
    description="Scans a CSV file for user-generated content and returns metadata.",
)
def scan_csv(context: OpExecutionContext) -> Dict[str, Any]:
    """Read a CSV file and return its rows and a total count.

    The CSV is expected to have a header row with at least a 'content' column.
    """
    csv_path = context.op_config["csv_path"]
    rows: List[Dict[str, str]] = []
    try:
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rows.append(row)
    except FileNotFoundError:
        context.log.error(f"CSV file not found at path: {csv_path}")
        rows = []

    total_items = len(rows)
    context.log.info(f"Scanned {total_items} items from {csv_path}")
    return {"rows": rows, "total_items": total_items}


@op(
    ins={"csv_data": In(dict)},
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Evaluates toxicity levels and decides routing.",
)
def toxicity_check(context: OpExecutionContext, csv_data: Dict[str, Any]) -> Dict[str, Any]:
    """Mock toxicity evaluation.

    Assigns a random toxicity score to each content item and determines if any
    exceed the threshold of 0.7.
    """
    rows = csv_data.get("rows", [])
    threshold = 0.7
    high_toxicity = False
    evaluated_items = []

    for row in rows:
        content = row.get("content", "")
        score = random.random()  # Mock score between 0 and 1
        evaluated_items.append({"content": content, "toxicity_score": score})
        if score > threshold:
            high_toxicity = True

    context.log.info(
        f"Toxicity check completed. High toxicity detected: {high_toxicity}"
    )
    return {"high_toxicity": high_toxicity, "evaluated_items": evaluated_items}


@op(
    ins={"toxicity_info": In(dict)},
    out=Out(Optional(dict)),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Removes toxic content and flags the user when toxicity is high.",
)
def remove_and_flag_content(
    context: OpExecutionContext, toxicity_info: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Process removal and flagging if high toxicity is detected."""
    if not toxicity_info.get("high_toxicity"):
        context.log.info("No high toxicity detected; skipping removal.")
        return None

    items = toxicity_info.get("evaluated_items", [])
    removed_items = [item for item in items if item["toxicity_score"] > 0.7]

    # Mock removal and flagging actions
    context.log.info(f"Removing {len(removed_items)} toxic items and flagging users.")
    return {"action": "remove_and_flag", "removed_count": len(removed_items)}


@op(
    ins={"toxicity_info": In(dict)},
    out=Out(Optional(dict)),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Publishes safe content when toxicity is low.",
)
def publish_content(
    context: OpExecutionContext, toxicity_info: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Publish content if no high toxicity is detected."""
    if toxicity_info.get("high_toxicity"):
        context.log.info("High toxicity detected; skipping publishing.")
        return None

    items = toxicity_info.get("evaluated_items", [])
    safe_items = [item for item in items if item["toxicity_score"] <= 0.7]

    # Mock publishing action
    context.log.info(f"Publishing {len(safe_items)} safe items.")
    return {"action": "publish", "published_count": len(safe_items)}


@op(
    ins={
        "remove_result": In(Optional(dict)),
        "publish_result": In(Optional(dict)),
    },
    out=Out(dict),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Creates a unified audit log entry after branch execution.",
)
def audit_log(
    context: OpExecutionContext,
    remove_result: Optional[Dict[str, Any]],
    publish_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Combine results from both branches and log an audit entry."""
    log_entry = {
        "remove_action": remove_result,
        "publish_action": publish_result,
    }
    context.log.info(f"Audit Log Entry: {log_entry}")
    return log_entry


@job(description="Content moderation pipeline with branching and merging.")
def content_moderation_job():
    csv_data = scan_csv()
    tox_info = toxicity_check(csv_data)
    remove_res = remove_and_flag_content(tox_info)
    publish_res = publish_content(tox_info)
    audit_log(remove_res, publish_res)


if __name__ == "__main__":
    result = content_moderation_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")