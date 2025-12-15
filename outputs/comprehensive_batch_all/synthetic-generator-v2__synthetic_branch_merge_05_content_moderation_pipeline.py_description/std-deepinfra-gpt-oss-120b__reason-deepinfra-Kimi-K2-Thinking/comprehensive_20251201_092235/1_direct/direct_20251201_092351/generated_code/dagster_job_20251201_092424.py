from __future__ import annotations

import csv
import random
from typing import Dict, Any

from dagster import (
    op,
    job,
    graph,
    RetryPolicy,
    schedule,
    ConfigurableResource,
    resource,
    In,
    Out,
    Nothing,
)


class CSVResource(ConfigurableResource):
    """Simple resource to provide the path to the CSV file."""

    csv_path: str

    def get_path(self) -> str:
        return self.csv_path


@resource
def csv_resource(init_context) -> CSVResource:
    """Resource stub that reads the CSV path from config."""
    config = init_context.resource_config or {}
    path = config.get("csv_path", "data/user_content.csv")
    return CSVResource(csv_path=path)


@op(
    required_resource_keys={"csv"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(dict),
    description="Scans a CSV file for user-generated content and returns metadata.",
)
def scan_csv(context) -> Dict[str, Any]:
    """Read the CSV and return metadata such as total rows."""
    csv_path = context.resources.csv.get_path()
    context.log.info(f"Scanning CSV at path: {csv_path}")

    try:
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
    except FileNotFoundError:
        context.log.error(f"CSV file not found at {csv_path}. Returning empty result.")
        rows = []

    total_items = len(rows)
    context.log.info(f"Found {total_items} items in CSV.")
    return {"total_items": total_items, "rows": rows}


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(str),
    description="Evaluates toxicity and decides which downstream path to take.",
)
def toxicity_check(context, scan_data: Dict[str, Any]) -> str:
    """
    Mock toxicity evaluation.
    Returns "remove" if average toxicity > 0.7, else "publish".
    """
    rows = scan_data.get("rows", [])
    if not rows:
        context.log.warning("No rows to evaluate; defaulting to publish.")
        return "publish"

    # Mock toxicity scores between 0 and 1 for each row
    scores = [random.random() for _ in rows]
    avg_score = sum(scores) / len(scores)
    context.log.info(f"Average toxicity score: {avg_score:.2f}")

    decision = "remove" if avg_score > 0.7 else "publish"
    context.log.info(f"Toxicity decision: {decision}")
    return decision


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(dict),
    description="Removes toxic content and flags the user if decision is 'remove'.",
)
def remove_and_flag_content(
    context, scan_data: Dict[str, Any], decision: str
) -> Dict[str, Any]:
    if decision != "remove":
        context.log.info("Skipping removal; decision is not 'remove'.")
        return {"action": "skipped"}
    # Mock removal logic
    context.log.info("Removing toxic content and flagging user accounts.")
    return {"action": "removed", "items_processed": scan_data.get("total_items", 0)}


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(dict),
    description="Publishes safe content if decision is 'publish'.",
)
def publish_content(
    context, scan_data: Dict[str, Any], decision: str
) -> Dict[str, Any]:
    if decision != "publish":
        context.log.info("Skipping publishing; decision is not 'publish'.")
        return {"action": "skipped"}
    # Mock publishing logic
    context.log.info("Publishing safe content to the platform.")
    return {"action": "published", "items_processed": scan_data.get("total_items", 0)}


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Creates a final audit log entry after removal or publishing.",
)
def audit_log(
    context,
    removal_result: Dict[str, Any],
    publish_result: Dict[str, Any],
) -> None:
    """
    Consolidates results from both branches and logs an audit entry.
    """
    context.log.info("Generating audit log entry.")
    actions = {
        "removal": removal_result.get("action"),
        "publish": publish_result.get("action"),
    }
    items_processed = {
        "removal": removal_result.get("items_processed", 0),
        "publish": publish_result.get("items_processed", 0),
    }
    context.log.info(f"Actions taken: {actions}")
    context.log.info(f"Items processed per branch: {items_processed}")
    # In a real implementation, write to persistent storage or monitoring system.


@graph
def moderation_graph() -> None:
    scan = scan_csv()
    decision = toxicity_check(scan)
    removal = remove_and_flag_content(scan, decision)
    publishing = publish_content(scan, decision)
    audit_log(removal, publishing)


@job(resource_defs={"csv": csv_resource})
def moderation_job() -> None:
    moderation_graph()


@schedule(
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    job=moderation_job,
    execution_timezone="UTC",
)
def daily_moderation_schedule():
    """Schedule that runs the moderation job daily."""
    return {
        "resources": {
            "csv": {
                "config": {
                    "csv_path": "data/user_content.csv"
                }
            }
        }
    }


if __name__ == "__main__":
    result = moderation_job.execute_in_process(
        run_config={
            "resources": {
                "csv": {
                    "config": {
                        "csv_path": "data/user_content.csv"
                    }
                }
            }
        }
    )
    assert result.success