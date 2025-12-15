from pathlib import Path
import csv
import random
from typing import List, Dict, Any

from dagster import (
    op,
    job,
    RetryPolicy,
    In,
    Out,
    Nothing,
    ConfigurableResource,
    Config,
    Field,
    String,
    Int,
    Float,
)


class CSVResource(ConfigurableResource):
    """Simple resource to provide the path to the CSV file."""

    path: str = Field(
        default_value="data/content.csv",
        description="Path to the CSV file containing user‑generated content.",
    )

    def get_path(self) -> Path:
        return Path(self.path)


@op(
    out=Out(List[Dict[str, Any]]),
    required_resource_keys={"csv"},
    description="Scans a CSV file for user‑generated content and returns rows as dictionaries.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def scan_csv(context) -> List[Dict[str, Any]]:
    csv_path = context.resources.csv.get_path()
    if not csv_path.is_file():
        context.log.warning(f"CSV file not found at {csv_path}. Returning empty list.")
        return []

    rows: List[Dict[str, Any]] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    context.log.info(f"Scanned {len(rows)} rows from {csv_path}.")
    return rows


@op(
    ins={"content": In(List[Dict[str, Any]])},
    out=Out(bool),
    description="Evaluates toxicity level of the content. Returns True if average toxicity > 0.7.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def toxicity_check(context, content: List[Dict[str, Any]]) -> bool:
    if not content:
        context.log.info("No content to evaluate. Assuming low toxicity.")
        return False

    # Mock toxicity scoring: assign a random score between 0 and 1 to each item.
    scores = [random.random() for _ in content]
    avg_score = sum(scores) / len(scores)
    context.log.info(f"Average toxicity score: {avg_score:.3f}")

    high_toxicity = avg_score > 0.7
    if high_toxicity:
        context.log.warning("Content classified as HIGH toxicity.")
    else:
        context.log.info("Content classified as LOW toxicity.")
    return high_toxicity


@op(
    ins={"high_toxicity": In(bool), "content": In(List[Dict[str, Any]])},
    out=Out(Dict[str, Any]),
    description="Removes toxic content and flags the user if toxicity is high.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def remove_and_flag_content(context, high_toxicity: bool, content: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not high_toxicity:
        context.log.info("Skipping removal because toxicity is low.")
        return {}

    # Mock removal: pretend we removed all items.
    removed_count = len(content)
    context.log.warning(f"Removed {removed_count} toxic items and flagged associated users.")
    return {"removed_count": removed_count, "action": "remove_and_flag"}


@op(
    ins={"high_toxicity": In(bool), "content": In(List[Dict[str, Any]])},
    out=Out(Dict[str, Any]),
    description="Publishes safe content if toxicity is low.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def publish_content(context, high_toxicity: bool, content: List[Dict[str, Any]]) -> Dict[str, Any]:
    if high_toxicity:
        context.log.info("Skipping publishing because toxicity is high.")
        return {}

    # Mock publishing: pretend we published all items.
    published_count = len(content)
    context.log.info(f"Published {published_count} safe items.")
    return {"published_count": published_count, "action": "publish"}


@op(
    ins={
        "remove_result": In(Dict[str, Any]),
        "publish_result": In(Dict[str, Any]),
    },
    out=Out(Nothing),
    description="Creates a unified audit log entry after removal or publishing.",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def audit_log(context, remove_result: Dict[str, Any], publish_result: Dict[str, Any]) -> Nothing:
    # Combine results for logging.
    if remove_result:
        context.log.info(f"Audit Log - Removal: {remove_result}")
    if publish_result:
        context.log.info(f"Audit Log - Publishing: {publish_result}")

    context.log.info("Audit log entry created.")
    return Nothing


@job(
    resource_defs={"csv": CSVResource},
    description="Content moderation pipeline with conditional branching and merge.",
)
def content_moderation_job():
    content = scan_csv()
    high = toxicity_check(content)

    removal = remove_and_flag_content(high_toxicity=high, content=content)
    publishing = publish_content(high_toxicity=high, content=content)

    audit_log(remove_result=removal, publish_result=publishing)


if __name__ == "__main__":
    result = content_moderation_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")