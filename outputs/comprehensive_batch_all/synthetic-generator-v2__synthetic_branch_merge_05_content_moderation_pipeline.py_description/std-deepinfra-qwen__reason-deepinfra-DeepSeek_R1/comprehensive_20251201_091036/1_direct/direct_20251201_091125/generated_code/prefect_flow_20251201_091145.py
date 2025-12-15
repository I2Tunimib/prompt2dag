from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def scan_csv(file_path: str) -> pd.DataFrame:
    """Scans a CSV file for user-generated content and returns metadata."""
    df = pd.read_csv(file_path)
    logger = get_run_logger()
    logger.info(f"Scanned CSV file with {len(df)} items.")
    return df

@task(retries=2, retry_delay_seconds=300)
def toxicity_check(content: pd.DataFrame) -> pd.DataFrame:
    """Evaluates the toxicity level of the content and routes to the appropriate task."""
    content['toxicity_score'] = content['text'].apply(lambda x: 0.6 if 'safe' in x else 0.8)
    logger = get_run_logger()
    logger.info(f"Evaluated toxicity for {len(content)} items.")
    return content

@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(toxic_content: pd.DataFrame) -> pd.DataFrame:
    """Removes toxic content and flags associated user accounts for review."""
    logger = get_run_logger()
    logger.info(f"Removed and flagged {len(toxic_content)} toxic items.")
    toxic_content['action'] = 'removed'
    return toxic_content

@task(retries=2, retry_delay_seconds=300)
def publish_content(safe_content: pd.DataFrame) -> pd.DataFrame:
    """Publishes safe content to the platform."""
    logger = get_run_logger()
    logger.info(f"Published {len(safe_content)} safe items.")
    safe_content['action'] = 'published'
    return safe_content

@task(retries=2, retry_delay_seconds=300)
def audit_log(results: list[pd.DataFrame]) -> None:
    """Creates a final audit log entry from the results of the remove and publish tasks."""
    combined_results = pd.concat(results)
    logger = get_run_logger()
    logger.info(f"Created audit log for {len(combined_results)} items.")
    combined_results.to_csv('audit_log.csv', index=False)

@flow
def content_moderation_pipeline(file_path: str):
    """Orchestrates the content moderation pipeline."""
    content = scan_csv(file_path)
    evaluated_content = toxicity_check(content)
    
    toxic_content = evaluated_content[evaluated_content['toxicity_score'] >= 0.7]
    safe_content = evaluated_content[evaluated_content['toxicity_score'] < 0.7]
    
    removed_content = remove_and_flag_content.submit(toxic_content)
    published_content = publish_content.submit(safe_content)
    
    audit_log([removed_content, published_content])

if __name__ == '__main__':
    # Example file path
    file_path = 'user_content.csv'
    content_moderation_pipeline(file_path)

# Optional: Schedule configuration (commented out)
# from prefect.orion.schemas.schedules import CronSchedule
# from prefect.deployments import Deployment
# deployment = Deployment.build_from_flow(
#     flow=content_moderation_pipeline,
#     name="daily-content-moderation",
#     schedule=CronSchedule(cron="0 0 * * *"),  # Run daily at midnight
# )
# deployment.apply()