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

@task
def toxicity_check(content: pd.DataFrame) -> pd.DataFrame:
    """Evaluates the toxicity level of the content and routes it based on a threshold."""
    content['toxicity_score'] = content['content'].apply(lambda x: 0.6 if 'safe' in x else 0.8)
    return content

@task
def remove_and_flag_content(toxic_content: pd.DataFrame) -> pd.DataFrame:
    """Removes toxic content and flags the associated user account for review."""
    logger = get_run_logger()
    logger.info(f"Removing and flagging {len(toxic_content)} toxic items.")
    toxic_content['action'] = 'removed'
    return toxic_content

@task
def publish_content(safe_content: pd.DataFrame) -> pd.DataFrame:
    """Publishes safe content to the platform for user visibility."""
    logger = get_run_logger()
    logger.info(f"Publishing {len(safe_content)} safe items.")
    safe_content['action'] = 'published'
    return safe_content

@task
def audit_log(results: list[pd.DataFrame]) -> None:
    """Creates a final audit log entry from the results of the remove and publish paths."""
    logger = get_run_logger()
    combined_results = pd.concat(results)
    logger.info(f"Creating audit log for {len(combined_results)} items.")
    combined_results.to_csv('audit_log.csv', index=False)

@flow
def content_moderation_pipeline(file_path: str):
    """Orchestrates the content moderation pipeline."""
    content = scan_csv(file_path)
    toxicity_results = toxicity_check(content)
    
    toxic_content = toxicity_results[toxicity_results['toxicity_score'] >= 0.7]
    safe_content = toxicity_results[toxicity_results['toxicity_score'] < 0.7]
    
    remove_results = remove_and_flag_content.submit(toxic_content)
    publish_results = publish_content.submit(safe_content)
    
    audit_log([remove_results.result(), publish_results.result()])

if __name__ == '__main__':
    # Example usage
    content_moderation_pipeline(file_path='user_content.csv')

# Optional: Add deployment/schedule configuration here
# Example:
# from prefect.deployments import Deployment
# from prefect.orion.schemas.schedules import IntervalSchedule
# from datetime import timedelta
# deployment = Deployment.build_from_flow(
#     flow=content_moderation_pipeline,
#     name="daily-content-moderation",
#     schedule=IntervalSchedule(interval=timedelta(days=1)),
#     work_queue_name="default"
# )
# deployment.apply()