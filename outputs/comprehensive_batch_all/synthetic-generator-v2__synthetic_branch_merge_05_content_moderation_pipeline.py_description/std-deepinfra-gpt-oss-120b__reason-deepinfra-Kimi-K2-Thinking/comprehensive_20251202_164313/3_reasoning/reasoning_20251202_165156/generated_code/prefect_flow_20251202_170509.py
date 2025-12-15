import csv
import random
from datetime import datetime
from typing import Dict, List, Any

from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def scan_csv(file_path: str) -> Dict[str, Any]:
    """
    Scans a CSV file for user-generated content and returns metadata.
    
    Args:
        file_path: Path to the CSV file containing user content
        
    Returns:
        Dictionary containing content items and total count
    """
    content_items = []
    
    try:
        with open(file_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                content_items.append({
                    'content_id': row.get('content_id', f'content_{len(content_items)}'),
                    'user_id': row.get('user_id', f'user_{random.randint(1, 1000)}'),
                    'content_text': row.get('content_text', 'Sample user content'),
                    'toxicity_score': float(row.get('toxicity_score', random.random()))
                })
    except FileNotFoundError:
        # Generate mock data for demonstration if file doesn't exist
        for i in range(10):
            content_items.append({
                'content_id': f'content_{i}',
                'user_id': f'user_{random.randint(1, 1000)}',
                'content_text': f'Sample user content {i}',
                'toxicity_score': random.random()
            })
    
    return {
        'total_items': len(content_items),
        'content_items': content_items,
        'scan_timestamp': datetime.utcnow().isoformat()
    }


@task(retries=2, retry_delay_seconds=300)
def toxicity_check(content_metadata: Dict[str, Any], threshold: float = 0.7) -> Dict[str, Any]:
    """
    Evaluates toxicity level and routes execution based on threshold.
    
    Args:
        content_metadata: Metadata from scan_csv including content items
        threshold: Toxicity threshold for routing
        
    Returns:
        Dictionary with routing decision and processed data
    """
    content_items = content_metadata.get('content_items', [])
    
    if not content_items:
        return {
            'is_toxic': False,
            'content_items': [],
            'average_toxicity': 0.0,
            'routing_decision': 'no_content'
        }
    
    avg_toxicity = sum(item['toxicity_score'] for item in content_items) / len(content_items)
    is_toxic = avg_toxicity > threshold
    
    return {
        'is_toxic': is_toxic,
        'content_items': content_items,
        'average_toxicity': avg_toxicity,
        'threshold': threshold,
        'routing_decision': 'remove' if is_toxic else 'publish'
    }


@task(retries=2, retry_delay_seconds=300)
def remove_and_flag_content(toxicity_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes toxic content and flags user accounts for review.
    
    Args:
        toxicity_data: Data from toxicity check containing toxic content
        
    Returns:
        Action result with details of removed content
    """
    content_items = toxicity_data.get('content_items', [])
    removed_items = []
    
    for item in content_items:
        removed_items.append({
            'content_id': item['content_id'],
            'user_id': item['user_id'],
            'action': 'removed',
            'reason': 'high_toxicity',
            'toxicity_score': item['toxicity_score']
        })
    
    return {
        'action_type': 'remove_and_flag',
        'processed_items': len(removed_items),
        'removed_items': removed_items,
        'timestamp': datetime.utcnow().isoformat()
    }


@task(retries=2, retry_delay_seconds=300)
def publish_content(toxicity_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publishes safe content to the platform.
    
    Args:
        toxicity_data: Data from toxicity check containing safe content
        
    Returns:
        Action result with details of published content
    """
    content_items = toxicity_data.get('content_items', [])
    published_items = []
    
    for item in content_items:
        published_items.append({
            'content_id': item['content_id'],
            'user_id': item['user_id'],
            'action': 'published',
            'toxicity_score': item['toxicity_score']
        })
    
    return {
        'action_type': 'publish',
        'processed_items': len(published_items),
        'published_items': published_items,
        'timestamp': datetime.utcnow().isoformat()
    }


@task(retries=2, retry_delay_seconds=300)
def audit_log(action_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a final audit log entry after either branch completes.
    
    Args:
        action_result: Result from either remove_and_flag_content or publish_content
        
    Returns:
        Audit log entry
    """
    log_entry = {
        'audit_id': f'audit_{int(datetime.utcnow().timestamp())}',
        'action_type': action_result.get('action_type'),
        'processed_items': action_result.get('processed_items', 0),
        'timestamp': datetime.utcnow().isoformat(),
        'details': action_result
    }
    
    print(f"AUDIT LOG: {log_entry}")
    
    return log_entry


@flow(name="content-moderation-pipeline")
def content_moderation_flow(csv_path: str = "user_content.csv"):
    """
    Content moderation pipeline with conditional branching.
    
    Scheduled to run daily. Retries configured for 2 attempts
    with 5-minute delays between retries.
    """
    # Step 1: Scan CSV for content
    scan_future = scan_csv.submit(csv_path)
    
    # Step 2: Check toxicity and determine routing
    toxicity_future = toxicity_check.submit(scan_future)
    
    # Branching decision point - wait for toxicity result
    toxicity_data = toxicity_future.result()
    
    # Step 3: Execute conditional branch
    if toxicity_data['routing_decision'] == 'remove':
        # High toxicity path
        action_future = remove_and_flag_content.submit(toxicity_data)
    else:
        # Low toxicity path
        action_future = publish_content.submit(toxicity_data)
    
    # Step 4: Merge - audit log runs after branch completes
    audit_log.submit(action_future)


if __name__ == '__main__':
    # Local execution for testing
    # For production deployment with daily schedule, use:
    # prefect deployment build content_moderation_flow:content_moderation_flow --name daily-moderation --cron "0 0 * * *"
    content_moderation_flow()