from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='extract_mahidol_aqi_html', retries=0)
def extract_mahidol_aqi_html():
    """Task: Extract Mahidol AQI HTML"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_mahidol_aqi_json', retries=0)
def transform_mahidol_aqi_json():
    """Task: Transform Mahidol AQI to JSON"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_mahidol_aqi_postgres', retries=0)
def load_mahidol_aqi_postgres():
    """Task: Load Mahidol AQI into PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='notify_pm25_email', retries=0)
def notify_pm25_email():
    """Task: Notify PM2.5 Email Alert"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


# Placeholder tasks for branch dependencies
@task(name='duplicate_check_branch', retries=0)
def duplicate_check_branch():
    """Placeholder for duplicate check branching logic"""
    pass


@task(name='alert_decision_branch', retries=0)
def alert_decision_branch():
    """Placeholder for alert decision branching logic"""
    pass


@flow(
    name="pm2.5_risk_alert_pipeline",
    task_runner=SequentialTaskRunner(),
)
def pm2_5_risk_alert_pipeline():
    # Entry point
    extract_future = extract_mahidol_aqi_html.submit()

    # Transform depends on extraction
    transform_future = transform_mahidol_aqi_json.submit(wait_for=[extract_future])

    # Branch for duplicate check before loading
    duplicate_check_future = duplicate_check_branch.submit()
    load_future = load_mahidol_aqi_postgres.submit(wait_for=[duplicate_check_future])

    # Branch for alert decision before notification
    alert_decision_future = alert_decision_branch.submit()
    notify_future = notify_pm25_email.submit(wait_for=[alert_decision_future])

    # Ensure all tasks complete (optional)
    return {
        "extract": extract_future,
        "transform": transform_future,
        "duplicate_check": duplicate_check_future,
        "load": load_future,
        "alert_decision": alert_decision_future,
        "notify": notify_future,
    }


if __name__ == "__main__":
    pm2_5_risk_alert_pipeline()