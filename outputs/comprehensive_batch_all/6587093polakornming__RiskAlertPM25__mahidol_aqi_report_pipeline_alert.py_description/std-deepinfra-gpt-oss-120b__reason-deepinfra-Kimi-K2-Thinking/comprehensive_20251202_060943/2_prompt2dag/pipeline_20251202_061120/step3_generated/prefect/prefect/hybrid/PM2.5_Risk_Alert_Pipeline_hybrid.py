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


@task(name='notify_pm25_email_alert', retries=0)
def notify_pm25_email_alert():
    """Task: Notify PM2.5 Email Alert"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_mahidol_aqi_postgres', retries=0)
def load_mahidol_aqi_postgres():
    """Task: Load Mahidol AQI into PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="pm2.5_risk_alert_pipeline",
    task_runner=SequentialTaskRunner(),
)
def pm2_5_risk_alert_pipeline():
    """Sequential pipeline for PM2.5 risk alert."""
    # Entry point
    extract_mahidol_aqi_html()
    # Transform depends on extraction
    transform_mahidol_aqi_json()
    # Load depends on transformation (and implicitly on any freshness checks)
    load_mahidol_aqi_postgres()
    # Notification depends on threshold checks (handled sequentially here)
    notify_pm25_email_alert()


if __name__ == "__main__":
    pm2_5_risk_alert_pipeline()