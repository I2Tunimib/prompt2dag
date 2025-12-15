from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task(name='extract_mahidol_aqi_html', retries=0)
def extract_mahidol_aqi_html():
    """Task: Extract Mahidol AQI HTML"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_html_to_json', retries=0)
def transform_html_to_json():
    """Task: Transform HTML to Structured JSON"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_mahidol_aqi_to_warehouse', retries=0)
def load_mahidol_aqi_to_warehouse():
    """Task: Load AQI Data to PostgreSQL Warehouse"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='notify_pm25_alert', retries=0)
def notify_pm25_alert():
    """Task: PM2.5 Email Alert Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="pm2.5_risk_alert_pipeline",
    task_runner=SequentialTaskRunner(),
)
def pm2_5_risk_alert_pipeline():
    """
    Sequential ETL pipeline:
    1. Extract Mahidol AQI HTML
    2. Transform HTML to JSON
    3. Load JSON into PostgreSQL warehouse
    4. Send email alert if PM2.5 exceeds thresholds
    """
    extract_mahidol_aqi_html()
    transform_html_to_json()
    load_mahidol_aqi_to_warehouse()
    notify_pm25_alert()


if __name__ == "__main__":
    pm2_5_risk_alert_pipeline()