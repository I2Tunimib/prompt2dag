from prefect import flow, task, SequentialTaskRunner

@task(name='get_data_mahidol_aqi_report', retries=0)
def get_data_mahidol_aqi_report():
    """Task: Scrape Mahidol AQI Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='create_json_object', retries=0)
def create_json_object():
    """Task: Parse and Validate AQI Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_mahidol_aqi_to_postgres', retries=0)
def load_mahidol_aqi_to_postgres():
    """Task: Load AQI Data to PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='alert_email', retries=0)
def alert_email():
    """Task: Send AQI Alert Emails"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="get_data_mahidol_aqi_report_pipeline", task_runner=SequentialTaskRunner)
def get_data_mahidol_aqi_report_pipeline():
    get_data = get_data_mahidol_aqi_report()
    create_json = create_json_object(wait_for=[get_data])
    load_data = load_mahidol_aqi_to_postgres(wait_for=[create_json])
    alert_email(wait_for=[load_data])

if __name__ == "__main__":
    get_data_mahidol_aqi_report_pipeline()