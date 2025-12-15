from prefect import flow, task, SequentialTaskRunner

@task(name='get_data_mahidol_aqi_report', retries=0)
def get_data_mahidol_aqi_report():
    """Task: Get Mahidol AQI Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='create_json_object', retries=0)
def create_json_object():
    """Task: Create JSON Object"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_mahidol_aqi_to_postgres', retries=0)
def load_mahidol_aqi_to_postgres():
    """Task: Load Mahidol AQI to PostgreSQL"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='alert_email', retries=0)
def alert_email():
    """Task: Send Email Alert"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="get_data_mahidol_aqi_report_pipeline", task_runner=SequentialTaskRunner)
def get_data_mahidol_aqi_report_pipeline():
    get_data_task = get_data_mahidol_aqi_report()
    create_json_task = create_json_object(wait_for=[get_data_task])
    load_postgres_task = load_mahidol_aqi_to_postgres(wait_for=[create_json_task])
    alert_email_task = alert_email(wait_for=[load_postgres_task])

if __name__ == "__main__":
    get_data_mahidol_aqi_report_pipeline()