from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_email import EmailNotifyOnFailure

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_for_sales_aggregation():
    logger = get_run_logger()
    logger.info("Waiting for daily_sales_aggregation to complete...")
    # Simulate waiting for an external task to complete
    # In a real scenario, this would involve checking the status of an external task
    # For example, using an Airflow API or similar mechanism
    import time
    time.sleep(3600)  # Simulate 1-hour timeout
    logger.info("daily_sales_aggregation completed successfully.")

@task(retries=2, retry_delay_seconds=300)
def load_sales_csv():
    logger = get_run_logger()
    logger.info("Loading aggregated sales CSV data...")
    # Simulate loading and validating CSV data
    # In a real scenario, this would involve reading a CSV file and performing data validation
    import pandas as pd
    data = pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02'],
        'product': ['A', 'B'],
        'revenue': [1000, 1500],
        'region': ['North', 'South']
    })
    logger.info("Sales CSV data loaded and validated.")
    return data

@task(retries=2, retry_delay_seconds=300)
def generate_dashboard(data):
    logger = get_run_logger()
    logger.info("Generating executive dashboard...")
    # Simulate generating a dashboard with key sales metrics and visualizations
    # In a real scenario, this would involve creating visualizations and metrics
    import matplotlib.pyplot as plt
    data.plot(x='date', y='revenue', kind='bar')
    plt.title("Revenue Trends")
    plt.show()
    logger.info("Executive dashboard generated.")

@flow(name="Executive Sales Dashboard Pipeline", on_failure=EmailNotifyOnFailure(email="admin@example.com"))
def executive_sales_dashboard_pipeline():
    logger = get_run_logger()
    logger.info("Starting Executive Sales Dashboard Pipeline...")
    
    # Wait for the daily sales aggregation to complete
    wait_for_sales_aggregation()
    
    # Load the aggregated sales CSV data
    sales_data = load_sales_csv()
    
    # Generate the executive dashboard
    generate_dashboard(sales_data)
    
    logger.info("Executive Sales Dashboard Pipeline completed successfully.")

if __name__ == '__main__':
    executive_sales_dashboard_pipeline()