from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import time

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_for_sales_aggregation():
    logger = get_run_logger()
    logger.info("Waiting for daily_sales_aggregation to complete...")
    # Simulate waiting for an external task to complete
    time.sleep(3600)  # 1-hour timeout
    logger.info("daily_sales_aggregation completed successfully.")

@task(retries=2, retry_delay_seconds=300)
def load_sales_csv():
    logger = get_run_logger()
    logger.info("Loading sales CSV data...")
    # Simulate loading CSV data
    data = pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02'],
        'product': ['A', 'B'],
        'region': ['North', 'South'],
        'revenue': [1000, 1500]
    })
    logger.info("Sales CSV data loaded successfully.")
    return data

@task(retries=2, retry_delay_seconds=300)
def generate_dashboard(data):
    logger = get_run_logger()
    logger.info("Generating executive dashboard...")
    # Simulate dashboard generation
    revenue_trends = data.groupby('date')['revenue'].sum()
    product_performance = data.groupby('product')['revenue'].sum()
    regional_analysis = data.groupby('region')['revenue'].sum()
    logger.info("Executive dashboard generated successfully.")
    return {
        'revenue_trends': revenue_trends,
        'product_performance': product_performance,
        'regional_analysis': regional_analysis
    }

@flow(name="Executive Sales Dashboard Pipeline")
def executive_sales_dashboard_pipeline():
    logger = get_run_logger()
    logger.info("Starting Executive Sales Dashboard Pipeline...")
    
    # Wait for the daily sales aggregation to complete
    wait_for_sales_aggregation()
    
    # Load the sales CSV data
    sales_data = load_sales_csv()
    
    # Generate the executive dashboard
    dashboard = generate_dashboard(sales_data)
    
    logger.info("Executive Sales Dashboard Pipeline completed successfully.")
    return dashboard

if __name__ == '__main__':
    executive_sales_dashboard_pipeline()