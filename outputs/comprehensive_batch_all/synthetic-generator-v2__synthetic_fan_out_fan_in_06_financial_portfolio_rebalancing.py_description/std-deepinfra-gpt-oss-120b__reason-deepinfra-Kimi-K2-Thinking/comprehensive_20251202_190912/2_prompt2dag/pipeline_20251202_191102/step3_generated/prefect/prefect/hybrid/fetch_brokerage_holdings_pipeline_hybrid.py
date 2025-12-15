from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule


@task(name='fetch_brokerage_holdings', retries=2)
def fetch_brokerage_holdings():
    """Task: Fetch Brokerage Holdings"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='analyze_portfolio', retries=2)
def analyze_portfolio():
    """Task: Analyze Portfolio"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='aggregate_and_rebalance', retries=2)
def aggregate_and_rebalance():
    """Task: Aggregate and Rebalance"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='generate_trade_orders', retries=2)
def generate_trade_orders():
    """Task: Generate Trade Orders"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="fetch_brokerage_holdings_pipeline", task_runner=ConcurrentTaskRunner())
def fetch_brokerage_holdings_pipeline():
    """
    Sequential pipeline for portfolio rebalancing.
    """
    # Entry point
    fetch_brokerage_holdings()
    # Subsequent steps respecting dependencies
    analyze_portfolio()
    aggregate_and_rebalance()
    generate_trade_orders()


# Deployment configuration
DeploymentSpec(
    name="fetch_brokerage_holdings_pipeline_deployment",
    flow=fetch_brokerage_holdings_pipeline,
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
    work_pool_name="default-agent-pool",
)