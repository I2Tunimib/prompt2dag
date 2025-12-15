from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_brokerage_holdings_pipeline",
    description="Portfolio Rebalancing DAG",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "rebalancing"],
) as dag:

    fetch_brokerage_holdings = DockerOperator(
        task_id='fetch_brokerage_holdings',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    analyze_portfolio = DockerOperator(
        task_id='analyze_portfolio',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    aggregate_and_rebalance = DockerOperator(
        task_id='aggregate_and_rebalance',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_trade_orders = DockerOperator(
        task_id='generate_trade_orders',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set sequential dependencies
    fetch_brokerage_holdings >> analyze_portfolio >> aggregate_and_rebalance >> generate_trade_orders