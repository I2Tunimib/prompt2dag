from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define the DAG
with DAG(
    dag_id='fetch_brokerage_holdings_pipeline',
    description='No description provided.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task definitions
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

    # Task dependencies
    fetch_brokerage_holdings >> [analyze_portfolio, analyze_portfolio, analyze_portfolio, analyze_portfolio, analyze_portfolio]
    [analyze_portfolio, analyze_portfolio, analyze_portfolio, analyze_portfolio, analyze_portfolio] >> aggregate_and_rebalance
    aggregate_and_rebalance >> generate_trade_orders