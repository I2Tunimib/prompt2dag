from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='fetch_brokerage_holdings_pipeline',
    description='This DAG implements a financial portfolio rebalancing pipeline using a fan-out fan-in pattern to process holdings from 5 brokerage accounts in parallel, analyze each portfolio independently, aggregate results to calculate rebalancing trades, and generate final trade orders.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task: fetch_brokerage_holdings
    fetch_brokerage_holdings = DockerOperator(
        task_id='fetch_brokerage_holdings',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: analyze_portfolio (5 instances)
    analyze_portfolio_1 = DockerOperator(
        task_id='analyze_portfolio_1',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    analyze_portfolio_2 = DockerOperator(
        task_id='analyze_portfolio_2',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    analyze_portfolio_3 = DockerOperator(
        task_id='analyze_portfolio_3',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    analyze_portfolio_4 = DockerOperator(
        task_id='analyze_portfolio_4',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )
    analyze_portfolio_5 = DockerOperator(
        task_id='analyze_portfolio_5',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: aggregate_and_rebalance
    aggregate_and_rebalance = DockerOperator(
        task_id='aggregate_and_rebalance',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: generate_trade_orders
    generate_trade_orders = DockerOperator(
        task_id='generate_trade_orders',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    fetch_brokerage_holdings >> [analyze_portfolio_1, analyze_portfolio_2, analyze_portfolio_3, analyze_portfolio_4, analyze_portfolio_5]
    [analyze_portfolio_1, analyze_portfolio_2, analyze_portfolio_3, analyze_portfolio_4, analyze_portfolio_5] >> aggregate_and_rebalance
    aggregate_and_rebalance >> generate_trade_orders