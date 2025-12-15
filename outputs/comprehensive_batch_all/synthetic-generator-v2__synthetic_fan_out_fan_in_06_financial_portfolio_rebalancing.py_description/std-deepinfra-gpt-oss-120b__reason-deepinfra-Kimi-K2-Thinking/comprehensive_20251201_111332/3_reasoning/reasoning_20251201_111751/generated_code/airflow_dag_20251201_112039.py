import datetime
from typing import Dict, List, Any

import pandas as pd
from airflow.decorators import dag, task


default_args = {
    'owner': 'finance-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}


@dag(
    dag_id='portfolio_rebalancing',
    default_args=default_args,
    description='Daily portfolio rebalancing across 5 brokerage accounts',
    schedule_interval='@daily',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'portfolio