from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def get_exchange_rate(currency_from: str, currency_to: str) -> float:
    """
    Return a static exchange rate for supported currency pairs.
    """
    rates = {
        ("EUR", "USD"): 1.10,
        ("JPY", "USD"): 0.0075,
    }
    return rates.get((currency_from, currency_to), 1.0)


def ingest_region(region: str, file_path: str) -> list[dict]:
    """
    Read a CSV file for a given region, add a region column,
    and push the records to XCom.
    """
    df = pd.read_csv(file_path)
    df["region"] = region.upper()
    return df.to_dict(orient="records")


def convert_region_to_usd(region: str, ti) -> list[dict]:
    """
    Pull ingested data from XCom, convert the amount column to USD
    using static exchange rates where needed, and push the converted
    records to XCom.
    """
    ingestion_task_id = f"ingest_{region.lower()}"
    records = ti.xcom_pull(task_ids=ingestion_task_id)
    if not records:
        return []

    df = pd.DataFrame(records)

    if region.upper() == "EU":
        rate = get_exchange_rate("EUR", "USD")
        df["amount_usd"] = df["amount"] * rate
    elif region.upper() == "APAC":
        rate = get_exchange_rate("JPY", "USD")
        df["amount_usd"] = df["amount"] * rate
    else:
        df["amount_usd"] = df["amount"]

    return df.to_dict(orient="records")


def aggregate_global_revenue(ti) -> str:
    """
    Pull converted regional data from XCom, aggregate revenues,
    generate a CSV report, and push the report path to XCom.
    """
    regions = ["us_east", "us_west", "eu", "apac"]
    dfs = []

    for region in regions:
        records = ti.xcom_pull(task_ids=f"convert_{region}")
        if records:
            dfs.append(pd.DataFrame(records))

    if not dfs:
        return ""

    all_df = pd.concat(dfs, ignore_index=True)

    total_revenue = all_df["amount_usd"].sum()
    report_df = pd.DataFrame(
        {
            "region": all_df["region"],
            "revenue_usd": all_df["amount_usd"],
        }
    )
    total_row = pd.DataFrame({"region": ["GLOBAL_TOTAL"], "revenue_usd": [total_revenue]})
    report_df = pd.concat([report_df, total_row], ignore_index=True)

    output_path = "/tmp/global_revenue_report.csv"
    report_df.to_csv(output_path, index=False)

    return output_path


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

with DAG(
    dag_id="multi_region_ecommerce_analytics",
    default_args=default_args,
    description="Daily multi-region ecommerce analytics pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "analytics"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_us_east = PythonOperator(
        task_id="ingest_us_east",
        python_callable=ingest_region,
        op_kwargs={"region": "US_EAST", "file_path": "/data/us_east_sales.csv"},
    )
    ingest_us_west = PythonOperator(
        task_id="ingest_us_west",
        python_callable=ingest_region,
        op_kwargs={"region": "US_WEST", "file_path": "/data/us_west_sales.csv"},
    )
    ingest_eu = PythonOperator(
        task_id="ingest_eu",
        python_callable=ingest_region,
        op_kwargs={"region": "EU", "file_path": "/data/eu_sales.csv"},
    )
    ingest_apac = PythonOperator(
        task_id="ingest_apac",
        python_callable=ingest_region,
        op_kwargs={"region": "APAC", "file_path": "/data/apac_sales.csv"},
    )

    convert_us_east = PythonOperator(
        task_id="convert_us_east",
        python_callable=convert_region_to_usd,
        op_kwargs={"region": "US_EAST"},
    )
    convert_us_west = PythonOperator(
        task_id="convert_us_west",
        python_callable=convert_region_to_usd,
        op_kwargs={"region": "US_WEST"},
    )
    convert_eu = PythonOperator(
        task_id="convert_eu",
        python_callable=convert_region_to_usd,
        op_kwargs={"region": "EU"},
    )
    convert_apac = PythonOperator(
        task_id="convert_apac",
        python_callable=convert_region_to_usd,
        op_kwargs={"region": "APAC"},
    )

    aggregate = PythonOperator(
        task_id="aggregate_global_revenue",
        python_callable=aggregate_global_revenue,
    )

    end = EmptyOperator(task_id="end")

    # Define dependencies
    start >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]

    ingest_us_east >> convert_us_east
    ingest_us_west >> convert_us_west
    ingest_eu >> convert_eu
    ingest_apac >> convert_apac

    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate

    aggregate >> end