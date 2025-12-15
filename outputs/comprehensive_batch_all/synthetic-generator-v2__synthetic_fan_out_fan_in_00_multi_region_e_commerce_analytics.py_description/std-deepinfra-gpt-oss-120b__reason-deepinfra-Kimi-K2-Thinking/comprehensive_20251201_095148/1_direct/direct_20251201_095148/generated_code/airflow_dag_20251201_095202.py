from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def ingest_sales_data(region: str, **kwargs):
    """
    Ingest sales data for a given region from a CSV file.

    The CSV path is constructed as ``/data/{region}_sales.csv``.
    The function returns a pandas DataFrame which is pushed to XCom.
    """
    file_path = f"/data/{region}_sales.csv"
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        # Return empty DataFrame if file is missing to keep pipeline robust
        df = pd.DataFrame()
    return df


def convert_to_usd(region: str, **kwargs):
    """
    Convert regional sales data to USD.

    For US-East and US-West the data is already in USD and is returned unchanged.
    For EU and APAC the function applies a static exchange rate.
    """
    ti = kwargs["ti"]
    df = ti.xcom_pull(key=None, task_ids=f"ingest_{region.lower()}_sales")
    if df.empty:
        return df

    if region == "EU":
        # Example static EUR to USD rate
        rate = 1.10
        df["amount_usd"] = df["amount"] * rate
    elif region == "APAC":
        # Example static JPY to USD rate
        rate = 0.009
        df["amount_usd"] = df["amount"] * rate
    else:
        # US regions already in USD
        df["amount_usd"] = df["amount"]
    return df


def aggregate_global_revenue(**kwargs):
    """
    Aggregate converted regional data into a global revenue report.

    Pulls converted DataFrames from XCom, calculates total revenue per region,
    sums them for a global total, and writes a CSV report.
    """
    ti = kwargs["ti"]
    regions = ["us_east", "us_west", "eu", "apac"]
    aggregated_rows = []

    for region in regions:
        df = ti.xcom_pull(key=None, task_ids=f"convert_{region}_sales")
        if df.empty:
            revenue = 0.0
        else:
            revenue = df["amount_usd"].sum()
        aggregated_rows.append({"region": region.upper(), "revenue_usd": revenue})

    report_df = pd.DataFrame(aggregated_rows)
    report_df["global_total_usd"] = report_df["revenue_usd"].sum()
    output_path = "/data/global_revenue_report.csv"
    report_df.to_csv(output_path, index=False)
    return output_path


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="multi_region_ecommerce_analytics",
    description="Daily pipeline that ingests, converts, and aggregates regional sales data.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=["ecommerce", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    ingest_us_east = PythonOperator(
        task_id="ingest_us_east_sales",
        python_callable=ingest_sales_data,
        op_kwargs={"region": "US_EAST"},
    )
    ingest_us_west = PythonOperator(
        task_id="ingest_us_west_sales",
        python_callable=ingest_sales_data,
        op_kwargs={"region": "US_WEST"},
    )
    ingest_eu = PythonOperator(
        task_id="ingest_eu_sales",
        python_callable=ingest_sales_data,
        op_kwargs={"region": "EU"},
    )
    ingest_apac = PythonOperator(
        task_id="ingest_apac_sales",
        python_callable=ingest_sales_data,
        op_kwargs={"region": "APAC"},
    )

    convert_us_east = PythonOperator(
        task_id="convert_us_east_sales",
        python_callable=convert_to_usd,
        op_kwargs={"region": "US_EAST"},
    )
    convert_us_west = PythonOperator(
        task_id="convert_us_west_sales",
        python_callable=convert_to_usd,
        op_kwargs={"region": "US_WEST"},
    )
    convert_eu = PythonOperator(
        task_id="convert_eu_sales",
        python_callable=convert_to_usd,
        op_kwargs={"region": "EU"},
    )
    convert_apac = PythonOperator(
        task_id="convert_apac_sales",
        python_callable=convert_to_usd,
        op_kwargs={"region": "APAC"},
    )

    aggregate = PythonOperator(
        task_id="aggregate_global_revenue",
        python_callable=aggregate_global_revenue,
    )

    end = EmptyOperator(task_id="end_pipeline")

    # Define dependencies
    start >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]

    ingest_us_east >> convert_us_east
    ingest_us_west >> convert_us_west
    ingest_eu >> convert_eu
    ingest_apac >> convert_apac

    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate

    aggregate >> end