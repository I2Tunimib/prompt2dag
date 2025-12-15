from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def ingest_region(region: str, file_path: str, **kwargs):
    """
    Read regional sales CSV file and push data via XCom.

    :param region: Name of the region.
    :param file_path: Path to the CSV file.
    :return: List of records (dicts) representing the sales data.
    """
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        # In a real pipeline, proper error handling would be required.
        df = pd.DataFrame(columns=["order_id", "revenue", "currency"])
    # Convert DataFrame to list of dicts for XCom serialization.
    return df.to_dict(orient="records")


def get_exchange_rate(currency: str) -> float:
    """
    Retrieve a daily exchange rate for the given currency to USD.
    This placeholder returns static rates; replace with real API calls as needed.

    :param currency: Currency code (e.g., 'EUR', 'JPY').
    :return: Exchange rate to USD.
    """
    rates = {
        "EUR": 1.10,   # 1 EUR = 1.10 USD
        "JPY": 0.0090, # 1 JPY = 0.0090 USD
    }
    return rates.get(currency.upper(), 1.0)


def convert_region(region: str, **kwargs):
    """
    Convert regional sales data to USD where needed.

    :param region: Name of the region.
    :return: List of records with revenue expressed in USD.
    """
    ti = kwargs["ti"]
    ingestion_task_id = f"ingest_{region.lower().replace('-', '_')}"
    records = ti.xcom_pull(task_ids=ingestion_task_id)

    if not records:
        return []

    df = pd.DataFrame(records)

    # Determine if conversion is required based on region.
    if region in ("EU", "APAC"):
        # Identify the source currency.
        source_currency = "EUR" if region == "EU" else "JPY"
        rate = get_exchange_rate(source_currency)
        # Convert revenue column to USD.
        df["revenue_usd"] = df["revenue"] * rate
    else:
        # US regions are already in USD.
        df["revenue_usd"] = df["revenue"]

    # Return only necessary fields.
    return df[["order_id", "revenue_usd"]].to_dict(orient="records")


def aggregate_regions(**kwargs):
    """
    Aggregate converted regional data, compute total revenue,
    and generate a global revenue report CSV.

    :return: Path to the generated report.
    """
    ti = kwargs["ti"]
    region_keys = ["us_east", "us_west", "eu", "apac"]
    all_frames = []

    for region in region_keys:
        task_id = f"convert_{region}"
        records = ti.xcom_pull(task_ids=task_id)
        if records:
            df = pd.DataFrame(records)
            df["region"] = region.upper()
            all_frames.append(df)

    if not all_frames:
        # No data to aggregate.
        return "/tmp/global_revenue_report.csv"

    combined_df = pd.concat(all_frames, ignore_index=True)

    # Calculate total revenue per region.
    regional_totals = combined_df.groupby("region")["revenue_usd"].sum().reset_index()
    # Calculate global total.
    global_total = combined_df["revenue_usd"].sum()

    # Prepare report DataFrame.
    report_df = pd.concat(
        [
            regional_totals,
            pd.DataFrame([{"region": "GLOBAL_TOTAL", "revenue_usd": global_total}]),
        ],
        ignore_index=True,
    )

    report_path = "/tmp/global_revenue_report.csv"
    report_df.to_csv(report_path, index=False)

    return report_path


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="multi_region_ecommerce_analytics",
    default_args=default_args,
    description="Daily pipeline processing regional sales data and generating a global revenue report.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,
    tags=["ecommerce", "analytics"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest_us_east = PythonOperator(
        task_id="ingest_us_east",
        python_callable=ingest_region,
        op_kwargs={
            "region": "US-East",
            "file_path": "/data/us_east_sales.csv",
        },
    )

    ingest_us_west = PythonOperator(
        task_id="ingest_us_west",
        python_callable=ingest_region,
        op_kwargs={
            "region": "US-West",
            "file_path": "/data/us_west_sales.csv",
        },
    )

    ingest_eu = PythonOperator(
        task_id="ingest_eu",
        python_callable=ingest_region,
        op_kwargs={
            "region": "EU",
            "file_path": "/data/eu_sales.csv",
        },
    )

    ingest_apac = PythonOperator(
        task_id="ingest_apac",
        python_callable=ingest_region,
        op_kwargs={
            "region": "APAC",
            "file_path": "/data/apac_sales.csv",
        },
    )

    convert_us_east = PythonOperator(
        task_id="convert_us_east",
        python_callable=convert_region,
        op_kwargs={"region": "US-East"},
    )

    convert_us_west = PythonOperator(
        task_id="convert_us_west",
        python_callable=convert_region,
        op_kwargs={"region": "US-West"},
    )

    convert_eu = PythonOperator(
        task_id="convert_eu",
        python_callable=convert_region,
        op_kwargs={"region": "EU"},
    )

    convert_apac = PythonOperator(
        task_id="convert_apac",
        python_callable=convert_region,
        op_kwargs={"region": "APAC"},
    )

    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_regions,
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