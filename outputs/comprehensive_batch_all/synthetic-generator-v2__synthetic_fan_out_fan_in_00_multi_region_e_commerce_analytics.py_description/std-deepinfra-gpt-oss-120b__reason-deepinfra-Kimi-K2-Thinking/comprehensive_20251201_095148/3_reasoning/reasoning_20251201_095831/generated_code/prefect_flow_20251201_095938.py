from prefect import flow, task
import pandas as pd
from typing import Any
import os


@task(retries=3, retry_delay_seconds=300)
def ingest_us_east() -> pd.DataFrame:
    """Ingest US-East region sales data from CSV source."""
    data = {
        'region': ['US-East'] * 3,
        'sales': [1000.0, 1500.0, 2000.0],
        'currency': ['USD'] * 3
    }
    return pd.DataFrame(data)


@task(retries=3, retry_delay_seconds=300)
def ingest_us_west() -> pd.DataFrame:
    """Ingest US-West region sales data from CSV source."""
    data = {
        'region': ['US-West'] * 3,
        'sales': [1200.0, 1800.0, 2200.0],
        'currency': ['USD'] * 3
    }
    return pd.DataFrame(data)


@task(retries=3, retry_delay_seconds=300)
def ingest_eu() -> pd.DataFrame:
    """Ingest EU region sales data from CSV source."""
    data = {
        'region': ['EU'] * 3,
        'sales': [900.0, 1300.0, 1700.0],
        'currency': ['EUR'] * 3
    }
    return pd.DataFrame(data)


@task(retries=3, retry_delay_seconds=300)
def ingest_apac() -> pd.DataFrame:
    """Ingest APAC region sales data from CSV source."""
    data = {
        'region': ['APAC'] * 3,
        'sales': [150000.0, 200000.0, 250000.0],
        'currency': ['JPY'] * 3
    }
    return pd.DataFrame(data)


@task(retries=3, retry_delay_seconds=300)
def convert_us_east(df: pd.DataFrame) -> pd.DataFrame:
    """Convert US-East data (already USD, no conversion needed)."""
    df = df.copy()
    df['sales_usd'] = df['sales']
    return df


@task(retries=3, retry_delay_seconds=300)
def convert_us_west(df: pd.DataFrame) -> pd.DataFrame:
    """Convert US-West data (already USD, no conversion needed)."""
    df = df.copy()
    df['sales_usd'] = df['sales']
    return df


@task(retries=3, retry_delay_seconds=300)
def convert_eu(df: pd.DataFrame) -> pd.DataFrame:
    """Convert EU data from EUR to USD using daily exchange rate."""
    df = df.copy()
    exchange_rate = 1.08
    df['sales_usd'] = df['sales'] * exchange_rate
    return df


@task(retries=3, retry_delay_seconds=300)
def convert_apac(df: pd.DataFrame) -> pd.DataFrame:
    """Convert APAC data from JPY to USD using daily exchange rate."""
    df = df.copy()
    exchange_rate = 0.0067
    df['sales_usd'] = df['sales'] * exchange_rate
    return df


@task(retries=3, retry_delay_seconds=300)
def aggregate_global_revenue(
    us_east_data: pd.DataFrame,
    us_west_data: pd.DataFrame,
    eu_data: pd.DataFrame,
    apac_data: pd.DataFrame
) -> str:
    """Aggregate all converted regional data and generate global revenue report CSV."""
    combined_df = pd.concat(
        [us_east_data, us_west_data, eu_data, apac_data],
        ignore_index=True
    )
    
    regional_revenues = combined_df.groupby('region')['sales_usd'].sum()
    global_total = regional_revenues.sum()
    
    report = f"""Global Revenue Report
=====================
Regional Revenues (USD):
{regional_revenues.to_string()}

Global Total Revenue: ${global_total:,.2f}
"""
    
    output_path = "global_revenue_report.csv"
    combined_df.to_csv(output_path, index=False)
    
    return report


@flow(name="ecommerce-analytics-pipeline")
def ecommerce_analytics_pipeline() -> str:
    """
    Multi-region ecommerce analytics pipeline.
    
    Daily execution schedule (configure via deployment).
    Email notifications on task failures (configure via deployment).
    """
    us_east_future = ingest_us_east.submit()
    us_west_future = ingest_us_west.submit()
    eu_future = ingest_eu.submit()
    apac_future = ingest_apac.submit()
    
    us_east_conv = convert_us_east.submit(us_east_future)
    us_west_conv = convert_us_west.submit(us_west_future)
    eu_conv = convert_eu.submit(eu_future)
    apac_conv = convert_apac.submit(apac_future)
    
    report = aggregate_global_revenue(
        us_east_conv,
        us_west_conv,
        eu_conv,
        apac_conv
    )
    
    return report


if __name__ == '__main__':
    result = ecommerce_analytics_pipeline()
    print(result)