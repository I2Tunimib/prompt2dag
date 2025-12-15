import pandas as pd
from pathlib import Path
from typing import List, Dict

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


def send_failure_email(task_name: str, exc: Exception) -> None:
    """
    Placeholder for sending an email notification on task failure.
    In a real deployment, integrate with an email service.
    """
    logger = get_run_logger()
    logger.error(f"Task '{task_name}' failed with exception: {exc}")
    # Example: send email using SMTP or an external service
    # For now we just log the failure.


def get_exchange_rate(currency: str) -> float:
    """
    Retrieve the daily exchange rate to USD.
    This placeholder returns static rates; replace with a real API call.
    """
    rates = {
        "EUR": 1.10,   # 1 EUR = 1.10 USD
        "JPY": 0.009,  # 1 JPY = 0.009 USD
    }
    return rates.get(currency.upper(), 1.0)


@task
def start_pipeline() -> None:
    """Marks the start of the pipeline."""
    logger = get_run_logger()
    logger.info("Pipeline started.")


@task(retries=3, retry_delay_seconds=300)
def ingest_region_sales(region: str, source_path: Path) -> pd.DataFrame:
    """
    Ingest sales data for a given region from a CSV file.
    """
    logger = get_run_logger()
    logger.info(f"Ingesting {region} sales data from {source_path}.")
    try:
        df = pd.read_csv(source_path)
        df["region"] = region
        return df
    except Exception as exc:
        send_failure_email(f"ingest_region_sales_{region}", exc)
        raise


@task(retries=3, retry_delay_seconds=300)
def convert_to_usd(region: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert regional sales amounts to USD.
    US regions are already in USD; EU and APAC require conversion.
    """
    logger = get_run_logger()
    logger.info(f"Converting {region} sales data to USD.")
    try:
        if region in ("US-East", "US-West"):
            # No conversion needed
            df["amount_usd"] = df["amount"]
        elif region == "EU":
            rate = get_exchange_rate("EUR")
            df["amount_usd"] = df["amount"] * rate
        elif region == "APAC":
            rate = get_exchange_rate("JPY")
            df["amount_usd"] = df["amount"] * rate
        else:
            df["amount_usd"] = df["amount"]
        return df
    except Exception as exc:
        send_failure_email(f"convert_to_usd_{region}", exc)
        raise


@task(retries=3, retry_delay_seconds=300)
def aggregate_global_revenue(
    converted_dfs: List[pd.DataFrame], output_path: Path
) -> Path:
    """
    Aggregate all regional data into a global revenue report.
    """
    logger = get_run_logger()
    logger.info("Aggregating global revenue report.")
    try:
        combined = pd.concat(converted_dfs, ignore_index=True)
        regional_revenue = combined.groupby("region")["amount_usd"].sum()
        global_total = regional_revenue.sum()

        report = pd.DataFrame({
            "region": regional_revenue.index,
            "revenue_usd": regional_revenue.values
        })
        report = report.append(
            {"region": "GLOBAL_TOTAL", "revenue_usd": global_total},
            ignore_index=True,
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(output_path, index=False)
        logger.info(f"Global revenue report written to {output_path}.")
        return output_path
    except Exception as exc:
        send_failure_email("aggregate_global_revenue", exc)
        raise


@task
def end_pipeline() -> None:
    """Marks the end of the pipeline."""
    logger = get_run_logger()
    logger.info("Pipeline completed.")


@flow
def ecommerce_analytics_flow() -> None:
    """
    Multi-region ecommerce analytics pipeline.
    Executes daily to produce a global revenue report.
    """
    # Start
    start_pipeline()

    # Define source CSV paths (replace with actual locations)
    base_dir = Path("data")
    sources: Dict[str, Path] = {
        "US-East": base_dir / "us_east_sales.csv",
        "US-West": base_dir / "us_west_sales.csv",
        "EU": base_dir / "eu_sales.csv",
        "APAC": base_dir / "apac_sales.csv",
    }

    # Fan‑out: ingest data in parallel
    ingest_futures = {
        region: ingest_region_sales.submit(region, path)
        for region, path in sources.items()
    }

    # Wait for ingestion to finish and collect DataFrames
    ingested_dfs = {
        region: future.result()
        for region, future in ingest_futures.items()
    }

    # Fan‑out: currency conversion in parallel
    convert_futures = {
        region: convert_to_usd.submit(region, df)
        for region, df in ingested_dfs.items()
    }

    # Collect converted DataFrames
    converted_dfs = [
        future.result()
        for future in convert_futures.values()
    ]

    # Fan‑in: aggregation
    output_report_path = Path("output") / "global_revenue_report.csv"
    aggregate_global_revenue(converted_dfs, output_report_path)

    # End
    end_pipeline()


# Note: In a production deployment, configure a daily schedule with no catch‑up,
# a maximum parallelism of 4, and email notifications on task failures.

if __name__ == "__main__":
    ecommerce_analytics_flow()