from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

from dagster import (
    DefaultScheduleStatus,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    JobDefinition,
    op,
    job,
    schedule,
)


def _read_csv(file_path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame. Returns empty DataFrame on failure."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        # In a real pipeline you would handle the error appropriately.
        # Here we return an empty DataFrame to keep the pipeline runnable.
        print(f"Failed to read {file_path}: {e}")
        return pd.DataFrame()


def _write_csv(df: pd.DataFrame, file_path: str) -> None:
    """Write a DataFrame to a CSV file."""
    try:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)
    except Exception as e:
        print(f"Failed to write {file_path}: {e}")


@op(out=Out(bool))
def start_pipeline() -> bool:
    """Marks the start of the pipeline."""
    return True


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_us_east(context) -> pd.DataFrame:
    """Ingest US‑East region sales data from a CSV source."""
    file_path = context.op_config["file_path"]
    context.log.info(f"Ingesting US‑East data from {file_path}")
    return _read_csv(file_path)


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_us_west(context) -> pd.DataFrame:
    """Ingest US‑West region sales data from a CSV source."""
    file_path = context.op_config["file_path"]
    context.log.info(f"Ingesting US‑West data from {file_path}")
    return _read_csv(file_path)


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_eu(context) -> pd.DataFrame:
    """Ingest EU region sales data from a CSV source."""
    file_path = context.op_config["file_path"]
    context.log.info(f"Ingesting EU data from {file_path}")
    return _read_csv(file_path)


@op(
    config_schema={"file_path": str},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_apac(context) -> pd.DataFrame:
    """Ingest APAC region sales data from a CSV source."""
    file_path = context.op_config["file_path"]
    context.log.info(f"Ingesting APAC data from {file_path}")
    return _read_csv(file_path)


@op(
    ins={"df": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_east(df: pd.DataFrame) -> pd.DataFrame:
    """US‑East data is already in USD; pass through."""
    return df


@op(
    ins={"df": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_west(df: pd.DataFrame) -> pd.DataFrame:
    """US‑West data is already in USD; pass through."""
    return df


@op(
    config_schema={"exchange_rate": float},
    ins={"df": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_eu(context, df: pd.DataFrame) -> pd.DataFrame:
    """Convert EU sales from EUR to USD using a daily exchange rate."""
    rate = context.op_config["exchange_rate"]
    context.log.info(f"Converting EU data using rate EUR→USD = {rate}")
    if "amount_eur" in df.columns:
        df = df.rename(columns={"amount_eur": "amount_usd"})
        df["amount_usd"] = df["amount_usd"] * rate
    return df


@op(
    config_schema={"exchange_rate": float},
    ins={"df": In(pd.DataFrame)},
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_apac(context, df: pd.DataFrame) -> pd.DataFrame:
    """Convert APAC sales from JPY to USD using a daily exchange rate."""
    rate = context.op_config["exchange_rate"]
    context.log.info(f"Converting APAC data using rate JPY→USD = {rate}")
    if "amount_jpy" in df.columns:
        df = df.rename(columns={"amount_jpy": "amount_usd"})
        df["amount_usd"] = df["amount_usd"] * rate
    return df


@op(
    config_schema={"output_path": str},
    ins={
        "us_east": In(pd.DataFrame),
        "us_west": In(pd.DataFrame),
        "eu": In(pd.DataFrame),
        "apac": In(pd.DataFrame),
    },
    out=Out(bool),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def aggregate_and_report(context, us_east: pd.DataFrame, us_west: pd.DataFrame, eu: pd.DataFrame, apac: pd.DataFrame) -> bool:
    """Aggregate regional revenues and generate a global CSV report."""
    context.log.info("Aggregating regional data")
    combined = pd.concat([us_east, us_west, eu, apac], ignore_index=True)

    if "amount_usd" not in combined.columns:
        context.log.error("Expected column 'amount_usd' not found in combined data")
        return False

    total_revenue = combined["amount_usd"].sum()
    context.log.info(f"Global revenue (USD): {total_revenue:,.2f}")

    # Add a summary row
    summary = pd.DataFrame([{"region": "GLOBAL_TOTAL", "amount_usd": total_revenue}])
    report_df = pd.concat([combined, summary], ignore_index=True)

    output_path = context.op_config["output_path"]
    context.log.info(f"Writing global revenue report to {output_path}")
    _write_csv(report_df, output_path)
    return True


@op(out=Out(bool))
def end_pipeline() -> bool:
    """Marks the end of the pipeline."""
    return True


@job
def ecommerce_analytics_job():
    """Dagster job that orchestrates the multi‑region ecommerce analytics pipeline."""
    start = start_pipeline()

    # Ingestion (fan‑out)
    us_east_raw = ingest_us_east().after(start)
    us_west_raw = ingest_us_west().after(start)
    eu_raw = ingest_eu().after(start)
    apac_raw = ingest_apac().after(start)

    # Currency conversion (second fan‑out)
    us_east_converted = convert_us_east(us_east_raw)
    us_west_converted = convert_us_west(us_west_raw)
    eu_converted = convert_eu(eu_raw)
    apac_converted = convert_apac(apac_raw)

    # Aggregation (fan‑in)
    aggregation = aggregate_and_report(
        us_east=us_east_converted,
        us_west=us_west_converted,
        eu=eu_converted,
        apac=apac_converted,
    )

    # End step
    end_pipeline().after(aggregation)


daily_ecommerce_schedule = ScheduleDefinition(
    job=ecommerce_analytics_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the ecommerce analytics pipeline.",
)


if __name__ == "__main__":
    result = ecommerce_analytics_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")