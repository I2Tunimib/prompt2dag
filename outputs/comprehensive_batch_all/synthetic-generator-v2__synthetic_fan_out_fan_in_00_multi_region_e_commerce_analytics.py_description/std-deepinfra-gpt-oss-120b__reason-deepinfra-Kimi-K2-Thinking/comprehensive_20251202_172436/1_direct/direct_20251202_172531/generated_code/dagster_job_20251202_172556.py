from typing import Dict

import pandas as pd
from dagster import (
    DefaultScheduleStatus,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
)


@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def start_op() -> None:
    """Marks the start of the pipeline."""
    return None


def _read_csv(file_path: str) -> pd.DataFrame:
    """Utility to read a CSV file into a DataFrame."""
    return pd.read_csv(file_path)


@op(
    ins={"_start": In(void=True)},
    out=Out(pd.DataFrame),
    config_schema={"file_path": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_us_east_op(_start, config) -> pd.DataFrame:
    """Ingest US‑East region sales data."""
    return _read_csv(config["file_path"])


@op(
    ins={"_start": In(void=True)},
    out=Out(pd.DataFrame),
    config_schema={"file_path": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_us_west_op(_start, config) -> pd.DataFrame:
    """Ingest US‑West region sales data."""
    return _read_csv(config["file_path"])


@op(
    ins={"_start": In(void=True)},
    out=Out(pd.DataFrame),
    config_schema={"file_path": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_eu_op(_start, config) -> pd.DataFrame:
    """Ingest EU region sales data."""
    return _read_csv(config["file_path"])


@op(
    ins={"_start": In(void=True)},
    out=Out(pd.DataFrame),
    config_schema={"file_path": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def ingest_apac_op(_start, config) -> pd.DataFrame:
    """Ingest APAC region sales data."""
    return _read_csv(config["file_path"])


def _pass_through_us(df: pd.DataFrame) -> pd.DataFrame:
    """US regions are already in USD; just rename the amount column."""
    df = df.copy()
    if "amount" in df.columns:
        df["amount_usd"] = df["amount"]
    return df


def _convert_currency(df: pd.DataFrame, rate: float) -> pd.DataFrame:
    """Convert a regional amount column to USD using the provided rate."""
    df = df.copy()
    if "amount" in df.columns:
        df["amount_usd"] = df["amount"] * rate
    else:
        # Fallback if column naming differs
        df["amount_usd"] = df.iloc[:, 0] * rate
    return df


@op(
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_east_op(us_east_raw: pd.DataFrame) -> pd.DataFrame:
    """Convert US‑East data (already USD)."""
    return _pass_through_us(us_east_raw)


@op(
    out=Out(pd.DataFrame),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_us_west_op(us_west_raw: pd.DataFrame) -> pd.DataFrame:
    """Convert US‑West data (already USD)."""
    return _pass_through_us(us_west_raw)


@op(
    out=Out(pd.DataFrame),
    config_schema={"eur_to_usd_rate": float},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_eu_op(eu_raw: pd.DataFrame, config) -> pd.DataFrame:
    """Convert EU data from EUR to USD."""
    return _convert_currency(eu_raw, config["eur_to_usd_rate"])


@op(
    out=Out(pd.DataFrame),
    config_schema={"jpy_to_usd_rate": float},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def convert_apac_op(apac_raw: pd.DataFrame, config) -> pd.DataFrame:
    """Convert APAC data from JPY to USD."""
    return _convert_currency(apac_raw, config["jpy_to_usd_rate"])


@op(
    ins={
        "us_east": In(pd.DataFrame),
        "us_west": In(pd.DataFrame),
        "eu": In(pd.DataFrame),
        "apac": In(pd.DataFrame),
    },
    out=Out(str),
    config_schema={"output_path": str},
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def aggregate_op(us_east: pd.DataFrame, us_west: pd.DataFrame, eu: pd.DataFrame, apac: pd.DataFrame, config) -> str:
    """Aggregate regional revenues and generate a global CSV report."""
    regional_data: Dict[str, pd.DataFrame] = {
        "US-East": us_east,
        "US-West": us_west,
        "EU": eu,
        "APAC": apac,
    }

    totals = {}
    global_total = 0.0
    for region, df in regional_data.items():
        revenue = df["amount_usd"].sum()
        totals[region] = revenue
        global_total += revenue

    report_df = pd.DataFrame(
        list(totals.items()) + [("Global", global_total)],
        columns=["Region", "Revenue_USD"],
    )
    output_path = config["output_path"]
    report_df.to_csv(output_path, index=False)
    return output_path


@op(retry_policy=RetryPolicy(max_retries=3, delay=300))
def end_op() -> None:
    """Marks the end of the pipeline."""
    return None


@job
def ecommerce_analytics_job():
    """Dagster job orchestrating the multi‑region ecommerce analytics pipeline."""
    start = start_op()

    us_east_raw = ingest_us_east_op(start)
    us_west_raw = ingest_us_west_op(start)
    eu_raw = ingest_eu_op(start)
    apac_raw = ingest_apac_op(start)

    us_east_converted = convert_us_east_op(us_east_raw)
    us_west_converted = convert_us_west_op(us_west_raw)
    eu_converted = convert_eu_op(eu_raw)
    apac_converted = convert_apac_op(apac_raw)

    _ = aggregate_op(
        us_east=us_east_converted,
        us_west=us_west_converted,
        eu=eu_converted,
        apac=apac_converted,
    )

    end_op()


daily_schedule = ScheduleDefinition(
    job=ecommerce_analytics_job,
    cron_schedule="0 0 * * *",  # Runs daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the ecommerce analytics pipeline.",
)


if __name__ == "__main__":
    result = ecommerce_analytics_job.execute_in_process(
        run_config={
            "ops": {
                "ingest_us_east_op": {"config": {"file_path": "data/us_east_sales.csv"}},
                "ingest_us_west_op": {"config": {"file_path": "data/us_west_sales.csv"}},
                "ingest_eu_op": {"config": {"file_path": "data/eu_sales.csv"}},
                "ingest_apac_op": {"config": {"file_path": "data/apac_sales.csv"}},
                "convert_eu_op": {"config": {"eur_to_usd_rate": 1.10}},
                "convert_apac_op": {"config": {"jpy_to_usd_rate": 0.0090}},
                "aggregate_op": {"config": {"output_path": "output/global_revenue_report.csv"}},
            }
        }
    )
    print(f"Job completed successfully: {result.success}")