from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict

from dagster import (
    op,
    job,
    RetryPolicy,
    ScheduleDefinition,
    Config,
    ConfigurableResource,
    ResourceDefinition,
    Definitions,
    get_dagster_logger,
)


class CSVIngestionConfig(Config):
    file_path: str = "data/region.csv"


class AggregationConfig(Config):
    output_path: str = "output/global_revenue_report.csv"


class ExchangeRateResource(ConfigurableResource):
    """Simple resource providing exchange rates for supported currencies."""

    eur_to_usd: float = 1.10  # Example rate
    jpy_to_usd: float = 0.0090  # Example rate

    def convert(self, amount: float, currency: str) -> float:
        if currency == "EUR":
            return amount * self.eur_to_usd
        if currency == "JPY":
            return amount * self.jpy_to_usd
        return amount  # Assume USD already


@op
def start_pipeline() -> None:
    """Marks the start of the pipeline."""
    get_dagster_logger().info("Pipeline started.")


def _read_csv(file_path: str) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dictionaries."""
    if not os.path.isfile(file_path):
        get_dagster_logger().warning(f"File {file_path} not found. Returning empty data.")
        return []
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


@op(
    config_schema=CSVIngestionConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest US-East region sales data from CSV.",
)
def ingest_us_east(context) -> List[Dict[str, str]]:
    cfg: CSVIngestionConfig = context.op_config
    get_dagster_logger().info(f"Ingesting US-East data from {cfg.file_path}")
    return _read_csv(cfg.file_path)


@op(
    config_schema=CSVIngestionConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest US-West region sales data from CSV.",
)
def ingest_us_west(context) -> List[Dict[str, str]]:
    cfg: CSVIngestionConfig = context.op_config
    get_dagster_logger().info(f"Ingesting US-West data from {cfg.file_path}")
    return _read_csv(cfg.file_path)


@op(
    config_schema=CSVIngestionConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest EU region sales data from CSV.",
)
def ingest_eu(context) -> List[Dict[str, str]]:
    cfg: CSVIngestionConfig = context.op_config
    get_dagster_logger().info(f"Ingesting EU data from {cfg.file_path}")
    return _read_csv(cfg.file_path)


@op(
    config_schema=CSVIngestionConfig,
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Ingest APAC region sales data from CSV.",
)
def ingest_apac(context) -> List[Dict[str, str]]:
    cfg: CSVIngestionConfig = context.op_config
    get_dagster_logger().info(f"Ingesting APAC data from {cfg.file_path}")
    return _read_csv(cfg.file_path)


@op(description="Pass‑through conversion for US‑East (already USD).")
def convert_us_east(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    get_dagster_logger().info("US-East data requires no currency conversion.")
    return data


@op(description="Pass‑through conversion for US‑West (already USD).")
def convert_us_west(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    get_dagster_logger().info("US-West data requires no currency conversion.")
    return data


@op(
    required_resource_keys={"exchange_rate"},
    description="Convert EU data from EUR to USD using exchange rates.",
)
def convert_eu(data: List[Dict[str, str]], context) -> List[Dict[str, str]]:
    exchange: ExchangeRateResource = context.resources.exchange_rate
    converted = []
    for row in data:
        amount = float(row.get("revenue", 0))
        usd_amount = exchange.convert(amount, "EUR")
        new_row = dict(row)
        new_row["revenue_usd"] = f"{usd_amount:.2f}"
        converted.append(new_row)
    get_dagster_logger().info("EU data converted to USD.")
    return converted


@op(
    required_resource_keys={"exchange_rate"},
    description="Convert APAC data from JPY to USD using exchange rates.",
)
def convert_apac(data: List[Dict[str, str]], context) -> List[Dict[str, str]]:
    exchange: ExchangeRateResource = context.resources.exchange_rate
    converted = []
    for row in data:
        amount = float(row.get("revenue", 0))
        usd_amount = exchange.convert(amount, "JPY")
        new_row = dict(row)
        new_row["revenue_usd"] = f"{usd_amount:.2f}"
        converted.append(new_row)
    get_dagster_logger().info("APAC data converted to USD.")
    return converted


@op(
    config_schema=AggregationConfig,
    description="Aggregate regional revenues and generate a global report CSV.",
)
def aggregate(
    us_east: List[Dict[str, str]],
    us_west: List[Dict[str, str]],
    eu: List[Dict[str, str]],
    apac: List[Dict[str, str]],
    context,
) -> Dict[str, float]:
    def sum_revenue(rows: List[Dict[str, str]]) -> float:
        total = 0.0
        for row in rows:
            # Prefer the USD column if present, otherwise assume original revenue is USD
            amount_str = row.get("revenue_usd") or row.get("revenue", "0")
            total += float(amount_str)
        return total

    totals = {
        "US_East": sum_revenue(us_east),
        "US_West": sum_revenue(us_west),
        "EU": sum_revenue(eu),
        "APAC": sum_revenue(apac),
    }
    totals["Global_Total"] = sum(totals.values())

    cfg: AggregationConfig = context.op_config
    output_path = cfg.output_path
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["region", "revenue_usd"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for region, revenue in totals.items():
            writer.writerow({"region": region, "revenue_usd": f"{revenue:.2f}"})

    get_dagster_logger().info(f"Global revenue report written to {output_path}")
    return totals


@op
def end_pipeline() -> None:
    """Marks the end of the pipeline."""
    get_dagster_logger().info("Pipeline completed.")


@job(
    resource_defs={"exchange_rate": ResourceDefinition.hardcoded_resource(ExchangeRateResource())},
    description="Multi‑region ecommerce analytics pipeline.",
)
def ecommerce_analytics():
    start = start_pipeline()
    # Ingestion phase
    us_east_raw = ingest_us_east()
    us_west_raw = ingest_us_west()
    eu_raw = ingest_eu()
    apac_raw = ingest_apac()

    # Conversion phase
    us_east_converted = convert_us_east(us_east_raw)
    us_west_converted = convert_us_west(us_west_raw)
    eu_converted = convert_eu(eu_raw)
    apac_converted = convert_apac(apac_raw)

    # Aggregation phase
    agg = aggregate(
        us_east=us_east_converted,
        us_west=us_west_converted,
        eu=eu_converted,
        apac=apac_converted,
    )

    # End
    end = end_pipeline()
    # Set explicit dependencies to enforce ordering
    start >> [us_east_raw, us_west_raw, eu_raw, apac_raw]
    [us_east_raw, us_west_raw, eu_raw, apac_raw] >> [
        us_east_converted,
        us_west_converted,
        eu_converted,
        apac_converted,
    ]
    [us_east_converted, us_west_converted, eu_converted, apac_converted] >> agg
    agg >> end


daily_schedule = ScheduleDefinition(
    job=ecommerce_analytics,
    cron_schedule="0 2 * * *",  # Runs daily at 02:00 UTC
    execution_timezone="UTC",
    description="Daily execution of the ecommerce analytics pipeline.",
    # No catchup behavior is default for Dagster schedules
)


defs = Definitions(
    jobs=[ecommerce_analytics],
    schedules=[daily_schedule],
    resources={"exchange_rate": ExchangeRateResource()},
)


if __name__ == "__main__":
    result = ecommerce_analytics.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")