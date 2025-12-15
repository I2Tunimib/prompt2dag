import os
import json
import random
from datetime import datetime
from typing import List, Dict

import pandas as pd
from dagster import op, job, In, Out, Nothing, get_dagster_logger


def _data_dir() -> str:
    """Return the data directory from the environment or default to the current directory."""
    return os.getenv("DATA_DIR", ".")


@op(out=Out(str))
def load_and_modify_op() -> str:
    """
    Load stations CSV, parse installation_date, standardize location names,
    and write the result to a JSON file.
    """
    logger = get_dagster_logger()
    csv_path = os.path.join(_data_dir(), "stations.csv")
    json_path = os.path.join(_data_dir(), "table_data_2.json")

    logger.info(f"Reading CSV from {csv_path}")
    df = pd.read_csv(csv_path)

    # Parse date column
    if "installation_date" in df.columns:
        df["installation_date"] = pd.to_datetime(df["installation_date"], errors="coerce")

    # Standardize location names
    if "location" in df.columns:
        df["location"] = df["location"].astype(str).str.strip().str.title()

    # Convert to list of dicts and write JSON
    records: List[Dict] = df.to_dict(orient="records")
    logger.info(f"Writing JSON to {json_path}")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return json_path


@op(ins={"input_path": In(str)}, out=Out(str))
def reconcile_geocode_op(input_path: str) -> str:
    """
    Mock geocoding: read JSON, add latitude and longitude fields,
    and write the reconciled JSON.
    """
    logger = get_dagster_logger()
    output_path = os.path.join(_data_dir(), "reconciled_table_2.json")

    logger.info(f"Reading JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        # Mock geocoding with random coordinates
        rec["latitude"] = round(random.uniform(-90, 90), 6)
        rec["longitude"] = round(random.uniform(-180, 180), 6)

    logger.info(f"Writing reconciled JSON to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return output_path


@op(ins={"input_path": In(str)}, out=Out(str))
def open_meteo_extension_op(input_path: str) -> str:
    """
    Mock OpenMeteo extension: add weather variables to each record.
    """
    logger = get_dagster_logger()
    output_path = os.path.join(_data_dir(), "open_meteo_2.json")

    logger.info(f"Reading JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        rec["apparent_temperature_max"] = round(random.uniform(20, 40), 1)
        rec["apparent_temperature_min"] = round(random.uniform(0, 20), 1)
        rec["precipitation_sum"] = round(random.uniform(0, 200), 1)
        rec["precipitation_hours"] = round(random.uniform(0, 24), 1)

    logger.info(f"Writing OpenMeteo JSON to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return output_path


@op(ins={"input_path": In(str)}, out=Out(str))
def land_use_extension_op(input_path: str) -> str:
    """
    Mock Land Use extension: add a land_use_type field.
    """
    logger = get_dagster_logger()
    output_path = os.path.join(_data_dir(), "land_use_2.json")

    logger.info(f"Reading JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    land_use_options = ["urban", "rural", "forest", "wetland"]
    for rec in records:
        rec["land_use_type"] = random.choice(land_use_options)

    logger.info(f"Writing Land Use JSON to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return output_path


@op(ins={"input_path": In(str)}, out=Out(str))
def population_density_extension_op(input_path: str) -> str:
    """
    Mock Population Density extension: add a population_density field.
    """
    logger = get_dagster_logger()
    output_path = os.path.join(_data_dir(), "pop_density_2.json")

    logger.info(f"Reading JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        rec["population_density"] = round(random.uniform(10, 5000), 1)  # people per km²

    logger.info(f"Writing Population Density JSON to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return output_path


@op(ins={"input_path": In(str)}, out=Out(str))
def environmental_calculation_op(input_path: str) -> str:
    """
    Compute a simple environmental risk score based on selected columns.
    """
    logger = get_dagster_logger()
    output_path = os.path.join(_data_dir(), "column_extended_2.json")

    logger.info(f"Reading JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    for rec in records:
        precip = rec.get("precipitation_sum", 0)
        pop_density = rec.get("population_density", 0)
        land_use = rec.get("land_use_type", "").lower()
        # Simple mock formula
        risk_score = (
            precip * 0.1
            + pop_density * 0.001
            + (5 if land_use == "urban" else 0)
        )
        rec["risk_score"] = round(risk_score, 3)

    logger.info(f"Writing risk score JSON to {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=str, indent=2)

    return output_path


@op(ins={"input_path": In(str)}, out=Out(Nothing))
def save_final_data_op(input_path: str) -> Nothing:
    """
    Convert the final JSON dataset to CSV.
    """
    logger = get_dagster_logger()
    csv_path = os.path.join(_data_dir(), "enriched_data_2.csv")

    logger.info(f"Reading final JSON from {input_path}")
    with open(input_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    logger.info(f"Saving final CSV to {csv_path}")
    df.to_csv(csv_path, index=False)

    return Nothing


@job
def environmental_monitoring_job():
    """
    Orchestrates the environmental monitoring data pipeline.
    """
    load_path = load_and_modify_op()
    reconcile_path = reconcile_geocode_op(load_path)
    open_meteo_path = open_meteo_extension_op(reconcile_path)
    land_use_path = land_use_extension_op(open_meteo_path)
    pop_density_path = population_density_extension_op(land_use_path)
    risk_path = environmental_calculation_op(pop_density_path)
    save_final_data_op(risk_path)


if __name__ == "__main__":
    result = environmental_monitoring_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")