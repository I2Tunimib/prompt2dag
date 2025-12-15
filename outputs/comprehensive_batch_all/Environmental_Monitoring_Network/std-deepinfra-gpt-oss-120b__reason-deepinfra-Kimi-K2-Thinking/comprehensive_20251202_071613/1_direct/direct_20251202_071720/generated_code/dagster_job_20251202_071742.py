import os
import json
import random
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
from dagster import op, job, In, Out, Nothing, get_dagster_logger


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(data: List[Dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@op(out=Out(Path))
def load_and_modify() -> Path:
    """Load stations CSV, parse dates, standardize location names, and save as JSON."""
    logger = get_dagster_logger()
    csv_path = DATA_DIR / "stations.csv"
    json_path = DATA_DIR / "table_data_2.json"

    if not csv_path.exists():
        logger.error("Input file %s does not exist.", csv_path)
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)
    if "installation_date" in df.columns:
        df["installation_date"] = pd.to_datetime(df["installation_date"]).dt.strftime("%Y-%m-%d")
    if "location" in df.columns:
        df["location"] = df["location"].str.title().str.strip()

    records = df.to_dict(orient="records")
    _save_json(records, json_path)
    logger.info("Saved modified data to %s", json_path)
    return json_path


@op(ins={"input_path": In(Path)}, out=Out(Path))
def reconcile_geocode(input_path: Path) -> Path:
    """Mock geocoding: add latitude and longitude columns."""
    logger = get_dagster_logger()
    output_path = DATA_DIR / "reconciled_table_2.json"

    data = _load_json(input_path)
    for record in data:
        # Simple deterministic mock based on hash of location
        seed = hash(record.get("location", "")) % (2**32)
        random.seed(seed)
        record["latitude"] = round(random.uniform(-90, 90), 6)
        record["longitude"] = round(random.uniform(-180, 180), 6)

    _save_json(data, output_path)
    logger.info("Geocoded data saved to %s", output_path)
    return output_path


@op(ins={"input_path": In(Path)}, out=Out(Path))
def open_meteo_extension(input_path: Path) -> Path:
    """Mock weather extension: add weather variables."""
    logger = get_dagster_logger()
    output_path = DATA_DIR / "open_meteo_2.json"

    data = _load_json(input_path)
    for record in data:
        record["apparent_temperature_max"] = round(random.uniform(20, 40), 2)
        record["apparent_temperature_min"] = round(random.uniform(0, 20), 2)
        record["precipitation_sum"] = round(random.uniform(0, 200), 2)
        record["precipitation_hours"] = round(random.uniform(0, 24), 2)

    _save_json(data, output_path)
    logger.info("Weather data added and saved to %s", output_path)
    return output_path


@op(ins={"input_path": In(Path)}, out=Out(Path))
def land_use_extension(input_path: Path) -> Path:
    """Mock land use classification."""
    logger = get_dagster_logger()
    output_path = DATA_DIR / "land_use_2.json"

    land_use_options = ["Urban", "Agricultural", "Forest", "Wetland", "Barren"]
    data = _load_json(input_path)
    for record in data:
        record["land_use_type"] = random.choice(land_use_options)

    _save_json(data, output_path)
    logger.info("Land use data added and saved to %s", output_path)
    return output_path


@op(ins={"input_path": In(Path)}, out=Out(Path))
def population_density_extension(input_path: Path) -> Path:
    """Mock population density data."""
    logger = get_dagster_logger()
    output_path = DATA_DIR / "pop_density_2.json"

    data = _load_json(input_path)
    for record in data:
        record["population_density"] = round(random.uniform(10, 5000), 2)  # persons per km²

    _save_json(data, output_path)
    logger.info("Population density added and saved to %s", output_path)
    return output_path


@op(ins={"input_path": In(Path)}, out=Out(Path))
def environmental_calculation(input_path: Path) -> Path:
    """Compute a simple risk score based on selected columns."""
    logger = get_dagster_logger()
    output_path = DATA_DIR / "column_extended_2.json"

    data = _load_json(input_path)
    for record in data:
        precip = record.get("precipitation_sum", 0)
        pop_density = record.get("population_density", 0)
        # Simple risk formula: weighted sum (weights are illustrative)
        risk_score = (
            0.4 * (precip / 200) + 0.4 * (pop_density / 5000) + 0.2 * (1 if record.get("land_use_type") == "Urban" else 0)
        )
        record["risk_score"] = round(risk_score, 4)

    _save_json(data, output_path)
    logger.info("Risk scores calculated and saved to %s", output_path)
    return output_path


@op(ins={"input_path": In(Path)}, out=Out(Nothing))
def save_final_data(input_path: Path) -> None:
    """Export the final JSON data to CSV."""
    logger = get_dagster_logger()
    csv_path = DATA_DIR / "enriched_data_2.csv"

    data = _load_json(input_path)
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    logger.info("Final enriched CSV saved to %s", csv_path)


@job
def environmental_monitoring_job():
    """Orchestrates the full environmental monitoring data pipeline."""
    load_path = load_and_modify()
    reconciled_path = reconcile_geocode(load_path)
    weather_path = open_meteo_extension(reconciled_path)
    land_use_path = land_use_extension(weather_path)
    pop_density_path = population_density_extension(land_use_path)
    risk_path = environmental_calculation(pop_density_path)
    save_final_data(risk_path)


if __name__ == "__main__":
    result = environmental_monitoring_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")