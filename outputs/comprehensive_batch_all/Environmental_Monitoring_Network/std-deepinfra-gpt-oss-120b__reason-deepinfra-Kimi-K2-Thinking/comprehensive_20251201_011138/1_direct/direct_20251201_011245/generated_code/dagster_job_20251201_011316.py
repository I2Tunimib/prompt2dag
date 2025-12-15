import json
import os
from datetime import datetime
from hashlib import md5
from typing import List, Dict

import pandas as pd
from dagster import op, job, In, Out, Nothing, get_dagster_logger


# Default directory for data files; can be overridden by setting the DATA_DIR environment variable.
DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)


def _write_json(data: List[Dict], filename: str) -> str:
    """Write a list of dictionaries to a JSON file."""
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def _read_json(filename: str) -> List[Dict]:
    """Read a JSON file containing a list of dictionaries."""
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@op(out=Out(str, description="Path to the JSON file produced by load & modify step"))
def load_and_modify_op() -> str:
    """
    Load stations CSV, parse installation_date, standardize location names,
    and write the result to JSON.
    """
    logger = get_dagster_logger()
    csv_path = os.path.join(DATA_DIR, "stations.csv")
    if not os.path.exists(csv_path):
        logger.error("Input file %s does not exist.", csv_path)
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)

    # Parse date column
    if "installation_date" in df.columns:
        df["installation_date"] = pd.to_datetime(df["installation_date"], errors="coerce")
    else:
        logger.warning("installation_date column not found; proceeding without date parsing.")

    # Standardize location names
    if "location" in df.columns:
        df["location"] = df["location"].astype(str).str.strip().str.title()
    else:
        logger.warning("location column not found; proceeding without location standardization.")

    records = df.to_dict(orient="records")
    json_path = _write_json(records, "table_data_2.json")
    logger.info("Load & modify step completed: %s", json_path)
    return json_path


def _dummy_lat_lon(location: str) -> Dict[str, float]:
    """Generate deterministic dummy latitude and longitude based on location string."""
    hash_bytes = md5(location.encode("utf-8")).digest()
    lat = (int.from_bytes(hash_bytes[:4], "big") % 180) - 90  # -90 to +90
    lon = (int.from_bytes(hash_bytes[4:8], "big") % 360) - 180  # -180 to +180
    return {"latitude": float(lat), "longitude": float(lon)}


@op(ins={"input_path": In(str)}, out=Out(str))
def reconcile_geocode_op(input_path: str) -> str:
    """
    Geocode station locations by adding latitude and longitude fields.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))

    for rec in records:
        location = rec.get("location", "")
        coords = _dummy_lat_lon(location)
        rec.update(coords)

    json_path = _write_json(records, "reconciled_table_2.json")
    logger.info("Geocoding step completed: %s", json_path)
    return json_path


def _dummy_weather(record: Dict) -> Dict:
    """Add dummy weather variables based on date and coordinates."""
    # Simple deterministic values using hash of date string
    date_str = record.get("installation_date")
    if isinstance(date_str, str):
        date_hash = int(md5(date_str.encode("utf-8")).hexdigest(), 16)
    else:
        date_hash = 0
    record["apparent_temperature_max"] = 30 + (date_hash % 10)
    record["apparent_temperature_min"] = 15 + (date_hash % 5)
    record["precipitation_sum"] = (date_hash % 20) * 0.1
    record["precipitation_hours"] = (date_hash % 24) * 0.5
    return record


@op(ins={"input_path": In(str)}, out=Out(str))
def open_meteo_extension_op(input_path: str) -> str:
    """
    Extend data with historical weather information using dummy values.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))

    for rec in records:
        _dummy_weather(rec)

    json_path = _write_json(records, "open_meteo_2.json")
    logger.info("OpenMeteo extension step completed: %s", json_path)
    return json_path


def _dummy_land_use(record: Dict) -> str:
    """Assign a dummy land use type based on latitude."""
    lat = record.get("latitude", 0)
    if lat > 0:
        return "Urban"
    elif lat < -30:
        return "Forest"
    else:
        return "Rural"


@op(ins={"input_path": In(str)}, out=Out(str))
def land_use_extension_op(input_path: str) -> str:
    """
    Add land use classification using a deterministic dummy algorithm.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))

    for rec in records:
        rec["land_use_type"] = _dummy_land_use(rec)

    json_path = _write_json(records, "land_use_2.json")
    logger.info("Land use extension step completed: %s", json_path)
    return json_path


def _dummy_population_density(record: Dict) -> float:
    """Generate a dummy population density based on latitude and land use."""
    lat = record.get("latitude", 0)
    land_use = record.get("land_use_type", "")
    base = 1000 - abs(lat) * 5
    if land_use == "Urban":
        base *= 2
    elif land_use == "Forest":
        base *= 0.5
    return max(base, 10)


@op(ins={"input_path": In(str)}, out=Out(str))
def population_density_extension_op(input_path: str) -> str:
    """
    Add population density data using dummy calculations.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))

    for rec in records:
        rec["population_density"] = _dummy_population_density(rec)

    json_path = _write_json(records, "pop_density_2.json")
    logger.info("Population density extension step completed: %s", json_path)
    return json_path


def _calculate_risk_score(record: Dict) -> float:
    """Simple risk score based on precipitation, population density, and land use."""
    precip = record.get("precipitation_sum", 0)
    pop_density = record.get("population_density", 0)
    land_use = record.get("land_use_type", "")
    land_use_factor = 5 if land_use == "Urban" else 2 if land_use == "Rural" else 1
    return precip * 0.1 + pop_density * 0.001 + land_use_factor


@op(ins={"input_path": In(str)}, out=Out(str))
def environmental_calculation_op(input_path: str) -> str:
    """
    Compute environmental risk score and add it as a new column.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))

    for rec in records:
        rec["risk_score"] = _calculate_risk_score(rec)

    json_path = _write_json(records, "column_extended_2.json")
    logger.info("Environmental calculation step completed: %s", json_path)
    return json_path


@op(ins={"input_path": In(str)}, out=Out(Nothing))
def save_final_data_op(input_path: str) -> Nothing:
    """
    Export the final enriched dataset to CSV.
    """
    logger = get_dagster_logger()
    records = _read_json(os.path.basename(input_path))
    df = pd.DataFrame(records)

    # Ensure dates are formatted as strings for CSV output
    if "installation_date" in df.columns:
        df["installation_date"] = pd.to_datetime(df["installation_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    csv_path = os.path.join(DATA_DIR, "enriched_data_2.csv")
    df.to_csv(csv_path, index=False)
    logger.info("Final CSV saved to %s", csv_path)
    return Nothing


@job
def env_monitoring_job():
    """
    Dagster job orchestrating the environmental monitoring data pipeline.
    """
    load_path = load_and_modify_op()
    reconcile_path = reconcile_geocode_op(load_path)
    open_meteo_path = open_meteo_extension_op(reconcile_path)
    land_use_path = land_use_extension_op(open_meteo_path)
    pop_density_path = population_density_extension_op(land_use_path)
    risk_path = environmental_calculation_op(pop_density_path)
    save_final_data_op(risk_path)


if __name__ == "__main__":
    result = env_monitoring_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")