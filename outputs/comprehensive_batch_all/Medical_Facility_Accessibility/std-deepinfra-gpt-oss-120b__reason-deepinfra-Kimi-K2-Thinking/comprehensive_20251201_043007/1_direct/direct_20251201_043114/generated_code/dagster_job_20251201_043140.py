import os
import json
import csv
from pathlib import Path
from typing import Dict, List

from dagster import (
    op,
    job,
    ConfigurableResource,
    InitResourceContext,
    In,
    Out,
    Nothing,
    ResourceDefinition,
)


class DataDirResource(ConfigurableResource):
    """Resource providing the path to the shared data directory."""

    path: str = "./data"

    def get_path(self) -> Path:
        """Return the absolute Path object for the data directory."""
        return Path(self.path).expanduser().resolve()


def _ensure_dir(file_path: Path) -> None:
    """Create parent directories for the given file path if they do not exist."""
    file_path.parent.mkdir(parents=True, exist_ok=True)


def _read_csv(csv_path: Path) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dictionaries."""
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_json(data: List[Dict], json_path: Path) -> None:
    """Write a list of dictionaries to a JSON file."""
    _ensure_dir(json_path)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _read_json(json_path: Path) -> List[Dict]:
    """Read a JSON file containing a list of dictionaries."""
    with json_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_csv(data: List[Dict], csv_path: Path) -> None:
    """Write a list of dictionaries to a CSV file."""
    if not data:
        return
    _ensure_dir(csv_path)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


@op(
    required_resource_keys={"data_dir"},
    out={"json_path": Out(str)},
    description="Load facilities CSV, clean, and write JSON.",
)
def load_and_modify(context: InitResourceContext) -> str:
    data_dir: DataDirResource = context.resources.data_dir
    csv_path = data_dir.get_path() / "facilities.csv"
    json_path = data_dir.get_path() / "table_data_2.json"

    if not csv_path.is_file():
        context.log.warning(f"Input CSV not found at {csv_path}. Using empty dataset.")
        rows = []
    else:
        rows = _read_csv(csv_path)

    # Minimal cleaning: strip whitespace from all string fields
    cleaned = [
        {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        for row in rows
    ]

    _write_json(cleaned, json_path)
    context.log.info(f"Wrote cleaned data to {json_path}")
    return str(json_path)


@op(
    ins={"json_path": In(str)},
    required_resource_keys={"data_dir"},
    out={"reconciled_path": Out(str)},
    description="Geocode addresses using HERE API (simulated).",
)
def reconcile_geocode(context: InitResourceContext, json_path: str) -> str:
    data_dir: DataDirResource = context.resources.data_dir
    input_path = Path(json_path)
    output_path = data_dir.get_path() / "reconciled_table_2.json"

    data = _read_json(input_path)

    # Simulate geocoding by adding dummy latitude/longitude
    for row in data:
        row.setdefault("latitude", 0.0)
        row.setdefault("longitude", 0.0)

    _write_json(data, output_path)
    context.log.info(f"Geocoded data written to {output_path}")
    return str(output_path)


@op(
    ins={"json_path": In(str)},
    required_resource_keys={"data_dir"},
    out={"pt_distance_path": Out(str)},
    description="Calculate distance to nearest public transport stop (simulated).",
)
def calculate_distance_pt(context: InitResourceContext, json_path: str) -> str:
    data_dir: DataDirResource = context.resources.data_dir
    input_path = Path(json_path)
    output_path = data_dir.get_path() / "distance_pt_2.json"

    data = _read_json(input_path)

    # Simulate distance calculation
    for row in data:
        row.setdefault("distance_to_pt", 0.0)

    _write_json(data, output_path)
    context.log.info(f"Public transport distances added to {output_path}")
    return str(output_path)


@op(
    ins={"json_path": In(str)},
    required_resource_keys={"data_dir"},
    out={"final_json_path": Out(str)},
    description="Calculate distance to nearest residential area (simulated).",
)
def calculate_distance_residential(context: InitResourceContext, json_path: str) -> str:
    data_dir: DataDirResource = context.resources.data_dir
    input_path = Path(json_path)
    output_path = data_dir.get_path() / "column_extended_2.json"

    data = _read_json(input_path)

    # Simulate distance calculation
    for row in data:
        row.setdefault("distance_to_residential", 0.0)

    _write_json(data, output_path)
    context.log.info(f"Residential distances added to {output_path}")
    return str(output_path)


@op(
    ins={"json_path": In(str)},
    required_resource_keys={"data_dir"},
    out={"csv_path": Out(str)},
    description="Export enriched data to CSV.",
)
def save_final_data(context: InitResourceContext, json_path: str) -> str:
    data_dir: DataDirResource = context.resources.data_dir
    input_path = Path(json_path)
    output_path = data_dir.get_path() / "enriched_data_2.csv"

    data = _read_json(input_path)
    _write_csv(data, output_path)
    context.log.info(f"Final CSV written to {output_path}")
    return str(output_path)


@job(
    resource_defs={"data_dir": ResourceDefinition.hardcoded_resource(DataDirResource())},
    description="Medical facility accessibility pipeline.",
)
def medical_facility_accessibility_job():
    json_path_1 = load_and_modify()
    json_path_2 = reconcile_geocode(json_path=json_path_1)
    json_path_3 = calculate_distance_pt(json_path=json_path_2)
    json_path_4 = calculate_distance_residential(json_path=json_path_3)
    save_final_data(json_path=json_path_4)


if __name__ == "__main__":
    result = medical_facility_accessibility_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")