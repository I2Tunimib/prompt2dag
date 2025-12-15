import os
import json
from pathlib import Path
from typing import List

from prefect import flow, task


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET_ID = os.getenv("DATASET_ID", "2")
DATE_COLUMN = os.getenv("DATE_COLUMN", "Fecha_id")
TABLE_PREFIX = "JOT_"
RECONCILIATOR_ID = os.getenv("RECONCILIATOR_ID", "geocodingHere")
EXTENDER_ID = os.getenv("EXTENDER_ID", "reconciledColumnExt")


def _ensure_data_dir() -> None:
    """Make sure the data directory exists."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


@task
def load_and_modify() -> List[Path]:
    """Ingest CSV files and convert them to JSON format."""
    _ensure_data_dir()
    csv_files = list(DATA_DIR.glob("*.csv"))
    json_files: List[Path] = []

    for csv_path in csv_files:
        table_name = f"{TABLE_PREFIX}{csv_path.stem}"
        json_path = DATA_DIR / f"table_data_{table_name}.json"

        # Placeholder for actual Docker call.
        # Example Docker command:
        # docker run --rm -v {DATA_DIR}:/data -e DATASET_ID={DATASET_ID} \
        #   -e DATE_COLUMN={DATE_COLUMN} -e TABLE_NAME={table_name} \
        #   i2t-backendwithintertwino6-load-and-modify:latest

        # Simulate output by creating an empty JSON array.
        json_path.write_text(json.dumps([]))
        json_files.append(json_path)

    return json_files


@task
def data_reconciliation(input_files: List[Path]) -> List[Path]:
    """Standardize city names using the HERE geocoding service."""
    reconciled_files: List[Path] = []

    for in_path in input_files:
        identifier = in_path.stem.replace("table_data_", "")
        out_path = DATA_DIR / f"reconciled_table_{identifier}.json"

        # Placeholder for actual Docker call.
        # docker run ... i2t-backendwithintertwino6-reconciliation:latest

        out_path.write_text(json.dumps([]))
        reconciled_files.append(out_path)

    return reconciled_files


@task
def openmeteo_extension(input_files: List[Path]) -> List[Path]:
    """Enrich the dataset with weather information from OpenMeteo."""
    enriched_files: List[Path] = []

    for in_path in input_files:
        identifier = in_path.stem.replace("reconciled_table_", "")
        out_path = DATA_DIR / f"open_meteo_{identifier}.json"

        # Placeholder for actual Docker call.
        # docker run ... i2t-backendwithintertwino6-openmeteo-extension:latest

        out_path.write_text(json.dumps([]))
        enriched_files.append(out_path)

    return enriched_files


@task
def column_extension(input_files: List[Path]) -> List[Path]:
    """Append additional data properties as defined by integration parameters."""
    extended_files: List[Path] = []

    for in_path in input_files:
        identifier = in_path.stem.replace("open_meteo_", "")
        out_path = DATA_DIR / f"column_extended_{identifier}.json"

        # Placeholder for actual Docker call.
        # docker run ... i2t-backendwithintertwino6-column-extension:latest

        out_path.write_text(json.dumps([]))
        extended_files.append(out_path)

    return extended_files


@task
def save_final_data(input_files: List[Path]) -> List[Path]:
    """Consolidate and export the fully enriched dataset as CSV."""
    final_files: List[Path] = []

    for in_path in input_files:
        identifier = in_path.stem.replace("column_extended_", "")
        out_path = DATA_DIR / f"enriched_data_{identifier}.csv"

        # Placeholder for actual Docker call.
        # docker run ... i2t-backendwithintertwino6-save:latest

        # Simulate CSV output with a header row.
        out_path.write_text("id,name,value\n")
        final_files.append(out_path)

    return final_files


@flow
def data_processing_pipeline() -> None:
    """Orchestrates the sequential data processing pipeline."""
    loaded = load_and_modify()
    reconciled = data_reconciliation(loaded)
    weather_enriched = openmeteo_extension(reconciled)
    column_extended = column_extension(weather_enriched)
    _ = save_final_data(column_extended)


if __name__ == "__main__":
    # To schedule this flow, create a Prefect deployment with a cron schedule.
    data_processing_pipeline()