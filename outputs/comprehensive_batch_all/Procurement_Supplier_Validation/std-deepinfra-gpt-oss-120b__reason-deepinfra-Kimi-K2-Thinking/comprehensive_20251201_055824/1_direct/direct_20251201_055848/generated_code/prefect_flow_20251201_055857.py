import os
import json
import pathlib
from typing import Any, Dict

import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


DATA_DIR = os.getenv("DATA_DIR", "./data")
BASE_URL = "http://localhost:3003"


@task(retries=1, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_and_modify(data_dir: str = DATA_DIR) -> pathlib.Path:
    """Load the supplier CSV, standardize it, and write JSON output.

    Calls the external load‑and‑modify service.
    """
    csv_path = pathlib.Path(data_dir) / "suppliers.csv"
    json_path = pathlib.Path(data_dir) / "table_data_2.json"

    payload: Dict[str, Any] = {
        "dataset_id": 2,
        "table_name_prefix": "JOT_",
        "input_file": str(csv_path),
        "output_file": str(json_path),
    }

    try:
        response = requests.post(f"{BASE_URL}/load-modify", json=payload, timeout=30)
        response.raise_for_status()
        # Assume service writes the file; verify existence
        if not json_path.is_file():
            raise FileNotFoundError(f"Expected output not found: {json_path}")
    except Exception as exc:
        raise RuntimeError(f"Load and modify step failed: {exc}") from exc

    return json_path


@task(retries=1, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def reconcile_entities(json_path: pathlib.Path) -> pathlib.Path:
    """Reconcile supplier names against Wikidata and produce enriched JSON.

    Calls the external reconciliation service.
    """
    reconciled_path = json_path.parent / "reconciled_table_2.json"

    payload: Dict[str, Any] = {
        "primary_column": "supplier_name",
        "reconciliator_id": "wikidataEntity",
        "dataset_id": 2,
        "input_file": str(json_path),
        "output_file": str(reconciled_path),
    }

    try:
        response = requests.post(f"{BASE_URL}/reconcile", json=payload, timeout=30)
        response.raise_for_status()
        if not reconciled_path.is_file():
            raise FileNotFoundError(f"Expected reconciled output not found: {reconciled_path}")
    except Exception as exc:
        raise RuntimeError(f"Entity reconciliation step failed: {exc}") from exc

    return reconciled_path


@task(retries=1, retry_delay_seconds=10)
def save_final_data(json_path: pathlib.Path, data_dir: str = DATA_DIR) -> pathlib.Path:
    """Export the reconciled JSON to a CSV file.

    Calls the external save service.
    """
    csv_path = pathlib.Path(data_dir) / "enriched_data_2.csv"

    payload: Dict[str, Any] = {
        "dataset_id": 2,
        "input_file": str(json_path),
        "output_file": str(csv_path),
    }

    try:
        response = requests.post(f"{BASE_URL}/save", json=payload, timeout=30)
        response.raise_for_status()
        if not csv_path.is_file():
            raise FileNotFoundError(f"Expected CSV output not found: {csv_path}")
    except Exception as exc:
        raise RuntimeError(f"Save final data step failed: {exc}") from exc

    return csv_path


@flow
def supplier_validation_pipeline() -> pathlib.Path:
    """Orchestrates the supplier validation pipeline.

    Steps:
    1. Load & modify CSV to JSON.
    2. Reconcile supplier names with Wikidata.
    3. Save enriched data to CSV.
    """
    json_path = load_and_modify()
    reconciled_path = reconcile_entities(json_path)
    final_csv = save_final_data(reconciled_path)
    return final_csv


# Deployment schedule can be configured separately, e.g. using Prefect Cloud or
# Prefect Server UI to run this flow daily at 02:00 UTC.

if __name__ == "__main__":
    result_path = supplier_validation_pipeline()
    print(f"Pipeline completed. Final CSV located at: {result_path}")