import os
import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=1, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def load_and_modify(data_dir: Path) -> Path:
    """
    Load the supplier CSV, standardize formats, and write a JSON file.

    Args:
        data_dir: Directory containing the input CSV and where output will be stored.

    Returns:
        Path to the generated JSON file.
    """
    csv_path = data_dir / "suppliers.csv"
    json_path = data_dir / "table_data_2.json"

    if not csv_path.is_file():
        raise FileNotFoundError(f"Input CSV not found at {csv_path}")

    # Load CSV
    df = pd.read_csv(csv_path)

    # Example standardization: strip whitespace from string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # Convert to list of records and write JSON
    records: List[Dict] = df.to_dict(orient="records")
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return json_path


@task(retries=1, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def reconcile_entities(json_path: Path) -> Path:
    """
    Reconcile supplier names against Wikidata and augment records with a Wikidata ID.

    Args:
        json_path: Path to the JSON file produced by ``load_and_modify``.

    Returns:
        Path to the reconciled JSON file.
    """
    reconciled_path = json_path.parent / "reconciled_table_2.json"

    with json_path.open("r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    for record in records:
        supplier_name = record.get("supplier_name")
        if not supplier_name:
            record["wikidata_id"] = None
            continue

        # Simple Wikidata search using the public API
        try:
            response = requests.get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbsearchentities",
                    "search": supplier_name,
                    "language": "en",
                    "format": "json",
                    "limit": 1,
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("search"):
                wikidata_id = data["search"][0].get("id")
                record["wikidata_id"] = wikidata_id
            else:
                record["wikidata_id"] = None
        except Exception as exc:
            # In case of API failure, store None but do not stop the whole pipeline
            record["wikidata_id"] = None
            print(f"Warning: failed to reconcile '{supplier_name}': {exc}")

    with reconciled_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return reconciled_path


@task(retries=1, retry_delay_seconds=10)
def save_final_data(json_path: Path, data_dir: Path) -> Path:
    """
    Export the reconciled supplier data to a CSV file.

    Args:
        json_path: Path to the reconciled JSON file.
        data_dir: Directory where the final CSV will be written.

    Returns:
        Path to the generated CSV file.
    """
    csv_path = data_dir / "enriched_data_2.csv"

    with json_path.open("r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False)

    return csv_path


@flow
def supplier_validation_flow():
    """
    Orchestrates the supplier validation pipeline:
    1. Load & modify raw CSV data.
    2. Reconcile supplier names against Wikidata.
    3. Save the enriched data back to CSV.
    """
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)

    json_path = load_and_modify(data_dir)
    reconciled_path = reconcile_entities(json_path)
    final_csv = save_final_data(reconciled_path, data_dir)

    print(f"Pipeline completed. Enriched CSV available at: {final_csv}")


if __name__ == "__main__":
    # Local execution entry point.
    supplier_validation_flow()
    # Note: For production deployments, configure a Prefect deployment with a schedule as needed.