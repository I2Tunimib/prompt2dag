import os
import json
import hashlib
from typing import List, Dict

import pandas as pd
from dagster import op, job, Out, In, Nothing, get_dagster_logger


def _get_data_dir() -> str:
    """Return the data directory from the environment or a default."""
    return os.getenv("DATA_DIR", "/tmp/data")


@op(out=Out(str))
def load_and_modify() -> str:
    """
    Load the supplier CSV, standardize column names, and write a JSON file.

    Returns:
        The path to the generated JSON file.
    """
    logger = get_dagster_logger()
    data_dir = _get_data_dir()
    csv_path = os.path.join(data_dir, "suppliers.csv")
    json_path = os.path.join(data_dir, "table_data_2.json")

    if not os.path.isfile(csv_path):
        logger.error(f"Input CSV not found at {csv_path}")
        raise FileNotFoundError(csv_path)

    try:
        df = pd.read_csv(csv_path, dtype=str)
        # Standardize column names: lower case and replace spaces with underscores
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        records: List[Dict[str, str]] = df.to_dict(orient="records")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        logger.info(f"Wrote standardized JSON to {json_path}")
        return json_path
    except Exception as exc:
        logger.error(f"Failed to load and modify data: {exc}")
        raise


@op(ins={"json_path": In(str)}, out=Out(str))
def reconcile_entities(json_path: str) -> str:
    """
    Perform a dummy reconciliation of supplier names against Wikidata.

    Adds a synthetic ``wikidata_id`` field to each record.

    Args:
        json_path: Path to the input JSON file.

    Returns:
        Path to the reconciled JSON file.
    """
    logger = get_dagster_logger()
    data_dir = _get_data_dir()
    reconciled_path = os.path.join(data_dir, "reconciled_table_2.json")

    if not os.path.isfile(json_path):
        logger.error(f"Input JSON not found at {json_path}")
        raise FileNotFoundError(json_path)

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            records = json.load(f)

        for record in records:
            name = record.get("supplier_name", "")
            # Create a deterministic dummy Wikidata ID based on the name hash
            hash_bytes = hashlib.sha256(name.encode("utf-8")).digest()
            dummy_id = int.from_bytes(hash_bytes[:4], "big") % 1_000_000
            record["wikidata_id"] = f"Q{dummy_id}"

        with open(reconciled_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logger.info(f"Wrote reconciled JSON to {reconciled_path}")
        return reconciled_path
    except Exception as exc:
        logger.error(f"Failed during entity reconciliation: {exc}")
        raise


@op(ins={"reconciled_path": In(str)}, out=Out(Nothing))
def save_final_data(reconciled_path: str) -> None:
    """
    Export the reconciled supplier data to a CSV file.

    Args:
        reconciled_path: Path to the reconciled JSON file.
    """
    logger = get_dagster_logger()
    data_dir = _get_data_dir()
    output_csv = os.path.join(data_dir, "enriched_data_2.csv")

    if not os.path.isfile(reconciled_path):
        logger.error(f"Reconciled JSON not found at {reconciled_path}")
        raise FileNotFoundError(reconciled_path)

    try:
        with open(reconciled_path, "r", encoding="utf-8") as f:
            records = json.load(f)

        df = pd.DataFrame(records)
        df.to_csv(output_csv, index=False, encoding="utf-8")
        logger.info(f"Saved final CSV to {output_csv}")
    except Exception as exc:
        logger.error(f"Failed to save final data: {exc}")
        raise


@job
def supplier_validation_job():
    """
    Dagster job that orchestrates the supplier validation pipeline:
    1. Load and modify CSV data.
    2. Reconcile supplier names against Wikidata.
    3. Save the enriched data back to CSV.
    """
    json_path = load_and_modify()
    reconciled_path = reconcile_entities(json_path)
    save_final_data(reconciled_path)


if __name__ == "__main__":
    # Ensure the DATA_DIR environment variable points to a writable location
    # before executing the job, e.g.:
    # export DATA_DIR=/path/to/data
    result = supplier_validation_job.execute_in_process()
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")