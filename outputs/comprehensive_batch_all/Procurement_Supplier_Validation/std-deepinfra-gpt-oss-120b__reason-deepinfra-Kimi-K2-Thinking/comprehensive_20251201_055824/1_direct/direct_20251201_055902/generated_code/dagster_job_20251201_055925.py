import os
import json
import hashlib
from typing import List, Dict

import pandas as pd
from dagster import op, job, resource, In, Out, String, Nothing, get_dagster_logger


@resource(config_schema={"data_dir": String})
def data_dir_resource(init_context):
    """Provides the directory where input and output files are stored."""
    return init_context.resource_config["data_dir"]


@op(required_resource_keys={"data_dir"}, out=Out(String))
def load_and_modify(context) -> str:
    """
    Load ``suppliers.csv`` from the data directory, standardize column names,
    and write the data to ``table_data_2.json``.
    Returns the path to the generated JSON file.
    """
    logger = get_dagster_logger()
    data_dir = context.resources.data_dir
    csv_path = os.path.join(data_dir, "suppliers.csv")
    json_path = os.path.join(data_dir, "table_data_2.json")

    if not os.path.isfile(csv_path):
        logger.warning("Input CSV not found at %s. Creating dummy data.", csv_path)
        os.makedirs(data_dir, exist_ok=True)
        dummy_df = pd.DataFrame(
            {
                "supplier_name": ["Acme Corp", "Globex Inc", "Soylent Corp"],
                "location": ["New York", "San Francisco", "Chicago"],
                "contact_info": ["acme@example.com", "info@globex.com", "contact@soylent.com"],
            }
        )
        dummy_df.to_csv(csv_path, index=False)

    df = pd.read_csv(csv_path)

    # Standardize column names: strip whitespace and lower‑case
    df.columns = [col.strip().lower() for col in df.columns]

    # Write JSON records (list of dicts)
    records: List[Dict] = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Loaded CSV and wrote JSON to %s", json_path)
    return json_path


@op(required_resource_keys={"data_dir"}, ins={"json_path": In(String)}, out=Out(String))
def reconcile_entities(context, json_path: str) -> str:
    """
    Mock reconciliation of ``supplier_name`` against Wikidata.
    Adds a ``wikidata_id`` field to each record and writes the result to
    ``reconciled_table_2.json``.
    Returns the path to the reconciled JSON file.
    """
    logger = get_dagster_logger()
    data_dir = context.resources.data_dir
    reconciled_path = os.path.join(data_dir, "reconciled_table_2.json")

    with open(json_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    for record in records:
        name = record.get("supplier_name", "")
        # Deterministic mock ID based on hash of the name
        hash_bytes = hashlib.sha256(name.encode("utf-8")).digest()
        mock_id = int.from_bytes(hash_bytes[:4], "big") % 1_000_000
        record["wikidata_id"] = f"Q{mock_id}"

    with open(reconciled_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    logger.info("Reconciled entities and wrote JSON to %s", reconciled_path)
    return reconciled_path


@op(required_resource_keys={"data_dir"}, ins={"reconciled_path": In(String)}, out=Out(String))
def save_final_data(context, reconciled_path: str) -> str:
    """
    Convert the reconciled JSON back to CSV and write it as
    ``enriched_data_2.csv``.
    Returns the path to the final CSV file.
    """
    logger = get_dagster_logger()
    data_dir = context.resources.data_dir
    csv_out_path = os.path.join(data_dir, "enriched_data_2.csv")

    with open(reconciled_path, "r", encoding="utf-8") as f:
        records: List[Dict] = json.load(f)

    df = pd.DataFrame(records)
    df.to_csv(csv_out_path, index=False)

    logger.info("Saved final enriched CSV to %s", csv_out_path)
    return csv_out_path


@job(resource_defs={"data_dir": data_dir_resource})
def supplier_validation_job():
    """
    Orchestrates the supplier data validation pipeline:
    1. Load and modify raw CSV.
    2. Reconcile supplier names against Wikidata.
    3. Save the enriched data back to CSV.
    """
    json_path = load_and_modify()
    reconciled_path = reconcile_entities(json_path)
    save_final_data(reconciled_path)


if __name__ == "__main__":
    # Execute the job in-process with a default data directory.
    default_data_dir = os.getenv("DATA_DIR", "./data")
    result = supplier_validation_job.execute_in_process(
        run_config={
            "resources": {
                "data_dir": {"config": {"data_dir": default_data_dir}}
            }
        }
    )
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")