from prefect import flow, task
import requests
import os
import json
import csv

DATA_DIR = os.getenv("DATA_DIR", "./data")

@task
def load_and_modify_data():
    """
    Load supplier CSV, standardize formats, and convert to JSON.
    """
    url = "http://load-and-modify-service:3003/process"
    params = {
        "DATASET_ID": 2,
        "TABLE_NAME_PREFIX": "JOT_"
    }
    files = {"file": open(os.path.join(DATA_DIR, "suppliers.csv"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "table_data_2.json"), "w") as f:
        json.dump(response.json(), f)
    return os.path.join(DATA_DIR, "table_data_2.json")

@task
def reconcile_entities(input_file):
    """
    Disambiguate supplier_name using the Wikidata API to find canonical entities.
    """
    url = "http://reconciliation-service:3003/reconcile"
    params = {
        "PRIMARY_COLUMN": "supplier_name",
        "RECONCILIATOR_ID": "wikidataEntity",
        "DATASET_ID": 2
    }
    with open(input_file, "r") as f:
        data = json.load(f)
    response = requests.post(url, params=params, json=data)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "reconciled_table_2.json"), "w") as f:
        json.dump(response.json(), f)
    return os.path.join(DATA_DIR, "reconciled_table_2.json")

@task
def save_final_data(input_file):
    """
    Export the validated supplier data to CSV.
    """
    url = "http://save-service:3003/save"
    params = {
        "DATASET_ID": 2
    }
    with open(input_file, "r") as f:
        data = json.load(f)
    response = requests.post(url, params=params, json=data)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "enriched_data_2.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(data[0].keys())
        for row in data:
            writer.writerow(row.values())

@flow
def procurement_supplier_validation_pipeline():
    """
    Orchestrates the procurement supplier validation pipeline.
    """
    modified_data_file = load_and_modify_data()
    reconciled_data_file = reconcile_entities(modified_data_file)
    save_final_data(reconciled_data_file)

if __name__ == "__main__":
    procurement_supplier_validation_pipeline()