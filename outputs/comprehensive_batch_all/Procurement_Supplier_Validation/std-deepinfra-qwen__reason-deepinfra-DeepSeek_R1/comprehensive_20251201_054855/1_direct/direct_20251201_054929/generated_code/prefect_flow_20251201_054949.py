from prefect import flow, task
import requests
import os
import json
import csv

DATA_DIR = os.getenv("DATA_DIR", "./data")

@task
def load_and_modify_data():
    """
    Load and modify supplier data from CSV to JSON.
    """
    url = "http://localhost:3003/load-and-modify"
    params = {
        "DATASET_ID": 2,
        "TABLE_NAME_PREFIX": "JOT_"
    }
    files = {"file": open(os.path.join(DATA_DIR, "suppliers.csv"), "rb")}
    response = requests.post(url, params=params, files=files)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "table_data_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def reconcile_entities():
    """
    Reconcile supplier names using the Wikidata API.
    """
    url = "http://localhost:3003/reconciliation"
    params = {
        "PRIMARY_COLUMN": "supplier_name",
        "RECONCILIATOR_ID": "wikidataEntity",
        "DATASET_ID": 2
    }
    with open(os.path.join(DATA_DIR, "table_data_2.json"), "r") as f:
        data = json.load(f)
    response = requests.post(url, params=params, json=data)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "reconciled_table_2.json"), "w") as f:
        json.dump(response.json(), f)

@task
def save_final_data():
    """
    Save the final validated supplier data to CSV.
    """
    url = "http://localhost:3003/save"
    params = {
        "DATASET_ID": 2
    }
    with open(os.path.join(DATA_DIR, "reconciled_table_2.json"), "r") as f:
        data = json.load(f)
    response = requests.post(url, params=params, json=data)
    response.raise_for_status()
    with open(os.path.join(DATA_DIR, "enriched_data_2.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(data[0].keys())
        for item in data:
            writer.writerow(item.values())

@flow
def procurement_supplier_validation_pipeline():
    """
    Orchestrates the procurement supplier validation pipeline.
    """
    load_and_modify_data()
    reconcile_entities()
    save_final_data()

if __name__ == "__main__":
    procurement_supplier_validation_pipeline()