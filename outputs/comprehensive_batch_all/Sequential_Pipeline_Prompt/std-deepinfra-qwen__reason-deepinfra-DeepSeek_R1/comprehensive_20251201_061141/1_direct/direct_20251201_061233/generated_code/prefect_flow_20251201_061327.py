from prefect import flow, task
import os
import requests

DATA_DIR = os.getenv("DATA_DIR", "/app/data")
DATASET_ID = os.getenv("DATASET_ID", "2")
DATE_COLUMN = os.getenv("DATE_COLUMN", "Fecha_id")
TABLE_NAMING_CONVENTION = os.getenv("TABLE_NAMING_CONVENTION", "JOT_{}")
RECONCILIATOR_ID = os.getenv("RECONCILIATOR_ID", "geocodingHere")
EXTENDER_ID = os.getenv("EXTENDER_ID", "reconciledColumnExt")


@task
def load_and_modify_data():
    """Load and modify CSV data to JSON format."""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    for file in files:
        with open(os.path.join(DATA_DIR, file), 'r') as f:
            data = f.read()
        response = requests.post(
            "http://load-and-modify:3003/modify",
            json={
                "data": data,
                "dataset_id": DATASET_ID,
                "date_column": DATE_COLUMN,
                "table_naming_convention": TABLE_NAMING_CONVENTION
            }
        )
        with open(os.path.join(DATA_DIR, f"table_data_{file}.json"), 'w') as f:
            f.write(response.text)


@task
def data_reconciliation():
    """Reconcile city names using HERE geocoding service."""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith('table_data_') and f.endswith('.json')]
    for file in files:
        with open(os.path.join(DATA_DIR, file), 'r') as f:
            data = f.read()
        response = requests.post(
            "http://reconciliation:3003/reconcile",
            json={
                "data": data,
                "primary_column": "City",
                "optional_columns": ["County", "Country"],
                "reconciliator_id": RECONCILIATOR_ID
            }
        )
        with open(os.path.join(DATA_DIR, f"reconciled_table_{file}.json"), 'w') as f:
            f.write(response.text)


@task
def openmeteo_data_extension():
    """Enrich the dataset with weather information from OpenMeteo."""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith('reconciled_table_') and f.endswith('.json')]
    for file in files:
        with open(os.path.join(DATA_DIR, file), 'r') as f:
            data = f.read()
        response = requests.post(
            "http://openmeteo-extension:3003/extend",
            json={
                "data": data,
                "weather_attributes": ["apparent_temperature_max", "apparent_temperature_min", "precipitation_sum", "precipitation_hours"],
                "date_format": "%Y-%m-%d"
            }
        )
        with open(os.path.join(DATA_DIR, f"open_meteo_{file}.json"), 'w') as f:
            f.write(response.text)


@task
def column_extension():
    """Append additional data properties to the dataset."""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith('open_meteo_') and f.endswith('.json')]
    for file in files:
        with open(os.path.join(DATA_DIR, file), 'r') as f:
            data = f.read()
        response = requests.post(
            "http://column-extension:3003/extend",
            json={
                "data": data,
                "extender_id": EXTENDER_ID
            }
        )
        with open(os.path.join(DATA_DIR, f"column_extended_{file}.json"), 'w') as f:
            f.write(response.text)


@task
def save_final_data():
    """Consolidate and export the fully enriched dataset as a CSV file."""
    files = [f for f in os.listdir(DATA_DIR) if f.startswith('column_extended_') and f.endswith('.json')]
    for file in files:
        with open(os.path.join(DATA_DIR, file), 'r') as f:
            data = f.read()
        response = requests.post(
            "http://save:3003/save",
            json={
                "data": data
            }
        )
        with open(os.path.join(DATA_DIR, f"enriched_data_{file}.csv"), 'w') as f:
            f.write(response.text)


@flow
def data_processing_pipeline():
    """Orchestrates the data processing pipeline."""
    load_and_modify_data()
    data_reconciliation()
    openmeteo_data_extension()
    column_extension()
    save_final_data()


if __name__ == '__main__':
    data_processing_pipeline()