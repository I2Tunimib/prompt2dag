# -*- coding: utf-8 -*-
"""
Generated DAG: retail_inventory_reconciliation
Description: No description provided.
Pattern: fanin
Generation timestamp: 2024-06-28T12:00:00Z
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.timezone import timezone
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException

# -------------------------------------------------------------------------
# Default arguments applied to all tasks
# -------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="retail_inventory_reconciliation",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "inventory", "reconciliation"],
    is_paused_upon_creation=True,  # DAG is disabled by default
    max_active_runs=1,
    timezone=timezone("UTC"),
) as dag:

    # ---------------------------------------------------------------------
    # Helper functions
    # ---------------------------------------------------------------------
    def _fetch_csv(connection_id: str) -> str:
        """
        Generic helper to fetch CSV content from an HTTP connection.
        Returns the CSV as a string.
        """
        hook = HttpHook(http_conn_id=connection_id, method="GET")
        try:
            response = hook.run(endpoint="/inventory.csv")
            if response.status_code != 200:
                raise AirflowException(
                    f"Failed to fetch CSV from {connection_id}: "
                    f"status {response.status_code}"
                )
            return response.text
        except Exception as exc:
            raise AirflowException(f"Error fetching CSV from {connection_id}") from exc

    def _normalize_csv(csv_content: str) -> List[Dict[str, Any]]:
        """
        Very simple CSV normalisation placeholder.
        Returns a list of dictionaries representing rows.
        """
        import csv
        from io import StringIO

        try:
            reader = csv.DictReader(StringIO(csv_content))
            normalized = []
            for row in reader:
                # Example normalisation: strip whitespace and uppercase SKU
                row["sku"] = row.get("sku", "").strip().upper()
                normalized.append(row)
            return normalized
        except Exception as exc:
            raise AirflowException("Error normalising CSV data") from exc

    # ---------------------------------------------------------------------
    # Fetch tasks
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def fetch_east_warehouse_csv() -> str:
        """Fetch East Warehouse CSV via HTTP connection."""
        return _fetch_csv("east_warehouse_api")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def fetch_north_warehouse_csv() -> str:
        """Fetch North Warehouse CSV via HTTP connection."""
        return _fetch_csv("north_warehouse_api")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def fetch_south_warehouse_csv() -> str:
        """Fetch South Warehouse CSV via HTTP connection."""
        return _fetch_csv("south_warehouse_api")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def fetch_west_warehouse_csv() -> str:
        """Fetch West Warehouse CSV via HTTP connection."""
        return _fetch_csv("west_warehouse_api")

    # ---------------------------------------------------------------------
    # Normalisation tasks
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def normalize_east_skus(csv_content: str) -> List[Dict[str, Any]]:
        """Normalise East Warehouse SKU data."""
        return _normalize_csv(csv_content)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def normalize_north_skus(csv_content: str) -> List[Dict[str, Any]]:
        """Normalise North Warehouse SKU data."""
        return _normalize_csv(csv_content)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def normalize_south_skus(csv_content: str) -> List[Dict[str, Any]]:
        """Normalise South Warehouse SKU data."""
        return _normalize_csv(csv_content)

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def normalize_west_skus(csv_content: str) -> List[Dict[str, Any]]:
        """Normalise West Warehouse SKU data."""
        return _normalize_csv(csv_content)

    # ---------------------------------------------------------------------
    # Reconciliation task
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def reconcile_all_inventories(
        east: List[Dict[str, Any]],
        north: List[Dict[str, Any]],
        south: List[Dict[str, Any]],
        west: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Combine all normalised inventories into a single reconciled list.
        Placeholder logic: concatenate lists.
        """
        try:
            reconciled = east + north + south + west
            # Real reconciliation logic would go here (e.g., deduplication,
            # quantity aggregation, discrepancy detection, etc.)
            return reconciled
        except Exception as exc:
            raise AirflowException("Error reconciling inventories") from exc

    # ---------------------------------------------------------------------
    # Reporting task
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def generate_final_report(reconciled_data: List[Dict[str, Any]]) -> str:
        """
        Generate a final report from reconciled inventory data.
        Returns a string representation (e.g., CSV or JSON) for simplicity.
        """
        import json

        try:
            report = json.dumps(reconciled_data, indent=2)
            # In a real pipeline you might write this to a file, S3, etc.
            return report
        except Exception as exc:
            raise AirflowException("Error generating final report") from exc

    # ---------------------------------------------------------------------
    # Task orchestration
    # ---------------------------------------------------------------------
    # Fetch
    east_csv = fetch_east_warehouse_csv()
    north_csv = fetch_north_warehouse_csv()
    south_csv = fetch_south_warehouse_csv()
    west_csv = fetch_west_warehouse_csv()

    # Normalise
    east_norm = normalize_east_skus(east_csv)
    north_norm = normalize_north_skus(north_csv)
    south_norm = normalize_south_skus(south_csv)
    west_norm = normalize_west_skus(west_csv)

    # Reconcile
    reconciled = reconcile_all_inventories(
        east=east_norm,
        north=north_norm,
        south=south_norm,
        west=west_norm,
    )

    # Report
    final_report = generate_final_report(reconciled)

    # Define explicit dependencies (optional, as XCom chaining already sets them)
    (
        east_csv
        >> east_norm
    )
    (
        north_csv
        >> north_norm
    )
    (
        south_csv
        >> south_norm
    )
    (
        west_csv
        >> west_norm
    )
    (
        east_norm,
        north_norm,
        south_norm,
        west_norm,
    ) >> reconciled >> final_report

# End of DAG definition.