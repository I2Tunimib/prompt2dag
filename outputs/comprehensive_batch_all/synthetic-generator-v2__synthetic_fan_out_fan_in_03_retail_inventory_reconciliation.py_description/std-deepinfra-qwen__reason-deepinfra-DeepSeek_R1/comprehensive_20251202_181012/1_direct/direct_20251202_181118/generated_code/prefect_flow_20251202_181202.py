from prefect import flow, task
import pandas as pd

@task
def fetch_warehouse_csv(warehouse: str) -> pd.DataFrame:
    """Fetch CSV file from the specified warehouse."""
    # Mock CSV fetch
    return pd.DataFrame({
        'SKU': [f'{warehouse}-SKU-{i}' for i in range(1000)],
        'Quantity': [i for i in range(1000)]
    })

@task
def normalize_sku_format(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize SKU formats in the DataFrame."""
    df['SKU'] = df['SKU'].str.upper().str.strip()
    df['SKU'] = df['SKU'].str.replace(r'^N-', 'NORTH-', regex=True)
    df['SKU'] = df['SKU'].str.replace(r'^S-', 'SOUTH-', regex=True)
    df['SKU'] = df['SKU'].str.replace(r'^E-', 'EAST-', regex=True)
    df['SKU'] = df['SKU'].str.replace(r'^W-', 'WEST-', regex=True)
    return df

@task
def reconcile_inventory(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Reconcile inventory discrepancies across all warehouses."""
    combined_df = pd.concat(dfs)
    discrepancies = combined_df.groupby('SKU')['Quantity'].nunique().reset_index()
    discrepancies = discrepancies[discrepancies['Quantity'] > 1]
    return discrepancies

@task
def generate_discrepancy_report(discrepancies: pd.DataFrame) -> pd.DataFrame:
    """Generate a discrepancy report file."""
    # Mock report generation
    report = discrepancies.copy()
    report['Recommendation'] = 'Investigate and reconcile'
    return report

@task
def generate_final_report(discrepancy_report: pd.DataFrame) -> None:
    """Generate the final reconciliation report as a PDF."""
    # Mock PDF report generation
    print("Generating final PDF report...")
    discrepancy_report.to_csv('final_report.csv', index=False)
    print("Final report generated.")

@flow(retries=2, retry_delay_seconds=300)
def retail_inventory_reconciliation():
    """Retail inventory reconciliation pipeline."""
    # Fetch CSV files in parallel
    north_df = fetch_warehouse_csv.submit('north')
    south_df = fetch_warehouse_csv.submit('south')
    east_df = fetch_warehouse_csv.submit('east')
    west_df = fetch_warehouse_csv.submit('west')

    # Normalize SKU formats concurrently
    north_normalized = normalize_sku_format.submit(north_df)
    south_normalized = normalize_sku_format.submit(south_df)
    east_normalized = normalize_sku_format.submit(east_df)
    west_normalized = normalize_sku_format.submit(west_df)

    # Reconcile inventory discrepancies
    normalized_dfs = [north_normalized, south_normalized, east_normalized, west_normalized]
    discrepancies = reconcile_inventory(normalized_dfs)

    # Generate discrepancy report
    discrepancy_report = generate_discrepancy_report(discrepancies)

    # Generate final reconciliation report
    generate_final_report(discrepancy_report)

if __name__ == '__main__':
    retail_inventory_reconciliation()

# Deployment/schedule configuration (optional)
# Schedule: Daily execution starting January 1, 2024
# Maximum 2 retries with 5-minute delay between retries
# No catchup for missed executions
# Linear progression with parallel execution at two stages
# Maximum parallel width: 4 tasks (during fetch and normalization phases)