import hashlib
from typing import Any
import pandas as pd
from prefect import flow, task


@task(
    name="extract_claims",
    retries=2,
    retry_delay_seconds=300,
    description="Extracts patient claims data from CSV file"
)
def extract_claims(csv_path: str = "data/claims.csv") -> pd.DataFrame:
    """
    Extract patient claims data from CSV.
    
    Args:
        csv_path: Path to claims CSV file
        
    Returns:
        DataFrame containing claims data
    """
    # Simulate CSV extraction
    # In production, this would read from actual CSV file:
    # return pd.read_csv(csv_path)
    
    # Sample data for demonstration
    data = {
        'claim_id': [1001, 1002, 1003, 1004],
        'patient_id': ['P001', 'P002', 'P003', 'P001'],
        'provider_id': ['PRV001', 'PRV002', 'PRV003', 'PRV001'],
        'procedure_code': ['A123', 'B456', 'A123', 'C789'],
        'claim_amount': [150.00, 250.00, 175.00, 500.00],
        'claim_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04']
    }
    return pd.DataFrame(data)


@task(
    name="extract_providers",
    retries=2,
    retry_delay_seconds=300,
    description="Extracts provider data from CSV file"
)
def extract_providers(csv_path: str = "data/providers.csv") -> pd.DataFrame:
    """
    Extract provider data from CSV.
    
    Args:
        csv_path: Path to providers CSV file
        
    Returns:
        DataFrame containing provider data
    """
    # Simulate CSV extraction
    # In production, this would read from actual CSV file:
    # return pd.read_csv(csv_path)
    
    # Sample data for demonstration
    data = {
        'provider_id': ['PRV001', 'PRV002', 'PRV003'],
        'provider_name': ['Dr. Smith', 'Dr. Jones', 'Dr. Brown'],
        'specialty': ['Cardiology', 'Neurology', 'Orthopedics'],
        'location': ['New York', 'Boston', 'Chicago']
    }
    return pd.DataFrame(data)


@task(
    name="transform_join",
    retries=2,
    retry_delay_seconds=300,
    description="Joins data, anonymizes PII, and calculates risk scores"
)
def transform_join(
    claims_df: pd.DataFrame,
    providers_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform data by joining claims and providers, anonymizing PII,
    and calculating risk scores.
    
    Args:
        claims_df: Claims DataFrame from extract_claims
        providers_df: Providers DataFrame from extract_providers
        
    Returns:
        Transformed DataFrame with anonymized data and risk scores
    """
    # Join claims and provider data
    joined_df = claims_df.merge(
        providers_df,
        on='provider_id',
        how='left'
    )
    
    # Anonymize PII by hashing patient identifiers
    def hash_identifier(identifier: str) -> str:
        """Hash patient identifier using SHA-256."""
        return hashlib.sha256(identifier.encode()).hexdigest()[:16]
    
    joined_df['patient_id_hashed'] = joined_df['patient_id'].apply(hash_identifier)
    # Drop original patient_id for privacy
    joined_df = joined_df.drop(columns=['patient_id'])
    
    # Calculate risk scores based on procedure codes and amounts
    def calculate_risk_score(row: pd.Series) -> float:
        """
        Calculate risk score based on claim amount and procedure code.
        Score ranges from 0-10.
        """
        # Base score from amount (capped at $1000 = 10 points)
        base_score = min(row['claim_amount'] / 100.0, 10.0)
        
        # Procedure code risk multipliers
        high_risk_codes = {'A123', 'C789'}  # High-risk procedures
        medium_risk_codes = {'B456'}  # Medium-risk procedures
        
        if row['procedure_code'] in high_risk_codes:
            multiplier = 1.5
        elif row['procedure_code'] in medium_risk_codes:
            multiplier = 1.2
        else:
            multiplier = 1.0
        
        return round(base_score * multiplier, 2)
    
    joined_df['risk_score'] = joined_df.apply(calculate_risk_score, axis=1)
    
    return joined_df


@task(
    name="load_warehouse",
    retries=2,
    retry_delay_seconds=300,
    description="Loads transformed data to healthcare analytics warehouse"
)
def load_warehouse(transformed_df: pd.DataFrame) -> None:
    """
    Load transformed data to Postgres analytics warehouse.
    
    Args:
        transformed_df: Transformed DataFrame to load
    """
    # Simulate loading to warehouse
    # In production, this would use psycopg2, sqlalchemy, or similar
    # to insert into claims_fact and providers_dim tables
    
    print(f"Loading {len(transformed_df)} records to warehouse...")
    
    # Simulate loading to claims_fact table
    claims_fact = transformed_df[[
        'claim_id', 'provider_id', 'procedure_code',
        'claim_amount', 'claim_date', 'patient_id_hashed', 'risk_score'
    ]]
    print(f"Loading to claims_fact table: {len(claims_fact)} records")
    
    # Simulate loading to providers_dim table (deduplicated)
    providers_dim = transformed_df[[
        'provider_id', 'provider_name', 'specialty', 'location'
    ]].drop_duplicates()
    print(f"Loading to providers_dim table: {len(providers_dim)} records")
    
    print("Warehouse load completed successfully.")


@task(
    name="refresh_bi",
    retries=2,
    retry_delay_seconds=300,
    description="Triggers refresh of BI dashboard tools"
)
def refresh_bi() -> None:
    """
    Trigger refresh of Power BI and Tableau dashboards.
    """
    # Simulate BI refresh
    # In production, this would call:
    # - Power BI REST API: POST https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/refreshes
    # - Tableau API: POST /api/{api-version}/sites/{site-id}/tasks/extractRefreshes
    
    print("Triggering Power BI dataset refresh...")
    print("Power BI refresh initiated.")
    
    print("Triggering Tableau workbook refresh...")
    print("Tableau refresh initiated.")
    
    print("BI refresh completed successfully.")


@flow(
    name="healthcare-claims-etl",
    description="Daily healthcare claims processing ETL pipeline"
)
def healthcare_claims_etl(
    claims_csv_path: str = "data/claims.csv",
    providers_csv_path: str = "data/providers.csv"
) -> None:
    """
    Main ETL flow for healthcare claims processing.
    
    Orchestrates:
    1. Parallel extraction of claims and provider data
    2. Transformation with anonymization and risk scoring
    3. Parallel loading to warehouse and BI refresh
    
    Args:
        claims_csv_path: Path to claims CSV file
        providers_csv_path: Path to providers CSV file
    """
    # Extract Stage - parallel execution using .submit()
    claims_future = extract_claims.submit(claims_csv_path)
    providers_future = extract_providers.submit(providers_csv_path)
    
    # Transform Stage - sequential, depends on both extract tasks
    # Prefect automatically waits for futures to resolve
    transformed_df = transform_join(claims_future, providers_future)
    
    # Load Stage - parallel execution after transform
    warehouse_future = load_warehouse.submit(transformed_df)
    bi_future = refresh_bi.submit()
    
    # Wait for all load tasks to complete
    warehouse_future.wait()
    bi_future.wait()


if __name__ == '__main__':
    # Local execution for testing
    # For production deployment with daily schedule starting 2024-01-01:
    # prefect deployment build healthcare_claims_etl.py:healthcare_claims_etl \
    #   --name "daily-healthcare-claims" \
    #   --cron "0 2 * * *" \
    #   --start-date "2024-01-01T00:00:00" \
    #   --apply
    healthcare_claims_etl()