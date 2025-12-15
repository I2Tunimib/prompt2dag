import hashlib
from typing import Any, Dict

import pandas as pd
from dagster import (
    RetryPolicy,
    job,
    op,
    OpExecutionContext,
    ScheduleDefinition,
    Definitions,
    ResourceDefinition,
    Config,
    OpDefinition,
)

# --- Resources (minimal stubs) ---
# In production, these would be more robust with connection pooling, etc.

class PostgresWarehouseResource:
    """Stub resource for connecting to Postgres analytics warehouse."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "public"):
        """Load a DataFrame to a Postgres table."""
        # In production, use SQLAlchemy or psycopg2 with proper error handling
        # For this example, we'll just log the action
        print(f"Loading {len(df)} rows to {schema}.{table_name}")
        # Example: df.to_sql(table_name, self.connection_string, schema=schema, if_exists='append', index=False)

class BIToolsResource:
    """Stub resource for triggering BI tool refreshes."""
    
    def __init__(self, power_bi_url: str, tableau_url: str):
        self.power_bi_url = power_bi_url
        self.tableau_url = tableau_url
    
    def refresh_power_bi(self):
        """Trigger Power BI dataset refresh."""
        print(f"Triggering Power BI refresh at {self.power_bi_url}")
        # In production: requests.post(self.power_bi_url, headers=..., json=...)
    
    def refresh_tableau(self):
        """Trigger Tableau workbook refresh."""
        print(f"Triggering Tableau refresh at {self.tableau_url}")
        # In production: Use Tableau Server Client library

# --- Configuration ---
class ClaimsConfig(Config):
    claims_csv_path: str = "data/claims.csv"
    providers_csv_path: str = "data/providers.csv"

# --- Ops ---

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    description="Extracts patient claims data from CSV file"
)
def extract_claims(context: OpExecutionContext, config: ClaimsConfig) -> pd.DataFrame:
    """Extract claims data from CSV."""
    context.log.info(f"Reading claims from {config.claims_csv_path}")
    
    # Read CSV with basic error handling
    try:
        df = pd.read_csv(config.claims_csv_path)
        context.log.info(f"Successfully extracted {len(df)} claims records")
        return df
    except Exception as e:
        context.log.error(f"Failed to extract claims: {e}")
        raise

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Extracts provider data from CSV file"
)
def extract_providers(context: OpExecutionContext, config: ClaimsConfig) -> pd.DataFrame:
    """Extract provider data from CSV."""
    context.log.info(f"Reading providers from {config.providers_csv_path}")
    
    try:
        df = pd.read_csv(config.providers_csv_path)
        context.log.info(f"Successfully extracted {len(df)} provider records")
        return df
    except Exception as e:
        context.log.error(f"Failed to extract providers: {e}")
        raise

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Joins claims and provider data, anonymizes PII, and calculates risk scores"
)
def transform_join(
    context: OpExecutionContext, 
    claims_df: pd.DataFrame, 
    providers_df: pd.DataFrame
) -> pd.DataFrame:
    """Transform and join data with anonymization and risk scoring."""
    context.log.info("Starting transformation")
    
    # Validate input data
    if claims_df.empty or providers_df.empty:
        raise ValueError("Input DataFrames cannot be empty")
    
    # Join claims with providers on provider_id
    # Assuming both DataFrames have 'provider_id' column
    transformed_df = claims_df.merge(providers_df, on="provider_id", how="left")
    context.log.info(f"Joined data: {len(transformed_df)} records")
    
    # Anonymize PII: hash patient_id
    if "patient_id" in transformed_df.columns:
        transformed_df["patient_id_hashed"] = transformed_df["patient_id"].apply(
            lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16]
        )
        # Drop original patient_id for privacy
        transformed_df = transformed_df.drop(columns=["patient_id"])
        context.log.info("Anonymized patient identifiers")
    
    # Calculate risk score based on procedure codes and amounts
    # Simplified: risk = amount / 1000 + procedure_code_factor
    def calculate_risk_score(row):
        amount = row.get("claim_amount", 0)
        procedure_code = str(row.get("procedure_code", "0"))
        
        # Simple factor based on procedure code prefix
        procedure_factor = 0
        if procedure_code.startswith("9"):
            procedure_factor = 2.5
        elif procedure_code.startswith("8"):
            procedure_factor = 2.0
        elif procedure_code.startswith("7"):
            procedure_factor = 1.5
        else:
            procedure_factor = 1.0
        
        return (amount / 1000.0) + procedure_factor
    
    transformed_df["risk_score"] = transformed_df.apply(calculate_risk_score, axis=1)
    context.log.info("Calculated risk scores")
    
    return transformed_df

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Loads transformed data to healthcare analytics warehouse tables"
)
def load_warehouse(
    context: OpExecutionContext, 
    transformed_df: pd.DataFrame,
    warehouse: PostgresWarehouseResource
) -> None:
    """Load transformed data to Postgres warehouse."""
    context.log.info("Loading data to warehouse")
    
    if transformed_df.empty:
        context.log.warning("No data to load")
        return
    
    # Split into fact and dimension tables as per description
    # claims_fact table
    claims_fact_cols = ["claim_id", "provider_id", "patient_id_hashed", "claim_date", 
                        "procedure_code", "claim_amount", "risk_score"]
    claims_fact_df = transformed_df[claims_fact_cols].drop_duplicates()
    
    # providers_dim table
    providers_dim_cols = ["provider_id", "provider_name", "provider_specialty", "provider_location"]
    providers_dim_df = transformed_df[providers_dim_cols].drop_duplicates()
    
    # Load to warehouse
    warehouse.load_dataframe(claims_fact_df, "claims_fact")
    warehouse.load_dataframe(providers_dim_df, "providers_dim")
    
    context.log.info(f"Loaded {len(claims_fact_df)} claims and {len(providers_dim_df)} providers to warehouse")

@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Triggers refresh of BI dashboard tools including Power BI and Tableau"
)
def refresh_bi(context: OpExecutionContext, bi_tools: BIToolsResource) -> None:
    """Trigger BI tool refreshes."""
    context.log.info("Triggering BI tool refreshes")
    
    try:
        bi_tools.refresh_power_bi()
        bi_tools.refresh_tableau()
        context.log.info("BI refresh triggered successfully")
    except Exception as e:
        context.log.error(f"BI refresh failed: {e}")
        # Don't fail the pipeline if BI refresh fails, just log it
        # In production, you might want to raise or have specific error handling

# --- Job Definition ---

@job(
    description="Healthcare claims processing ETL pipeline with staged extraction, transformation, and loading",
    resource_defs={
        "warehouse": ResourceDefinition.hardcoded_resource(
            PostgresWarehouseResource("postgresql://user:pass@localhost:5432/warehouse")
        ),
        "bi_tools": ResourceDefinition.hardcoded_resource(
            BIToolsResource(
                power_bi_url="https://api.powerbi.com/v1.0/.../refreshes",
                tableau_url="https://tableau-server/api/.../refresh"
            )
        )
    }
)
def healthcare_claims_etl():
    """Healthcare claims ETL job with parallel extract and load stages."""
    
    # Extract stage - runs in parallel
    claims_data = extract_claims()
    providers_data = extract_providers()
    
    # Transform stage - depends on both extract ops
    transformed_data = transform_join(claims_data, providers_data)
    
    # Load stage - runs in parallel after transform
    load_warehouse(transformed_data)
    refresh_bi()

# --- Schedule ---
# Daily schedule starting January 1, 2024
healthcare_claims_schedule = ScheduleDefinition(
    job=healthcare_claims_etl,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    execution_timezone="UTC",
)

# --- Definitions (for Dagster deployment) ---
defs = Definitions(
    jobs=[healthcare_claims_etl],
    schedules=[healthcare_claims_schedule],
)

# --- Launch Pattern ---
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = healthcare_claims_etl.execute_in_process(
        run_config={
            "ops": {
                "extract_claims": {
                    "config": {
                        "claims_csv_path": "data/claims.csv"
                    }
                },
                "extract_providers": {
                    "config": {
                        "providers_csv_path": "data/providers.csv"
                    }
                }
            },
            "resources": {
                "warehouse": {
                    "config": {
                        "connection_string": "postgresql://user:pass@localhost:5432/warehouse"
                    }
                },
                "bi_tools": {
                    "config": {
                        "power_bi_url": "https://api.powerbi.com/v1.0/.../refreshes",
                        "tableau_url": "https://tableau-server/api/.../refresh"
                    }
                }
            }
        }
    )
    
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")