import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine
import hashlib

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def extract_claims(claims_path: str) -> pd.DataFrame:
    """
    Extracts patient claims data from a CSV file.
    """
    logger = get_run_logger()
    logger.info(f"Extracting claims data from {claims_path}")
    
    try:
        df = pd.read_csv(claims_path)
        logger.info(f"Successfully extracted {len(df)} claims records")
        return df
    except Exception as e:
        logger.error(f"Failed to extract claims: {e}")
        raise

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def extract_providers(providers_path: str) -> pd.DataFrame:
    """
    Extracts provider data from a CSV file.
    """
    logger = get_run_logger()
    logger.info(f"Extracting providers data from {providers_path}")
    
    try:
        df = pd.read_csv(providers_path)
        logger.info(f"Successfully extracted {len(df)} provider records")
        return df
    except Exception as e:
        logger.error(f"Failed to extract providers: {e}")
        raise

@task(retries=2, retry_delay_seconds=300)
def transform_join(claims_df: pd.DataFrame, providers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Joins claims and provider data, anonymizes PII, and calculates risk scores.
    """
    logger = get_run_logger()
    logger.info("Transforming and joining claims and providers data")
    
    try:
        # Validate required columns
        required_claims_cols = ['provider_id', 'patient_id', 'claim_amount', 'procedure_code']
        required_providers_cols = ['id', 'provider_name', 'specialty']
        
        for col in required_claims_cols:
            if col not in claims_df.columns:
                raise ValueError(f"Missing required column in claims data: {col}")
        
        for col in required_providers_cols:
            if col not in providers_df.columns:
                raise ValueError(f"Missing required column in providers data: {col}")
        
        # Join claims and providers data
        joined_df = claims_df.merge(
            providers_df,
            left_on='provider_id',
            right_on='id',
            how='inner',
            suffixes=('_claim', '_provider')
        )
        
        logger.info(f"Joined data shape: {joined_df.shape}")
        
        # Anonymize PII by hashing patient identifiers
        if 'patient_id' in joined_df.columns:
            logger.info("Anonymizing patient identifiers")
            joined_df['patient_id_hash'] = joined_df['patient_id'].astype(str).apply(
                lambda x: hashlib.sha256(x.encode()).hexdigest()[:16]
            )
            joined_df.drop(columns=['patient_id'], inplace=True)
        
        # Calculate risk scores based on procedure codes and amounts
        if 'procedure_code' in joined_df.columns and 'claim_amount' in joined_df.columns:
            logger.info("Calculating risk scores")
            
            # Normalize claim amounts (0-70 points)
            amount_score = (joined_df['claim_amount'].fillna(0) / 1000).clip(0, 70)
            
            # Extract numeric component from procedure code (0-30 points)
            proc_numeric = joined_df['procedure_code'].str.extract('(\d+)').astype(float).fillna(0)
            procedure_score = (proc_numeric % 100 * 0.3).clip(0, 30)
            
            joined_df['risk_score'] = amount_score + procedure_score
            
        logger.info(f"Transformation complete. Processed {len(joined_df)} records")
        return joined_df
    
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

@task(retries=2, retry_delay_seconds=300)
def load_warehouse(transformed_df: pd.DataFrame, db_connection_string: str):
    """
    Loads transformed data to healthcare analytics warehouse.
    """
    logger = get_run_logger()
    logger.info("Loading data to analytics warehouse")
    
    try:
        engine = create_engine(db_connection_string)
        
        # Load to claims_fact table
        claims_fact_cols = ['claim_id', 'patient_id_hash', 'provider_id', 
                           'procedure_code', 'claim_amount', 'risk_score']
        
        # Add claim_date if it exists
        if 'claim_date' in transformed_df.columns:
            claims_fact_cols.append('claim_date')
        
        # Ensure we have the columns we need
        available_cols = [col for col in claims_fact_cols if col in transformed_df.columns]
        if available_cols:
            claims_fact_df = transformed_df[available_cols].copy()
            claims_fact_df.to_sql(
                'claims_fact',
                engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            logger.info(f"Loaded {len(claims_fact_df)} records to claims_fact")
        
        # Load to providers_dim table
        providers_dim_cols = ['provider_id', 'provider_name', 'specialty', 'facility_name']
        available_provider_cols = [col for col in providers_dim_cols if col in transformed_df.columns]
        
        if available_provider_cols:
            providers_dim_df = transformed_df[available_provider_cols].drop_duplicates()
            providers_dim_df.to_sql(
                'providers_dim',
                engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Loaded {len(providers_dim_df)} records to providers_dim")
        
        logger.info("Warehouse load complete")
    
    except Exception as e:
        logger.error(f"Warehouse load failed: {e}")
        raise

@task(retries=2, retry_delay_seconds=300)
def refresh_bi():
    """
    Triggers refresh of BI dashboard tools including Power BI and Tableau.
    """
    logger = get_run_logger()
    logger.info("Triggering BI dashboard refresh")
    
    try:
        # Simulate Power BI refresh
        # In production, use Power BI REST API
        logger.info("Power BI refresh triggered via API (simulated)")
        
        # Simulate Tableau refresh
        # In production, use Tableau Server Client or REST API
        logger.info("Tableau refresh triggered via API (simulated)")