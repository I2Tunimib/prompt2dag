from dagster import op, job, ScheduleDefinition, Definitions, DefaultScheduleStatus, RetryPolicy
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Minimal configuration stubs
# In production, use Dagster resources or environment variables
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "inventory_db",
    "user": "db_user",
    "password": "db_password",
    "table": "inventory_shipments",
}

EMAIL_CONFIG = {
    "smtp_server": "smtp.company.com",
    "smtp_port": 587,
    "sender": "pipeline@company.com",
    "recipient": "supply-chain-team@company.com",
    "subject_prefix": "[Supply Chain ETL]",
}


@op
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor A (1,250 records)."""
    logger.info("Ingesting data from Vendor A")
    
    # Simulate CSV ingestion