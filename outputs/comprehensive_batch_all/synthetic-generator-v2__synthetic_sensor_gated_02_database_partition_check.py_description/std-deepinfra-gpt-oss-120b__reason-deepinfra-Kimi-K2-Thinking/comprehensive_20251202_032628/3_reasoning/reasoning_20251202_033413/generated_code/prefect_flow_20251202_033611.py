import time
import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import argparse
import re
from dateutil import parser

from prefect import flow, task

# Database configuration - use environment variables or Prefect Blocks in production
DB_PATH = os.getenv("DB_PATH", "./orders.db")
WAREHOUSE_PATH = os.getenv("WAREHOUSE_PATH", "./warehouse.db")


def get_database_connection(conn_id: str):
    """Get database connection based on connection ID."""
    if conn_id == "database_conn":
        return sqlite3.connect(DB_PATH)
    elif conn_id == "warehouse_conn":
        return sqlite3.connect(WAREHOUSE_PATH)
    else: