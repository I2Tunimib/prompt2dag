from dagster import op, job, In, Out, OpExecutionContext
import pandas as pd
from typing import List, Dict, Any
import os
from datetime import datetime


# Type aliases for clarity
HoldingsDataFrame = pd.DataFrame
AnalysisResult = Dict[str, Any]
TradeOrder = Dict[str, Any]


@op(out=Out(HoldingsDataFrame))
def fetch_brokerage_001() -> HoldingsDataFrame:
    """Simulate fetching holdings from brokerage account 001."""
    data = {
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [100, 50, 75, 10000],
        'price': [150.0, 2800.0, 300.0, 1.0]
    }
    return pd.DataFrame(data)


@op(out=Out(HoldingsDataFrame))
def fetch_brokerage_002() -> HoldingsDataFrame:
    """Simulate fetching holdings from brokerage account 002."""
    data = {
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [80, 60, 90, 8000],
        'price': [150.0, 2800.0, 300.0, 1.0]
    }
    return pd.DataFrame(data)


@op(out=Out(HoldingsDataFrame))
def fetch_brokerage_003() -> HoldingsDataFrame:
    """Simulate fetching holdings from brokerage account 003."""
    data = {
        'symbol': ['AAPL', 'GOOGL', 'MSFT', 'CASH'],
        'quantity': [120, 40, 60, 12000],
        'price': [150.0,