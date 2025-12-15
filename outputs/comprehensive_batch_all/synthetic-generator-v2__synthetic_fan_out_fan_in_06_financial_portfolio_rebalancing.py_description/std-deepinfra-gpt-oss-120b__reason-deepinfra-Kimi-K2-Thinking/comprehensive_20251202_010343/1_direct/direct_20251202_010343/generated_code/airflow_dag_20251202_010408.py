from datetime import datetime
import os
import random

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
}

# Target allocation configuration used in aggregation
TARGET_ALLOCATIONS = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25,
}


def _simulate_holdings() -> pd.DataFrame:
    """Generate a mock holdings DataFrame."""
    symbols = ["AAPL", "GOOGL", "MSFT", "CASH"]
    data = {
        "symbol": symbols,
        "quantity": [random.randint(10, 100) for _ in symbols],
        "price": [random.uniform(100, 500) for _ in symbols],
    }
    return pd.DataFrame(data)


@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """
    Simulate fetching holdings CSV for a brokerage account.

    :param account_id: Identifier of the brokerage account.
    :return: DataFrame with columns symbol, quantity, price.
    """
    # In a real implementation this would call an external API or read a file.
    # Here we generate mock data.
    df = _simulate_holdings()
    df["account_id"] = account_id
    return df


@task
def analyze_portfolio(holdings: pd.DataFrame) -> dict:
    """
    Analyze a single portfolio.

    Calculates total value, allocation percentages per symbol,
    and a simple risk score based on the number of distinct holdings.

    :param holdings: DataFrame with holdings for one account.
    :return: Dictionary with analysis results.
    """
    holdings["value"] = holdings["quantity"] * holdings["price"]
    total_value = holdings["value"].sum()
    allocation = (
        holdings.groupby("symbol")["value"].sum() / total_value
    ).to_dict()
    risk_score = len(holdings["symbol"].unique()) * 10  # simplistic risk metric

    return {
        "account_id": holdings["account_id"].iloc[0],
        "total_value": total_value,
        "allocation": allocation,
        "risk_score": risk_score,
    }


@task
def aggregate_results(analysis_results: list) -> dict:
    """
    Aggregate analysis from all accounts and compute rebalancing trades.

    :param analysis_results: List of dictionaries from analyze_portfolio.
    :return: Dictionary containing combined metrics and trade list.
    """
    combined_total = sum(item["total_value"] for item in analysis_results)

    # Combine allocations weighted by account total value
    combined_allocation = {}
    for item in analysis_results:
        weight = item["total_value"] / combined_total
        for symbol, pct in item["allocation"].items():
            combined_allocation[symbol] = combined_allocation.get(symbol, 0) + pct * weight

    # Determine trades needed
    trades = []
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = combined_allocation.get(symbol, 0.0)
        diff = current_pct - target_pct
        if abs(diff) > 0.02:  # threshold of 2%
            action = "SELL" if diff > 0 else "BUY"
            # Amount in monetary terms needed to reach target allocation
            amount = abs(diff) * combined_total
            trades.append(
                {
                    "action": action,
                    "symbol": symbol,
                    "amount": round(amount, 2),
                    "target_pct": round(target_pct * 100, 2),
                    "current_pct": round(current_pct * 100, 2),
                }
            )

    return {
        "combined_total_value": combined_total,
        "combined_allocation": combined_allocation,
        "trades": trades,
    }


@task
def generate_trade_orders(aggregation: dict) -> str:
    """
    Write the trade orders to a CSV file.

    :param aggregation: Output from aggregate_results containing trades.
    :return: Path to the generated CSV file.
    """
    trades = aggregation["trades"]
    if not trades:
        return "No trades required for this run."

    df = pd.DataFrame(trades)
    execution_date = datetime.utcnow().strftime("%Y%m%d")
    output_dir = os.getenv("AIRFLOW_HOME", "/tmp")
    file_path = os.path.join(output_dir, f"trade_orders_{execution_date}.csv")
    df.to_csv(file_path, index=False)
    return file_path


with DAG(
    dag_id="portfolio_rebalancing",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["finance", "rebalancing"],
) as dag:
    # Fetch tasks for each brokerage account
    fetch_001 = fetch_holdings("BROKERAGE_001")
    fetch_002 = fetch_holdings("BROKERAGE_002")
    fetch_003 = fetch_holdings("BROKERAGE_003")
    fetch_004 = fetch_holdings("BROKERAGE_004")
    fetch_005 = fetch_holdings("BROKERAGE_005")

    # Analysis tasks corresponding to each fetch
    analyze_001 = analyze_portfolio(fetch_001)
    analyze_002 = analyze_portfolio(fetch_002)
    analyze_003 = analyze_portfolio(fetch_003)
    analyze_004 = analyze_portfolio(fetch_004)
    analyze_005 = analyze_portfolio(fetch_005)

    # Aggregation and trade generation
    aggregated = aggregate_results(
        [
            analyze_001,
            analyze_002,
            analyze_003,
            analyze_004,
            analyze_005,
        ]
    )
    trade_file = generate_trade_orders(aggregated)

    # Set explicit dependencies
    fetch_001 >> analyze_001
    fetch_002 >> analyze_002
    fetch_003 >> analyze_003
    fetch_004 >> analyze_004
    fetch_005 >> analyze_005

    [
        analyze_001,
        analyze_002,
        analyze_003,
        analyze_004,
        analyze_005,
    ] >> aggregated >> trade_file