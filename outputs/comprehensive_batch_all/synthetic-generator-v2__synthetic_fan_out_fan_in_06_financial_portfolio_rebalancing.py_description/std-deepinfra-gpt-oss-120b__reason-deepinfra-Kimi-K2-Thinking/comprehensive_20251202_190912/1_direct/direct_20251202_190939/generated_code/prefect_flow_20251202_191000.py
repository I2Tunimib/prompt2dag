from pathlib import Path
import random
import csv
from typing import List, Dict

import pandas as pd
import numpy as np
from prefect import flow, task, get_run_logger


@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """Simulate fetching holdings CSV for a brokerage account.

    Returns a DataFrame with columns: symbol, quantity, price.
    """
    logger = get_run_logger()
    logger.info("Fetching holdings for %s", account_id)

    symbols = ["AAPL", "GOOGL", "MSFT", "CASH", "AMZN", "TSLA"]
    num_holdings = random.randint(5, 10)
    selected_symbols = random.sample(symbols, k=num_holdings)

    data = {
        "symbol": selected_symbols,
        "quantity": [random.randint(10, 200) for _ in range(num_holdings)],
        "price": [round(random.uniform(50, 1500), 2) for _ in range(num_holdings)],
    }
    df = pd.DataFrame(data)
    logger.debug("Fetched data for %s: %s", account_id, df.head())
    return df


@task
def analyze_portfolio(holdings: pd.DataFrame) -> Dict:
    """Analyze a single portfolio.

    Calculates total value, allocation percentages per symbol, and a simple risk score.
    """
    logger = get_run_logger()
    logger.info("Analyzing portfolio with %d holdings", len(holdings))

    holdings["value"] = holdings["quantity"] * holdings["price"]
    total_value = holdings["value"].sum()

    allocation = (
        holdings.groupby("symbol")["value"].sum() / total_value
    ).to_dict()

    risk_score = len(holdings["symbol"].unique()) * random.uniform(0.5, 1.5)

    result = {
        "total_value": total_value,
        "allocation": allocation,
        "risk_score": risk_score,
    }
    logger.debug("Analysis result: %s", result)
    return result


@task
def aggregate_results(analysis_results: List[Dict]) -> List[Dict]:
    """Aggregate analysis from all accounts and determine rebalancing trades.

    Returns a list of trade dictionaries.
    """
    logger = get_run_logger()
    logger.info("Aggregating results from %d accounts", len(analysis_results))

    combined_total = sum(r["total_value"] for r in analysis_results)

    # Sum values per symbol across all accounts
    symbol_values = {}
    for result in analysis_results:
        holdings_value = result["total_value"] * pd.Series(result["allocation"])
        for symbol, value in holdings_value.items():
            symbol_values[symbol] = symbol_values.get(symbol, 0) + value

    combined_allocation = {
        symbol: value / combined_total for symbol, value in symbol_values.items()
    }

    target_allocation = {
        "AAPL": 0.30,
        "GOOGL": 0.25,
        "MSFT": 0.20,
        "CASH": 0.25,
    }

    trades = []
    tolerance = 0.02  # 2%

    for symbol, target_pct in target_allocation.items():
        current_pct = combined_allocation.get(symbol, 0.0)
        diff = target_pct - current_pct
        if abs(diff) > tolerance:
            action = "BUY" if diff > 0 else "SELL"
            amount = abs(diff) * combined_total
            trade = {
                "symbol": symbol,
                "action": action,
                "amount": round(amount, 2),
                "target_pct": round(target_pct * 100, 2),
                "current_pct": round(current_pct * 100, 2),
            }
            trades.append(trade)
            logger.info(
                "Trade identified: %s %s of $%s (target %.2f%%, current %.2f%%)",
                action,
                symbol,
                trade["amount"],
                trade["target_pct"],
                trade["current_pct"],
            )

    logger.info("Total trades identified: %d", len(trades))
    return trades


@task
def generate_trade_orders(trades: List[Dict]) -> Path:
    """Generate a CSV file containing all rebalancing trade orders."""
    logger = get_run_logger()
    output_path = Path("trade_orders.csv")
    logger.info("Writing %d trades to %s", len(trades), output_path)

    fieldnames = ["symbol", "action", "amount", "target_pct", "current_pct"]
    with output_path.open(mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)

    logger.debug("Trade orders CSV written successfully")
    return output_path


@flow
def portfolio_rebalancing_flow():
    """Orchestrates the portfolio rebalancing pipeline."""
    logger = get_run_logger()
    logger.info("Starting portfolio rebalancing flow")

    account_ids = [
        "BROKERAGE_001",
        "BROKERAGE_002",
        "BROKERAGE_003",
        "BROKERAGE_004",
        "BROKERAGE_005",
    ]

    # Fan‑out: fetch holdings in parallel
    fetch_futures = {
        acct: fetch_holdings.submit(acct) for acct in account_ids
    }

    # Fan‑out: analyze each portfolio in parallel
    analysis_futures = {
        acct: analyze_portfolio.submit(fut) for acct, fut in fetch_futures.items()
    }

    # Fan‑in: collect analysis results
    analysis_results = [fut.result() for fut in analysis_futures.values()]

    # Aggregate and generate trades
    trades = aggregate_results(analysis_results)
    trade_file = generate_trade_orders(trades)

    logger.info("Portfolio rebalancing flow completed. Trade file: %s", trade_file)


# Note: In a production deployment, this flow would be scheduled to run daily via
# Prefect deployment configuration.

if __name__ == "__main__":
    portfolio_rebalancing_flow()