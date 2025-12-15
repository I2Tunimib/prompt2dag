from pathlib import Path
import random

import pandas as pd
from prefect import flow, task


@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """
    Simulate fetching holdings CSV for a brokerage account.

    Returns a DataFrame with columns: symbol, quantity, price.
    """
    symbols = ["AAPL", "GOOGL", "MSFT", "CASH"]
    rows = []
    for symbol in symbols:
        quantity = random.randint(10, 100)
        price = random.uniform(100, 500) if symbol != "CASH" else 1.0
        rows.append({"symbol": symbol, "quantity": quantity, "price": price})
    df = pd.DataFrame(rows)
    return df


@task
def analyze_portfolio(holdings: pd.DataFrame) -> dict:
    """
    Analyze a single portfolio.

    Returns a dictionary with total value, allocation percentages per symbol,
    and a simple risk score.
    """
    df = holdings.copy()
    df["value"] = df["quantity"] * df["price"]
    total_value = df["value"].sum()
    allocation = (df.set_index("symbol")["value"] / total_value).to_dict()
    risk_score = len(df) * random.random()
    return {
        "total_value": total_value,
        "allocation": allocation,
        "risk_score": risk_score,
    }


@task
def aggregate_results(analysis_list: list[dict]) -> list[dict]:
    """
    Aggregate analysis results from all accounts and determine rebalancing trades.

    Returns a list of trade dictionaries.
    """
    combined_total = sum(item["total_value"] for item in analysis_list)

    # Sum absolute values per symbol across accounts
    symbol_values: dict[str, float] = {}
    for item in analysis_list:
        for symbol, pct in item["allocation"].items():
            value = pct * item["total_value"]
            symbol_values[symbol] = symbol_values.get(symbol, 0.0) + value

    combined_allocation = {
        symbol: value / combined_total for symbol, value in symbol_values.items()
    }

    target_allocation = {"AAPL": 0.30, "GOOGL": 0.25, "MSFT": 0.20, "CASH": 0.25}
    trades: list[dict] = []

    for symbol, target_pct in target_allocation.items():
        current_pct = combined_allocation.get(symbol, 0.0)
        diff = current_pct - target_pct
        if abs(diff) > 0.02:  # threshold of 2%
            action = "SELL" if diff > 0 else "BUY"
            amount = round(abs(diff) * combined_total, 2)
            trades.append(
                {
                    "symbol": symbol,
                    "action": action,
                    "amount": amount,
                    "target_pct": target_pct,
                    "current_pct": round(current_pct, 4),
                }
            )
    return trades


@task
def generate_trade_orders_csv(trades: list[dict], output_path: str = "trade_orders.csv") -> str:
    """
    Write the list of trades to a CSV file.

    Returns the path to the generated CSV.
    """
    df = pd.DataFrame(trades)
    Path(output_path).write_text(df.to_csv(index=False))
    return output_path


@flow
def portfolio_rebalancing_flow() -> str:
    """
    Orchestrates the portfolio rebalancing pipeline:
    fetch → analyze → aggregate → generate CSV.
    """
    account_ids = [f"BROKERAGE_{i:03d}" for i in range(1, 6)]

    # Parallel fetch
    fetch_futures = [fetch_holdings.submit(account_id) for account_id in account_ids]
    holdings_list = [future.result() for future in fetch_futures]

    # Parallel analysis
    analyze_futures = [analyze_portfolio.submit(holdings) for holdings in holdings_list]
    analysis_results = [future.result() for future in analyze_futures]

    # Aggregation
    trades = aggregate_results(analysis_results)

    # CSV generation
    csv_path = generate_trade_orders_csv(trades)
    return csv_path


if __name__ == "__main__":
    # Daily execution schedule can be configured in a Prefect deployment.
    portfolio_rebalancing_flow()