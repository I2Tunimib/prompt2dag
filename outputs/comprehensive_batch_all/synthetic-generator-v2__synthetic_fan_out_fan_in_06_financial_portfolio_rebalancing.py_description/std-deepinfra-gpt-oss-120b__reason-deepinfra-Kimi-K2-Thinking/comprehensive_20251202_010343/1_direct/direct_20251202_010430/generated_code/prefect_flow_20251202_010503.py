from typing import List, Dict

import numpy as np
import pandas as pd
from prefect import flow, task

# Configuration constants
ACCOUNT_IDS = [
    "BROKERAGE_001",
    "BROKERAGE_002",
    "BROKERAGE_003",
    "BROKERAGE_004",
    "BROKERAGE_005",
]

TARGET_ALLOCATIONS: Dict[str, float] = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25,
}


@task
def fetch_holdings(account_id: str) -> pd.DataFrame:
    """Simulate fetching holdings CSV for a brokerage account."""
    symbols = list(TARGET_ALLOCATIONS.keys())
    rows = []
    for symbol in symbols:
        quantity = np.random.randint(10, 100)
        price = (
            np.random.uniform(100, 500) if symbol != "CASH" else 1.0
        )  # cash price = $1
        rows.append({"symbol": symbol, "quantity": quantity, "price": price})
    df = pd.DataFrame(rows)
    return df


@task
def analyze_portfolio(holdings: pd.DataFrame) -> dict:
    """Calculate portfolio metrics for a single account."""
    holdings = holdings.copy()
    holdings["value"] = holdings["quantity"] * holdings["price"]
    total_value = holdings["value"].sum()
    allocation = (
        holdings.set_index("symbol")["value"] / total_value
    ).to_dict()
    risk_score = len(holdings) * np.random.uniform(0.5, 1.5)
    return {
        "total_value": total_value,
        "allocation": allocation,
        "risk_score": risk_score,
        "holdings": holdings,
    }


@task
def aggregate_results(analysis_list: List[dict]) -> List[dict]:
    """Combine analysis from all accounts and compute rebalancing trades."""
    combined_holdings = pd.concat(
        [a["holdings"] for a in analysis_list], ignore_index=True
    )
    combined_holdings["value"] = (
        combined_holdings["quantity"] * combined_holdings["price"]
    )
    total_combined = combined_holdings["value"].sum()
    combined_allocation = (
        combined_holdings.groupby("symbol")["value"].sum()
        / total_combined
    ).to_dict()

    trades: List[dict] = []
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = combined_allocation.get(symbol, 0.0)
        diff_pct = target_pct - current_pct
        if abs(diff_pct) > 0.02:  # threshold of 2%
            desired_value = target_pct * total_combined
            current_value = current_pct * total_combined
            diff_value = desired_value - current_value
            avg_price = combined_holdings.loc[
                combined_holdings["symbol"] == symbol, "price"
            ].mean()
            if pd.isna(avg_price) or avg_price == 0:
                avg_price = 1.0
            quantity = diff_value / avg_price
            action = "BUY" if diff_value > 0 else "SELL"
            trades.append(
                {
                    "symbol": symbol,
                    "action": action,
                    "amount_usd": round(abs(diff_value), 2),
                    "quantity": round(abs(quantity), 2),
                    "target_allocation_pct": round(target_pct * 100, 2),
                    "current_allocation_pct": round(current_pct * 100, 2),
                    "diff_pct": round(diff_pct * 100, 2),
                }
            )
    return trades


@task
def generate_trade_orders_csv(
    trades: List[dict], output_path: str = "trade_orders.csv"
) -> str:
    """Write the list of trades to a CSV file."""
    if not trades:
        df = pd.DataFrame(
            columns=[
                "symbol",
                "action",
                "amount_usd",
                "quantity",
                "target_allocation_pct",
                "current_allocation_pct",
                "diff_pct",
            ]
        )
    else:
        df = pd.DataFrame(trades)
    df.to_csv(output_path, index=False)
    return output_path


@flow
def portfolio_rebalancing_flow() -> str:
    """Orchestrate the full portfolio rebalancing pipeline."""
    # Fan‑out: fetch holdings in parallel
    fetch_futures = {
        acct: fetch_holdings.submit(acct) for acct in ACCOUNT_IDS
    }
    fetched = {acct: fut.result() for acct, fut in fetch_futures.items()}

    # Fan‑out: analyze each portfolio in parallel
    analysis_futures = {
        acct: analyze_portfolio.submit(holdings)
        for acct, holdings in fetched.items()
    }
    analysis_results = [fut.result() for fut in analysis_futures.values()]

    # Fan‑in: aggregate results
    trades = aggregate_results(analysis_results)

    # Generate CSV file
    csv_path = generate_trade_orders_csv(trades)
    return csv_path


if __name__ == "__main__":
    # Local execution; deployment schedule can be configured separately.
    result_path = portfolio_rebalancing_flow()
    print(f"Trade orders written to: {result_path}")