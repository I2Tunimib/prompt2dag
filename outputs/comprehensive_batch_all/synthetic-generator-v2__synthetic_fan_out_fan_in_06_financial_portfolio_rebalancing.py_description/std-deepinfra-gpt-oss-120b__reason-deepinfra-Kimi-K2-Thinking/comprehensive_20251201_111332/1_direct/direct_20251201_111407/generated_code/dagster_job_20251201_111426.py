import csv
import os
import tempfile
from typing import Any, Dict, List

from dagster import In, Out, collect, job, op, schedule


ACCOUNT_IDS = [
    "BROKERAGE_001",
    "BROKERAGE_002",
    "BROKERAGE_003",
    "BROKERAGE_004",
    "BROKERAGE_005",
]

# Mock price data for symbols
PRICE_MAP = {
    "AAPL": 150.0,
    "GOOGL": 2800.0,
    "MSFT": 300.0,
    "CASH": 1.0,
}


@op(ins={"account_id": In(str)}, out=Out(Dict))
def fetch_holdings(context, account_id: str) -> Dict[str, Any]:
    """Simulate fetching holdings CSV data for a brokerage account.

    Returns a dictionary with the account identifier and a list of holdings.
    Each holding is a dict with ``symbol``, ``quantity`` and ``price``.
    """
    # In a real implementation this would call an external API.
    # Here we generate deterministic mock data based on the account id.
    base_quantity = int(account_id.split("_")[-1])  # 1‑5
    holdings = [
        {"symbol": "AAPL", "quantity": 10 * base_quantity, "price": PRICE_MAP["AAPL"]},
        {"symbol": "GOOGL", "quantity": 2 * base_quantity, "price": PRICE_MAP["GOOGL"]},
        {"symbol": "MSFT", "quantity": 5 * base_quantity, "price": PRICE_MAP["MSFT"]},
        {"symbol": "CASH", "quantity": 1000 * base_quantity, "price": PRICE_MAP["CASH"]},
    ]
    context.log.info(f"Fetched {len(holdings)} holdings for {account_id}")
    return {"account_id": account_id, "holdings": holdings}


@op(ins={"fetch_result": In(Dict)}, out=Out(Dict))
def analyze_portfolio(context, fetch_result: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a single portfolio.

    Calculates total value, allocation percentages per symbol, and a simple risk score.
    """
    account_id = fetch_result["account_id"]
    holdings = fetch_result["holdings"]

    total_value = sum(h["quantity"] * h["price"] for h in holdings)
    allocation = {
        h["symbol"]: (h["quantity"] * h["price"]) / total_value for h in holdings
    }
    risk_score = len(holdings)  # simplistic risk metric

    context.log.info(f"Analyzed {account_id}: total_value={total_value:.2f}")
    return {
        "account_id": account_id,
        "total_value": total_value,
        "allocation": allocation,
        "risk_score": risk_score,
    }


@op(
    ins={"analysis_results": In(List[Dict])},
    out=Out(Dict),
)
def aggregate_results(context, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate analysis from all accounts and compute rebalancing trades.

    Returns a dictionary containing combined totals, current allocations,
    target allocations, and a list of trade actions needed.
    """
    # Combine values per symbol across all accounts
    combined_values: Dict[str, float] = {}
    combined_total = 0.0
    for result in analysis_results:
        holdings = result["allocation"]
        for symbol, pct in holdings.items():
            value = pct * result["total_value"]
            combined_values[symbol] = combined_values.get(symbol, 0.0) + value
            combined_total += value

    # Compute current combined allocation percentages
    current_allocation = {
        symbol: value / combined_total for symbol, value in combined_values.items()
    }

    target_allocation = {
        "AAPL": 0.30,
        "GOOGL": 0.25,
        "MSFT": 0.20,
        "CASH": 0.25,
    }

    # Identify trades where allocation difference exceeds 2%
    trades: List[Dict[str, Any]] = []
    tolerance = 0.02
    for symbol, target_pct in target_allocation.items():
        current_pct = current_allocation.get(symbol, 0.0)
        diff = target_pct - current_pct
        if abs(diff) > tolerance:
            action = "BUY" if diff > 0 else "SELL"
            trade_value = diff * combined_total
            price = PRICE_MAP[symbol]
            quantity = abs(trade_value) / price
            trades.append(
                {
                    "action": action,
                    "symbol": symbol,
                    "quantity": round(quantity, 2),
                    "value": round(abs(trade_value), 2),
                    "target_pct": target_pct,
                    "current_pct": round(current_pct, 4),
                }
            )
            context.log.info(
                f"Trade identified: {action} {quantity} {symbol} "
                f"to move from {current_pct:.2%} to {target_pct:.2%}"
            )

    return {
        "combined_total_value": combined_total,
        "current_allocation": current_allocation,
        "target_allocation": target_allocation,
        "trades": trades,
    }


@op(ins={"aggregation": In(Dict)}, out=Out(str))
def generate_trade_orders(context, aggregation: Dict[str, Any]) -> str:
    """Generate a CSV file containing all rebalancing trade orders.

    Returns the path to the generated CSV file.
    """
    trades = aggregation["trades"]
    if not trades:
        context.log.info("No rebalancing trades required.")
        return ""

    # Create a temporary CSV file
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="trade_orders_")
    os.close(fd)  # Close the file descriptor; we'll open it via csv module

    fieldnames = [
        "action",
        "symbol",
        "quantity",
        "value",
        "target_pct",
        "current_pct",
    ]

    with open(path, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)

    context.log.info(f"Trade orders CSV generated at {path}")
    return path


@job
def portfolio_rebalance_job():
    """Dagster job that orchestrates the portfolio rebalancing pipeline."""
    # Fan‑out: fetch holdings for each account in parallel
    fetch_outputs = [
        fetch_holdings(account_id=aid) for aid in ACCOUNT_IDS
    ]

    # Parallel analysis of each fetched portfolio
    analysis_outputs = [analyze_portfolio(f) for f in fetch_outputs]

    # Fan‑in aggregation of all analysis results
    aggregated = aggregate_results(analysis_results=collect(*analysis_outputs))

    # Generate the final trade orders CSV
    generate_trade_orders(aggregated)


@schedule(
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    job=portfolio_rebalance_job,
    execution_timezone="UTC",
)
def daily_portfolio_rebalance_schedule():
    """Schedule that triggers the portfolio rebalancing job once per day."""
    return {}


if __name__ == "__main__":
    result = portfolio_rebalance_job.execute_in_process()
    if result.success:
        print("Portfolio rebalancing job completed successfully.")
    else:
        print("Portfolio rebalancing job failed.")