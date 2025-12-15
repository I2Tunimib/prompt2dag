from prefect import flow, task
import csv
import io
from typing import List, Dict, Any
import random

# Configuration
TARGET_ALLOCATIONS = {
    "AAPL": 30.0,
    "GOOGL": 25.0,
    "MSFT": 20.0,
    "CASH": 25.0
}
REBALANCING_THRESHOLD = 2.0


@task
def fetch_holdings(account_id: str) -> str:
    """Fetch holdings CSV data for a brokerage account."""
    random.seed(hash(account_id) % 1000)
    
    holdings = [
        ["symbol", "quantity", "price"],
        ["AAPL", str(random.randint(80, 120)), str(round(random.uniform(170, 190), 2))],
        ["GOOGL", str(random.randint(15, 35)), str(round(random.uniform(120, 140), 2))],
        ["MSFT", str(random.randint(30, 60)), str(round(random.uniform(250, 290), 2))],
        ["CASH", str(random.randint(5000, 15000)), "1.00"]
    ]
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(holdings)
    return output.getvalue()


@task
def analyze_portfolio(holdings_csv: str, account_id: str) -> Dict[str, Any]:
    """Analyze a single portfolio and compute metrics."""
    reader = csv.DictReader(io.StringIO(holdings_csv))
    holdings = list(reader)
    
    total_value = sum(float(h["quantity"]) * float(h["price"]) for h in holdings)
    
    analysis = {
        "account_id": account_id,
        "total_value": total_value,
        "holdings": []
    }
    
    for h in holdings:
        value = float(h["quantity"]) * float(h["price"])
        allocation_pct = (value / total_value) * 100
        analysis["holdings"].append({
            "symbol": h["symbol"],
            "quantity": float(h["quantity"]),
            "price": float(h["price"]),
            "value": value,
            "allocation_pct": allocation_pct
        })
    
    analysis["risk_score"] = min(100, max(0, 100 - (len(holdings) - 4) * 20))
    
    return analysis


@task
def aggregate_analyses(analyses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate portfolio analyses and determine rebalancing trades."""
    total_combined_value = sum(a["total_value"] for a in analyses)
    
    combined_holdings = {}
    for analysis in analyses:
        for holding in analysis["holdings"]:
            symbol = holding["symbol"]
            combined_holdings[symbol] = combined_holdings.get(symbol, 0) + holding["value"]
    
    current_allocations = {
        symbol: (value / total_combined_value) * 100
        for symbol, value in combined_holdings.items()
    }
    
    trades = []
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = current_allocations.get(symbol, 0)
        diff = current_pct - target_pct
        
        if abs(diff) > REBALANCING_THRESHOLD:
            trade_amount = (diff / 100) * total_combined_value
            
            trades.append({
                "symbol": symbol,
                "action": "SELL" if diff > 0 else "BUY",
                "amount": abs(round(trade_amount, 2)),
                "current_allocation_pct": round(current_pct, 2),
                "target_allocation_pct": target_pct,
                "difference_pct": round(diff, 2)
            })
    
    return {
        "total_combined_value": round(total_combined_value, 2),
        "current_allocations": {k: round(v, 2) for k, v in current_allocations.items()},
        "target_allocations": TARGET_ALLOCATIONS,
        "rebalancing_trades": trades
    }


@task
def generate_trade_orders(rebalancing_decisions: Dict[str, Any]) -> str:
    """Generate trade orders CSV from rebalancing decisions."""
    trades = rebalancing_decisions.get("rebalancing_trades", [])
    
    if not trades:
        return "No rebalancing trades needed."
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "symbol", "action", "amount", "current_allocation_pct",
        "target_allocation_pct", "difference_pct"
    ])
    
    for trade in trades:
        writer.writerow([
            trade["symbol"],
            trade["action"],
            trade["amount"],
            trade["current_allocation_pct"],
            trade["target_allocation_pct"],
            trade["difference_pct"]
        ])
    
    return output.getvalue()


@flow
def portfolio_rebalancing_flow() -> str:
    """
    Main flow for daily portfolio rebalancing across multiple brokerage accounts.
    Implements fan-out/fan-in pattern with maximum parallel width of 5.
    """
    account_ids = [f"BROKERAGE_{i:03d}" for i in range(1, 6)]
    
    # Fan-out: Fetch holdings from all accounts in parallel
    fetch_futures = [fetch_holdings.submit(account_id) for account_id in account_ids]
    
    # Fan-out: Analyze each portfolio in parallel
    analyze_futures = [
        analyze_portfolio.submit(fetch_future, account_id)
        for fetch_future, account_id in zip(fetch_futures, account_ids)
    ]
    
    # Fan-in: Aggregate all analyses
    rebalancing_decisions = aggregate_analyses(analyze_futures)
    
    # Generate final trade orders
    trade_orders_csv = generate_trade_orders(rebalancing_decisions)
    
    return trade_orders_csv


if __name__ == "__main__":
    # Local execution
    result = portfolio_rebalancing_flow()
    print(result)
    
    # For scheduled deployment, run:
    # prefect deployment build portfolio_rebalancing_flow.py:portfolio_rebalancing_flow --name daily-rebalancing --cron "0 9 * * *"
    # prefect deployment apply daily-rebalancing-deployment.yaml