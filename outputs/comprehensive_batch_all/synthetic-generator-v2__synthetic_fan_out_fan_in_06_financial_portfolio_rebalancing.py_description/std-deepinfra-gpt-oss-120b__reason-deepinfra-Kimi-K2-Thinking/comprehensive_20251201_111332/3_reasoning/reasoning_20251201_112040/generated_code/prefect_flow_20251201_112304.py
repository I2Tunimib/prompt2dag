import csv
from typing import List, Dict, Any
from prefect import flow, task

# Target allocation configuration
TARGET_ALLOCATIONS = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25
}

REBALANCING_THRESHOLD = 0.02  # 2% difference threshold


@task
def fetch_holdings(brokerage_id: str) -> List[Dict[str, Any]]:
    """Fetch holdings data for a brokerage account."""
    mock_holdings = {
        "BROKERAGE_001": [
            {"symbol": "AAPL", "quantity": 100, "price": 150.0},
            {"symbol": "GOOGL", "quantity": 20, "price": 2500.0},
            {"symbol": "CASH", "quantity": 5000, "price": 1.0}
        ],
        "BROKERAGE_002": [
            {"symbol": "MSFT", "quantity": 50, "price": 300.0},
            {"symbol": "AAPL", "quantity": 80, "price": 150.0},
            {"symbol": "CASH", "quantity": 3000, "price": 1.0}
        ],
        "BROKERAGE_003": [
            {"symbol": "GOOGL", "quantity": 30, "price": 2500.0},
            {"symbol": "MSFT", "quantity": 40, "price": 300.0},
            {"symbol": "CASH", "quantity": 4000, "price": 1.0}
        ],
        "BROKERAGE_004": [
            {"symbol": "AAPL", "quantity": 120, "price": 150.0},
            {"symbol": "CASH", "quantity": 6000, "price": 1.0}
        ],
        "BROKERAGE_005": [
            {"symbol": "MSFT", "quantity": 60, "price": 300.0},
            {"symbol": "GOOGL", "quantity": 25, "price": 2500.0},
            {"symbol": "CASH", "quantity": 2500, "price": 1.0}
        ]
    }
    
    return mock_holdings.get(brokerage_id, [])


@task
def analyze_portfolio(holdings: List[Dict[str, Any]], brokerage_id: str) -> Dict[str, Any]:
    """Analyze a single portfolio and calculate metrics."""
    if not holdings:
        return {
            "brokerage_id": brokerage_id,
            "total_value": 0.0,
            "allocations": {},
            "risk_score": 0.0
        }
    
    total_value = sum(h["quantity"] * h["price"] for h in holdings)
    
    allocations = {}
    for h in holdings:
        symbol = h["symbol"]
        value = h["quantity"] * h["price"]
        allocations[symbol] = value / total_value if total_value > 0 else 0.0
    
    risk_score = len(holdings) * 10
    
    return {
        "brokerage_id": brokerage_id,
        "total_value": total_value,
        "allocations": allocations,
        "risk_score": risk_score
    }


@task
def aggregate_analyses(analyses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate all portfolio analyses and identify rebalancing trades."""
    if not analyses:
        return []
    
    combined_value = sum(a["total_value"] for a in analyses)
    
    combined_allocations = {}
    for analysis in analyses:
        for symbol, allocation in analysis["allocations"].items():
            value_contribution = allocation * analysis["total_value"]
            combined_allocations[symbol] = combined_allocations.get(symbol, 0.0) + value_contribution
    
    for symbol in combined_allocations:
        combined_allocations[symbol] /= combined_value if combined_value > 0 else 1.0
    
    trades = []
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = combined_allocations.get(symbol, 0.0)
        diff = current_pct - target_pct
        
        if abs(diff) > REBALANCING_THRESHOLD:
            trade_amount = abs(diff) * combined_value
            
            action = "SELL" if diff > 0 else "BUY"
            trades.append({
                "action": action,
                "symbol": symbol,
                "amount": round(trade_amount, 2),
                "target_allocation": target_pct,
                "current_allocation": round(current_pct, 4),
                "difference": round(diff, 4)
            })
    
    return trades


@task
def generate_trade_orders_csv(trades: List[Dict[str, Any]], output_path: str = "trade_orders.csv") -> str:
    """Generate CSV file containing all rebalancing trade orders."""
    fieldnames = [
        "action", "symbol", "amount", "target_allocation",
        "current_allocation", "difference"
    ]
    
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if trades:
            writer.writerows(trades)
    
    return output_path


@flow
def portfolio_rebalancing_flow(output_path: str = "trade_orders.csv"):
    """
    Main flow for financial portfolio rebalancing.
    
    Implements fan-out fan-in pattern across 5 brokerage accounts.
    Schedule: Configure daily execution via Prefect deployment schedule
    """
    brokerage_ids = [f"BROKERAGE_{i:03d}" for i in range(1, 6)]
    
    fetch_futures = [fetch_holdings.submit(bid) for bid in brokerage_ids]
    
    analyze_futures = [
        analyze_portfolio.submit(holdings_future, bid)
        for holdings_future, bid in zip(fetch_futures, brokerage_ids)
    ]
    
    trades_future = aggregate_analyses.submit(analyze_futures)
    
    output_file_future = generate_trade_orders_csv.submit(trades_future, output_path)
    
    return output_file_future


if __name__ == "__main__":
    result