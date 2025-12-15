from typing import List, Dict
import csv
import tempfile

from dagster import op, job, In, Out, Nothing, get_dagster_logger


def _mock_holdings(account_id: str) -> List[Dict]:
    """Return mock holdings data for a given brokerage account."""
    # Simple deterministic mock data; in a real scenario this would be fetched from an API.
    base = {
        "BROKERAGE_001": [
            {"symbol": "AAPL", "quantity": 50, "price": 150.0},
            {"symbol": "GOOGL", "quantity": 10, "price": 2800.0},
            {"symbol": "CASH", "quantity": 1, "price": 10000.0},
        ],
        "BROKERAGE_002": [
            {"symbol": "MSFT", "quantity": 30, "price": 300.0},
            {"symbol": "AAPL", "quantity": 20, "price": 150.0},
            {"symbol": "CASH", "quantity": 1, "price": 8000.0},
        ],
        "BROKERAGE_003": [
            {"symbol": "GOOGL", "quantity": 5, "price": 2800.0},
            {"symbol": "MSFT", "quantity": 15, "price": 300.0},
            {"symbol": "CASH", "quantity": 1, "price": 5000.0},
        ],
        "BROKERAGE_004": [
            {"symbol": "AAPL", "quantity": 40, "price": 150.0},
            {"symbol": "MSFT", "quantity": 25, "price": 300.0},
            {"symbol": "CASH", "quantity": 1, "price": 6000.0},
        ],
        "BROKERAGE_005": [
            {"symbol": "GOOGL", "quantity": 8, "price": 2800.0},
            {"symbol": "AAPL", "quantity": 30, "price": 150.0},
            {"symbol": "CASH", "quantity": 1, "price": 7000.0},
        ],
    }
    return base.get(account_id, [])


@op(out=Out(List[Dict]))
def fetch_holdings_001():
    """Fetch holdings for BROKERAGE_001."""
    return _mock_holdings("BROKERAGE_001")


@op(out=Out(List[Dict]))
def fetch_holdings_002():
    """Fetch holdings for BROKERAGE_002."""
    return _mock_holdings("BROKERAGE_002")


@op(out=Out(List[Dict]))
def fetch_holdings_003():
    """Fetch holdings for BROKERAGE_003."""
    return _mock_holdings("BROKERAGE_003")


@op(out=Out(List[Dict]))
def fetch_holdings_004():
    """Fetch holdings for BROKERAGE_004."""
    return _mock_holdings("BROKERAGE_004")


@op(out=Out(List[Dict]))
def fetch_holdings_005():
    """Fetch holdings for BROKERAGE_005."""
    return _mock_holdings("BROKERAGE_005")


def _analyze_holdings(holdings: List[Dict]) -> Dict:
    """Calculate total value, allocation percentages, and a simple risk score."""
    total_value = sum(item["quantity"] * item["price"] for item in holdings)
    allocations = {}
    for item in holdings:
        symbol = item["symbol"]
        value = item["quantity"] * item["price"]
        allocations[symbol] = allocations.get(symbol, 0) + value
    # Convert to percentages
    for symbol in allocations:
        allocations[symbol] = (allocations[symbol] / total_value) * 100 if total_value else 0.0
    risk_score = len(allocations)  # simplistic risk metric: number of distinct symbols
    return {
        "total_value": total_value,
        "allocations": allocations,
        "risk_score": risk_score,
    }


@op(ins={"holdings": In(List[Dict])}, out=Out(Dict))
def analyze_portfolio_001(holdings: List[Dict]):
    """Analyze portfolio for BROKERAGE_001."""
    return _analyze_holdings(holdings)


@op(ins={"holdings": In(List[Dict])}, out=Out(Dict))
def analyze_portfolio_002(holdings: List[Dict]):
    """Analyze portfolio for BROKERAGE_002."""
    return _analyze_holdings(holdings)


@op(ins={"holdings": In(List[Dict])}, out=Out(Dict))
def analyze_portfolio_003(holdings: List[Dict]):
    """Analyze portfolio for BROKERAGE_003."""
    return _analyze_holdings(holdings)


@op(ins={"holdings": In(List[Dict])}, out=Out(Dict))
def analyze_portfolio_004(holdings: List[Dict]):
    """Analyze portfolio for BROKERAGE_004."""
    return _analyze_holdings(holdings)


@op(ins={"holdings": In(List[Dict])}, out=Out(Dict))
def analyze_portfolio_005(holdings: List[Dict]):
    """Analyze portfolio for BROKERAGE_005."""
    return _analyze_holdings(holdings)


@op(
    ins={
        "analysis_001": In(Dict),
        "analysis_002": In(Dict),
        "analysis_003": In(Dict),
        "analysis_004": In(Dict),
        "analysis_005": In(Dict),
    },
    out=Out(Dict),
)
def aggregate_analysis(
    analysis_001: Dict,
    analysis_002: Dict,
    analysis_003: Dict,
    analysis_004: Dict,
    analysis_005: Dict,
):
    """Aggregate analysis from all accounts and compute rebalancing trades."""
    analyses = [
        analysis_001,
        analysis_002,
        analysis_003,
        analysis_004,
        analysis_005,
    ]

    combined_total = sum(a["total_value"] for a in analyses)

    # Weighted allocation values across accounts
    combined_alloc_values: Dict[str, float] = {}
    for a in analyses:
        for symbol, pct in a["allocations"].items():
            value = a["total_value"] * pct / 100.0
            combined_alloc_values[symbol] = combined_alloc_values.get(symbol, 0.0) + value

    combined_allocations = {
        symbol: (value / combined_total) * 100 if combined_total else 0.0
        for symbol, value in combined_alloc_values.items()
    }

    target_allocations = {"AAPL": 30.0, "GOOGL": 25.0, "MSFT": 20.0, "CASH": 25.0}
    threshold = 2.0  # percent

    trades = []
    for symbol, target_pct in target_allocations.items():
        current_pct = combined_allocations.get(symbol, 0.0)
        diff = target_pct - current_pct
        if abs(diff) > threshold:
            action = "BUY" if diff > 0 else "SELL"
            # Approximate amount assuming a flat price of $100 per unit for simplicity
            amount = round((abs(diff) / 100.0) * combined_total / 100.0, 2)
            trades.append(
                {
                    "action": action,
                    "symbol": symbol,
                    "amount": amount,
                    "target_pct": target_pct,
                    "current_pct": round(current_pct, 2),
                }
            )

    return {
        "combined_total": combined_total,
        "combined_allocations": combined_allocations,
        "trades": trades,
    }


@op(ins={"aggregation": In(Dict)}, out=Out(str))
def generate_trade_orders_csv(aggregation: Dict):
    """Write the rebalancing trades to a CSV file and return its path."""
    trades = aggregation.get("trades", [])
    if not trades:
        logger = get_dagster_logger()
        logger.info("No trades required; CSV will contain only headers.")
    fieldnames = ["action", "symbol", "amount", "target_pct", "current_pct"]
    with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="", suffix=".csv") as tmp_file:
        writer = csv.DictWriter(tmp_file, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(trade)
        csv_path = tmp_file.name

    logger = get_dagster_logger()
    logger.info(f"Trade orders CSV written to {csv_path}")
    return csv_path


@job
def rebalance_job():
    """Orchestrate the full portfolio rebalancing pipeline."""
    h1 = fetch_holdings_001()
    h2 = fetch_holdings_002()
    h3 = fetch_holdings_003()
    h4 = fetch_holdings_004()
    h5 = fetch_holdings_005()

    a1 = analyze_portfolio_001(h1)
    a2 = analyze_portfolio_002(h2)
    a3 = analyze_portfolio_003(h3)
    a4 = analyze_portfolio_004(h4)
    a5 = analyze_portfolio_005(h5)

    agg = aggregate_analysis(
        analysis_001=a1,
        analysis_002=a2,
        analysis_003=a3,
        analysis_004=a4,
        analysis_005=a5,
    )

    generate_trade_orders_csv(agg)


if __name__ == "__main__":
    result = rebalance_job.execute_in_process()
    print(f"Job completed successfully: {result.success}")