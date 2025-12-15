import random
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dagster import In, Out, JobDefinition, job, op


SYMBOLS = ["AAPL", "GOOGL", "MSFT", "CASH"]
TARGET_ALLOCATIONS = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25,
}
TRADE_THRESHOLD = 0.02  # 2%


def _generate_mock_holdings() -> pd.DataFrame:
    """Create a mock holdings DataFrame with random quantities and prices."""
    random.seed(42)
    data = {
        "symbol": SYMBOLS,
        "quantity": [random.randint(10, 100) for _ in SYMBOLS],
        "price": [round(random.uniform(100, 500), 2) for _ in SYMBOLS],
    }
    return pd.DataFrame(data)


@op(out=Out(pd.DataFrame), description="Fetch holdings for brokerage 001.")
def fetch_holdings_001() -> pd.DataFrame:
    return _generate_mock_holdings()


@op(out=Out(pd.DataFrame), description="Fetch holdings for brokerage 002.")
def fetch_holdings_002() -> pd.DataFrame:
    return _generate_mock_holdings()


@op(out=Out(pd.DataFrame), description="Fetch holdings for brokerage 003.")
def fetch_holdings_003() -> pd.DataFrame:
    return _generate_mock_holdings()


@op(out=Out(pd.DataFrame), description="Fetch holdings for brokerage 004.")
def fetch_holdings_004() -> pd.DataFrame:
    return _generate_mock_holdings()


@op(out=Out(pd.DataFrame), description="Fetch holdings for brokerage 005.")
def fetch_holdings_005() -> pd.DataFrame:
    return _generate_mock_holdings()


def _analyze_holdings(holdings: pd.DataFrame) -> Dict:
    """Calculate total value, allocation percentages, and a simple risk score."""
    holdings["value"] = holdings["quantity"] * holdings["price"]
    total_value = holdings["value"].sum()
    allocations = (
        holdings.set_index("symbol")["value"] / total_value
    ).to_dict()
    risk_score = len(holdings)  # simplistic risk metric
    prices = holdings.set_index("symbol")["price"].to_dict()
    return {
        "total_value": total_value,
        "allocations": allocations,
        "risk_score": risk_score,
        "prices": prices,
    }


@op(
    ins={"holdings": In(pd.DataFrame)},
    out=Out(dict),
    description="Analyze portfolio for brokerage 001.",
)
def analyze_portfolio_001(holdings: pd.DataFrame) -> Dict:
    return _analyze_holdings(holdings)


@op(
    ins={"holdings": In(pd.DataFrame)},
    out=Out(dict),
    description="Analyze portfolio for brokerage 002.",
)
def analyze_portfolio_002(holdings: pd.DataFrame) -> Dict:
    return _analyze_holdings(holdings)


@op(
    ins={"holdings": In(pd.DataFrame)},
    out=Out(dict),
    description="Analyze portfolio for brokerage 003.",
)
def analyze_portfolio_003(holdings: pd.DataFrame) -> Dict:
    return _analyze_holdings(holdings)


@op(
    ins={"holdings": In(pd.DataFrame)},
    out=Out(dict),
    description="Analyze portfolio for brokerage 004.",
)
def analyze_portfolio_004(holdings: pd.DataFrame) -> Dict:
    return _analyze_holdings(holdings)


@op(
    ins={"holdings": In(pd.DataFrame)},
    out=Out(dict),
    description="Analyze portfolio for brokerage 005.",
)
def analyze_portfolio_005(holdings: pd.DataFrame) -> Dict:
    return _analyze_holdings(holdings)


@op(
    ins={
        "a1": In(dict),
        "a2": In(dict),
        "a3": In(dict),
        "a4": In(dict),
        "a5": In(dict),
    },
    out=Out(dict),
    description="Aggregate analysis results from all brokerages.",
)
def aggregate_analysis(
    a1: Dict, a2: Dict, a3: Dict, a4: Dict, a5: Dict
) -> Dict:
    analyses = [a1, a2, a3, a4, a5]

    combined_total = sum(a["total_value"] for a in analyses)

    # Compute combined value per symbol
    combined_values: Dict[str, float] = {sym: 0.0 for sym in SYMBOLS}
    combined_prices: Dict[str, float] = {}

    for a in analyses:
        for sym, alloc in a["allocations"].items():
            value = alloc * a["total_value"]
            combined_values[sym] += value
        # Keep the latest price for each symbol (prices are consistent across mocks)
        combined_prices.update(a["prices"])

    combined_allocations = {
        sym: combined_values[sym] / combined_total for sym in SYMBOLS
    }

    rebalancing_trades: List[Dict] = []
    for sym in SYMBOLS:
        current_pct = combined_allocations.get(sym, 0.0)
        target_pct = TARGET_ALLOCATIONS.get(sym, 0.0)
        diff = current_pct - target_pct
        if abs(diff) > TRADE_THRESHOLD:
            action = "SELL" if diff > 0 else "BUY"
            amount_usd = abs(diff) * combined_total
            rebalancing_trades.append(
                {
                    "symbol": sym,
                    "action": action,
                    "amount_usd": round(amount_usd, 2),
                    "target_pct": round(target_pct * 100, 2),
                    "current_pct": round(current_pct * 100, 2),
                }
            )

    return {
        "combined_total_value": combined_total,
        "combined_allocations": combined_allocations,
        "rebalancing_trades": rebalancing_trades,
    }


@op(
    ins={"aggregation": In(dict)},
    out=Out(str),
    description="Generate a CSV file with trade orders.",
)
def generate_trade_orders_csv(aggregation: Dict) -> str:
    trades = aggregation.get("rebalancing_trades", [])
    if not trades:
        return "No trades required; CSV not generated."

    df = pd.DataFrame(trades)
    output_path = Path("trade_orders.csv")
    df.to_csv(output_path, index=False)
    return str(output_path)


@job(description="Financial portfolio rebalancing pipeline.")
def rebalance_portfolio_job():
    # Fetch phase (fan‑out)
    holdings_001 = fetch_holdings_001()
    holdings_002 = fetch_holdings_002()
    holdings_003 = fetch_holdings_003()
    holdings_004 = fetch_holdings_004()
    holdings_005 = fetch_holdings_005()

    # Analysis phase (fan‑out)
    analysis_001 = analyze_portfolio_001(holdings_001)
    analysis_002 = analyze_portfolio_002(holdings_002)
    analysis_003 = analyze_portfolio_003(holdings_003)
    analysis_004 = analyze_portfolio_004(holdings_004)
    analysis_005 = analyze_portfolio_005(holdings_005)

    # Aggregation (fan‑in)
    aggregation = aggregate_analysis(
        analysis_001,
        analysis_002,
        analysis_003,
        analysis_004,
        analysis_005,
    )

    # Generate CSV
    generate_trade_orders_csv(aggregation)


if __name__ == "__main__":
    result = rebalance_portfolio_job.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline execution failed.")