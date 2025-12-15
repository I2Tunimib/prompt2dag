from dagster import op, job
from typing import List, Dict, Any
import csv
from datetime import datetime


# Type aliases for clarity
Holdings = List[Dict[str, Any]]
AnalysisResult = Dict[str, Any]
RebalancingTrade = Dict[str, Any]

# Target allocations (embedded in aggregation logic as per description)
# Could be moved to Dagster config: {"aapl_target": 0.30, "googl_target": 0.25, ...}
TARGET_ALLOCATIONS = {
    "AAPL": 0.30,
    "GOOGL": 0.25,
    "MSFT": 0.20,
    "CASH": 0.25
}

REBALANCING_THRESHOLD = 0.02  # 2% threshold


def mock_holdings_data(account_id: str) -> Holdings:
    """Generate mock holdings data for a brokerage account."""
    mock_data = {
        "BROKERAGE_001": [
            {"symbol": "AAPL", "quantity": 100, "price": 150.0},
            {"symbol": "GOOGL", "quantity": 50, "price": 2800.0},
            {"symbol": "CASH", "quantity": 10000, "price": 1.0},
        ],
        "BROKERAGE_002": [
            {"symbol": "MSFT", "quantity": 75, "price": 300.0},
            {"symbol": "GOOGL", "quantity": 30, "price": 2800.0},
            {"symbol": "CASH", "quantity": 15000, "price": 1.0},
        ],
        "BROKERAGE_003": [
            {"symbol": "AAPL", "quantity": 80, "price": 150.0},
            {"symbol": "MSFT", "quantity": 60, "price": 300.0},
            {"symbol": "CASH", "quantity": 8000, "price": 1.0},
        ],
        "BROKERAGE_004": [
            {"symbol": "GOOGL", "quantity": 40, "price": 2800.0},
            {"symbol": "AAPL", "quantity": 50, "price": 150.0},
            {"symbol": "CASH", "quantity": 12000, "price": 1.0},
        ],
        "BROKERAGE_005": [
            {"symbol": "MSFT", "quantity": 90, "price": 300.0},
            {"symbol": "AAPL", "quantity": 70, "price": 150.0},
            {"symbol": "CASH", "quantity": 9000, "price": 1.0},
        ],
    }
    return mock_data.get(account_id, [])


@op
def fetch_brokerage_001() -> Holdings:
    """Fetch holdings data from BROKERAGE_001."""
    return mock_holdings_data("BROKERAGE_001")


@op
def fetch_brokerage_002() -> Holdings:
    """Fetch holdings data from BROKERAGE_002."""
    return mock_holdings_data("BROKERAGE_002")


@op
def fetch_brokerage_003() -> Holdings:
    """Fetch holdings data from BROKERAGE_003."""
    return mock_holdings_data("BROKERAGE_003")


@op
def fetch_brokerage_004() -> Holdings:
    """Fetch holdings data from BROKERAGE_004."""
    return mock_holdings_data("BROKERAGE_004")


@op
def fetch_brokerage_005() -> Holdings:
    """Fetch holdings data from BROKERAGE_005."""
    return mock_holdings_data("BROKERAGE_005")


def analyze_portfolio(holdings: Holdings, account_id: str) -> AnalysisResult:
    """Calculate portfolio value, allocation percentages, and risk score."""
    total_value = sum(holding["quantity"] * holding["price"] for holding in holdings)
    
    allocations = {}
    for holding in holdings:
        symbol = holding["symbol"]
        value = holding["quantity"] * holding["price"]
        allocations[symbol] = value / total_value if total_value > 0 else 0
    
    # Simple risk score based on number of holdings (more holdings = lower concentration risk)
    risk_score = min(100, len(holdings) * 25)
    
    return {
        "account_id": account_id,
        "total_value": total_value,
        "allocations": allocations,
        "risk_score": risk_score,
        "holdings_count": len(holdings)
    }


@op
def analyze_brokerage_001(holdings: Holdings) -> AnalysisResult:
    """Analyze portfolio for BROKERAGE_001."""
    return analyze_portfolio(holdings, "BROKERAGE_001")


@op
def analyze_brokerage_002(holdings: Holdings) -> AnalysisResult:
    """Analyze portfolio for BROKERAGE_002."""
    return analyze_portfolio(holdings, "BROKERAGE_002")


@op
def analyze_brokerage_003(holdings: Holdings) -> AnalysisResult:
    """Analyze portfolio for BROKERAGE_003."""
    return analyze_portfolio(holdings, "BROKERAGE_003")


@op
def analyze_brokerage_004(holdings: Holdings) -> AnalysisResult:
    """Analyze portfolio for BROKERAGE_004."""
    return analyze_portfolio(holdings, "BROKERAGE_004")


@op
def analyze_brokerage_005(holdings: Holdings) -> AnalysisResult:
    """Analyze portfolio for BROKERAGE_005."""
    return analyze_portfolio(holdings, "BROKERAGE_005")


@op
def aggregate_portfolio_analysis(
    analysis_001: AnalysisResult,
    analysis_002: AnalysisResult,
    analysis_003: AnalysisResult,
    analysis_004: AnalysisResult,
    analysis_005: AnalysisResult,
) -> List[RebalancingTrade]:
    """Aggregate all portfolio analyses and calculate rebalancing trades."""
    analyses = [analysis_001, analysis_002, analysis_003, analysis_004, analysis_005]
    
    # Calculate combined portfolio value
    combined_value = sum(analysis["total_value"] for analysis in analyses)
    
    # Calculate combined allocations
    combined_allocations = {}
    for analysis in analyses:
        for symbol, allocation in analysis["allocations"].items():
            value_contribution = allocation * analysis["total_value"]
            if symbol not in combined_allocations:
                combined_allocations[symbol] = 0
            combined_allocations[symbol] += value_contribution
    
    # Convert to percentages
    for symbol in combined_allocations:
        combined_allocations[symbol] = combined_allocations[symbol] / combined_value
    
    # Calculate rebalancing trades
    rebalancing_trades = []
    
    for symbol, target_pct in TARGET_ALLOCATIONS.items():
        current_pct = combined_allocations.get(symbol, 0)
        diff = current_pct - target_pct
        
        if abs(diff) > REBALANCING_THRESHOLD:
            trade_amount = diff * combined_value
            action = "SELL" if diff > 0 else "BUY"
            
            rebalancing_trades.append({
                "symbol": symbol,
                "action": action,
                "amount": abs(trade_amount),
                "current_allocation": current_pct,
                "target_allocation": target_pct,
                "difference": diff
            })
    
    return rebalancing_trades


@op
def generate_trade_orders(rebalancing_trades: List[RebalancingTrade]) -> str:
    """Generate CSV file with rebalancing trade orders."""
    if not rebalancing_trades:
        filename = "no_trades_needed.csv"
        with open(filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["message"])
            writer.writerow(["No rebalancing trades needed. All allocations within threshold."])
        return filename
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"trade_orders_{timestamp}.csv"
    
    fieldnames = ["symbol", "action", "amount", "current_allocation", "target_allocation", "difference"]
    
    with open(filename, "w", newline="")