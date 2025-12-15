"""
Enhanced base evaluator with weighted scoring and semantic analysis support.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
import json
import logging
import sys
import re


class Severity(Enum):
    """Issue severity levels with associated penalties."""
    CRITICAL = "critical"   # Automatic fail or heavy penalty
    MAJOR = "major"         # Significant penalty
    MINOR = "minor"         # Light penalty
    INFO = "info"           # No penalty, informational


class Orchestrator(Enum):
    """Supported orchestrators."""
    AIRFLOW = "airflow"
    PREFECT = "prefect"
    DAGSTER = "dagster"
    UNKNOWN = "unknown"


# Penalty configuration by severity
DEFAULT_PENALTIES = {
    Severity.CRITICAL: 3.0,
    Severity.MAJOR: 1.5,
    Severity.MINOR: 0.5,
    Severity.INFO: 0.0,
}

# Weight configuration for score dimensions
DEFAULT_DIMENSION_WEIGHTS = {
    # Static Analysis Dimensions
    "correctness": 1.0,
    "code_quality": 1.0,
    "best_practices": 1.0,
    "maintainability": 1.0,
    "robustness": 1.0,
    # Compliance Dimensions
    "loadability": 1.0,
    "structure_validity": 1.0,
    "configuration_validity": 1.0,
    "task_validity": 1.0,
    "executability": 1.0,
    # Semantic Dimensions (NEW)
    "semantic_accuracy": 1.5,  # Higher weight for semantic accuracy
    "task_coverage": 1.2,
    "dependency_accuracy": 1.2,
}


@dataclass
class Issue:
    """Represents a single issue found during evaluation."""
    severity: Severity
    category: str
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    tool: Optional[str] = None
    code: Optional[str] = None
    penalty: Optional[float] = None  # Explicit penalty override
    
    def get_penalty(self, penalty_config: Dict[Severity, float] = None) -> float:
        """Get the penalty for this issue."""
        if self.penalty is not None:
            return self.penalty
        config = penalty_config or DEFAULT_PENALTIES
        return config.get(self.severity, 0.0)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity.value,
            "category": self.category,
            "message": self.message,
            "line": self.line,
            "column": self.column,
            "tool": self.tool,
            "code": self.code,
            "penalty": self.get_penalty(),
        }


@dataclass
class EvaluationScore:
    """A single evaluation score with metadata and penalty tracking."""
    name: str
    raw_score: float  # 0-10 scale before penalties
    max_score: float = 10.0
    weight: float = 1.0
    issues: List[Issue] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    penalties_applied: float = 0.0
    
    @property
    def total_penalty(self) -> float:
        """Calculate total penalty from issues."""
        return sum(issue.get_penalty() for issue in self.issues)
    
    @property
    def normalized_score(self) -> float:
        """Return score normalized to 0-10 scale with penalties applied."""
        score = max(0.0, min(10.0, self.raw_score - self.penalties_applied))
        return score
    
    @property
    def weighted_score(self) -> float:
        """Return weighted score for aggregation."""
        return self.normalized_score * self.weight
    
    @property
    def percentage(self) -> float:
        """Return score as percentage (0-100)."""
        return (self.normalized_score / self.max_score) * 100
    
    def apply_penalties(self, penalty_config: Dict[Severity, float] = None):
        """Apply penalties from issues to the score."""
        self.penalties_applied = sum(
            issue.get_penalty(penalty_config) for issue in self.issues
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "raw_score": round(self.raw_score, 2),
            "penalties_applied": round(self.penalties_applied, 2),
            "score": round(self.normalized_score, 2),
            "weight": self.weight,
            "weighted_score": round(self.weighted_score, 2),
            "max_score": self.max_score,
            "percentage": round(self.percentage, 1),
            "issue_count": len(self.issues),
            "issues_by_severity": {
                "critical": len([i for i in self.issues if i.severity == Severity.CRITICAL]),
                "major": len([i for i in self.issues if i.severity == Severity.MAJOR]),
                "minor": len([i for i in self.issues if i.severity == Severity.MINOR]),
                "info": len([i for i in self.issues if i.severity == Severity.INFO]),
            },
            "details": self.details,
            "error": self.error,
        }


@dataclass
class GateCheckResult:
    """Result of a gate check (must pass to continue)."""
    name: str
    passed: bool
    message: str
    is_critical: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "is_critical": self.is_critical,
        }


@dataclass
class EvaluationResult:
    """Complete evaluation result with gate checks, scores, and penalties."""
    evaluation_type: str
    file_path: str
    orchestrator: Orchestrator
    timestamp: str
    
    # Gate checks (must all pass for valid evaluation)
    gate_checks: List[GateCheckResult] = field(default_factory=list)
    
    # Dimension scores
    scores: Dict[str, EvaluationScore] = field(default_factory=dict)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Reference data (for semantic comparison)
    reference_data: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def gates_passed(self) -> bool:
        """Check if all critical gates passed."""
        return all(
            gate.passed for gate in self.gate_checks if gate.is_critical
        )
    
    @property
    def overall_score(self) -> float:
        """Calculate overall weighted score."""
        if not self.gates_passed:
            return 0.0
        
        valid_scores = [s for s in self.scores.values() if s.error is None]
        if not valid_scores:
            return 0.0
        
        total_weight = sum(s.weight for s in valid_scores)
        if total_weight == 0:
            return 0.0
        
        weighted_sum = sum(s.weighted_score for s in valid_scores)
        return weighted_sum / total_weight
    
    @property
    def total_penalties(self) -> float:
        """Calculate total penalties applied."""
        return sum(s.penalties_applied for s in self.scores.values())
    
    @property
    def grade(self) -> str:
        """Calculate letter grade from overall score."""
        score = self.overall_score
        if not self.gates_passed:
            return "F"
        if score >= 9.0:
            return "A"
        elif score >= 8.0:
            return "B"
        elif score >= 7.0:
            return "C"
        elif score >= 5.5:
            return "D"
        else:
            return "F"
    
    @property
    def passed(self) -> bool:
        """Check if evaluation passed minimum threshold."""
        return self.gates_passed and self.overall_score >= 5.0
    
    @property
    def all_issues(self) -> List[Issue]:
        """Collect all issues from all scores."""
        issues = []
        for score in self.scores.values():
            issues.extend(score.issues)
        return issues
    
    @property
    def critical_issues(self) -> List[Issue]:
        """Get only critical issues."""
        return [i for i in self.all_issues if i.severity == Severity.CRITICAL]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "evaluation_type": self.evaluation_type,
            "file_path": self.file_path,
            "orchestrator": self.orchestrator.value,
            "timestamp": self.timestamp,
            
            # Gate checks
            "gates_passed": self.gates_passed,
            "gate_checks": [g.to_dict() for g in self.gate_checks],
            
            # Scores
            "overall_score": round(self.overall_score, 2),
            "total_penalties": round(self.total_penalties, 2),
            "grade": self.grade,
            "passed": self.passed,
            "scores": {name: score.to_dict() for name, score in self.scores.items()},
            
            # Issues
            "issue_summary": {
                "total": len(self.all_issues),
                "critical": len([i for i in self.all_issues if i.severity == Severity.CRITICAL]),
                "major": len([i for i in self.all_issues if i.severity == Severity.MAJOR]),
                "minor": len([i for i in self.all_issues if i.severity == Severity.MINOR]),
            },
            
            # Metadata
            "metadata": self.metadata,
            "reference_data": self.reference_data,
        }


class BaseEvaluator(ABC):
    """Abstract base class for all evaluators."""
    
    EVALUATION_TYPE: str = "base"
    
    def __init__(
        self, 
        config: Optional[Dict[str, Any]] = None,
        penalty_config: Optional[Dict[Severity, float]] = None,
        weight_config: Optional[Dict[str, float]] = None,
    ):
        self.config = config or {}
        self.penalty_config = penalty_config or DEFAULT_PENALTIES
        self.weight_config = weight_config or DEFAULT_DIMENSION_WEIGHTS
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration."""
        log_level = self.config.get("logging", {}).get("level", "INFO").upper()
        numeric_level = getattr(logging, log_level, logging.INFO)
        
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=numeric_level,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[logging.StreamHandler(sys.stdout)],
            )
    
    @abstractmethod
    def evaluate(self, file_path: Path) -> EvaluationResult:
        """Run evaluation on the given file."""
        pass
    
    def detect_orchestrator(self, code: str) -> Orchestrator:
        """Detect which orchestrator the code is written for."""
        code_lower = code.lower()
        
        # Check for Airflow patterns
        airflow_patterns = [
            "from airflow", "import airflow", "dagbag", "dag(",
            "@dag", "airflow.operators", "airflow.providers",
        ]
        
        # Check for Prefect patterns
        prefect_patterns = [
            "from prefect", "import prefect", "@flow", "@task",
            "prefect.task", "prefect.flow",
        ]
        
        # Check for Dagster patterns
        dagster_patterns = [
            "from dagster", "import dagster", "@op", "@job",
            "@asset", "dagster.op", "dagster.job",
        ]
        
        airflow_score = sum(1 for p in airflow_patterns if p in code_lower)
        prefect_score = sum(1 for p in prefect_patterns if p in code_lower)
        dagster_score = sum(1 for p in dagster_patterns if p in code_lower)
        
        max_score = max(airflow_score, prefect_score, dagster_score)
        
        if max_score == 0:
            return Orchestrator.UNKNOWN
        elif airflow_score == max_score:
            return Orchestrator.AIRFLOW
        elif prefect_score == max_score:
            return Orchestrator.PREFECT
        else:
            return Orchestrator.DAGSTER
    
    def get_weight(self, dimension: str) -> float:
        """Get weight for a dimension."""
        return self.weight_config.get(dimension, 1.0)
    
    def apply_penalties_to_result(self, result: EvaluationResult):
        """Apply penalties to all scores in the result."""
        for score in result.scores.values():
            score.apply_penalties(self.penalty_config)
    
    def save_results(
        self,
        result: EvaluationResult,
        output_dir: Path,
        suffix: str = ""
    ) -> Path:
        """Save evaluation results to JSON file."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_stem = Path(result.file_path).stem
        
        filename = f"{self.EVALUATION_TYPE}_{file_stem}"
        if suffix:
            filename += f"_{suffix}"
        filename += f"_{timestamp}.json"
        
        output_path = output_dir / filename
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        
        self.logger.info(f"Results saved to: {output_path}")
        return output_path


def calculate_score_from_issues(
    issues: List[Issue],
    base_score: float = 10.0,
    penalty_config: Dict[Severity, float] = None,
) -> float:
    """Calculate score from issue list with configurable penalties."""
    config = penalty_config or DEFAULT_PENALTIES
    penalty = sum(issue.get_penalty(config) for issue in issues)
    return max(0.0, min(base_score, base_score - penalty))


def extract_key_terms(text: str) -> Set[str]:
    """Extract key terms from text for semantic comparison."""
    # Common stopwords to filter out
    stopwords = {
        'the', 'a', 'an', 'is', 'are', 'to', 'for', 'and', 'or', 'in', 'on',
        'at', 'by', 'with', 'from', 'as', 'be', 'was', 'were', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'must', 'shall', 'can', 'of', 'that', 'this',
        'these', 'those', 'it', 'its', 'if', 'then', 'else', 'when', 'where',
        'which', 'who', 'what', 'how', 'why', 'all', 'each', 'every', 'both',
        'few', 'more', 'most', 'other', 'some', 'such', 'no', 'not', 'only',
        'own', 'same', 'so', 'than', 'too', 'very', 'just', 'also', 'now',
    }
    
    # Extract words (3+ chars)
    words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]{2,}\b', text.lower())
    
    # Filter stopwords
    return {w for w in words if w not in stopwords}


def extract_action_verbs(text: str) -> Set[str]:
    """Extract action verbs relevant to data pipelines."""
    action_verbs = {
        'extract', 'transform', 'load', 'process', 'clean', 'validate',
        'fetch', 'save', 'upload', 'download', 'merge', 'join', 'filter',
        'aggregate', 'compute', 'calculate', 'send', 'notify', 'create',
        'delete', 'update', 'insert', 'select', 'query', 'read', 'write',
        'parse', 'convert', 'normalize', 'deduplicate', 'enrich', 'reconcile',
        'export', 'import', 'ingest', 'publish', 'subscribe', 'stream',
        'batch', 'schedule', 'trigger', 'wait', 'monitor', 'check', 'test',
    }
    
    text_lower = text.lower()
    found = set()
    
    for verb in action_verbs:
        if verb in text_lower:
            found.add(verb)
    
    return found