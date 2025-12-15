"""
Unified evaluator combining enhanced static analysis and platform compliance.
Uses weighted scoring with penalties to differentiate code quality.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import json
import yaml
import logging

from evaluators.base_evaluator import (
    EvaluationResult,
    EvaluationScore,
    Issue,
    Severity,
    Orchestrator,
)
from evaluators.enhanced_static_analyzer import EnhancedStaticAnalyzer
from evaluators.platform_compliance.airflow_compliance import AirflowComplianceTester
from evaluators.platform_compliance.prefect_compliance import PrefectComplianceTester
from evaluators.platform_compliance.dagster_compliance import DagsterComplianceTester


class UnifiedEvaluator:
    """
    Unified evaluator with weighted scoring and penalties.
    
    Combines:
    1. Static Analysis (50%) - 5 dimensions
    2. Platform Compliance (50%) - 5 dimensions
    
    Each dimension is scored 0-10 with penalties applied for issues.
    
    Penalty System:
    - CRITICAL issues in any dimension = that dimension scores 0
    - MAJOR issues = -2.0 points per issue (capped)
    - MINOR issues = -0.5 points per issue (capped)
    """
    
    COMPLIANCE_TESTERS = {
        Orchestrator.AIRFLOW: AirflowComplianceTester,
        Orchestrator.PREFECT: PrefectComplianceTester,
        Orchestrator.DAGSTER: DagsterComplianceTester,
    }
    
    # Category weights
    CATEGORY_WEIGHTS = {
        "static": 0.50,      # 50% - Static code analysis
        "compliance": 0.50,  # 50% - Platform compliance
    }
    
    def __init__(
        self, 
        config: Optional[Dict[str, Any]] = None,
        intermediate_yaml: Optional[Dict[str, Any]] = None,
    ):
        self.config = config or {}
        self.intermediate_yaml = intermediate_yaml
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize static analyzer
        self.static_analyzer = EnhancedStaticAnalyzer(config)
        
        if intermediate_yaml:
            self.static_analyzer.set_reference(intermediate_yaml)
    
    def set_reference(self, intermediate_yaml: Dict[str, Any]):
        """Set intermediate YAML for reference."""
        self.intermediate_yaml = intermediate_yaml
        self.static_analyzer.set_reference(intermediate_yaml)
    
    def load_reference_from_file(self, yaml_path: Path):
        """Load intermediate YAML from file."""
        with open(yaml_path, 'r') as f:
            self.intermediate_yaml = yaml.safe_load(f)
        self.static_analyzer.set_reference(self.intermediate_yaml)
    
    def evaluate(
        self,
        file_path: Path,
        orchestrator: Optional[Orchestrator] = None
    ) -> Dict[str, Any]:
        """Run complete evaluation."""
        file_path = Path(file_path)
        self.logger.info(f"Running unified evaluation on: {file_path}")
        
        # Run static analysis
        static_result = self.static_analyzer.evaluate(file_path)
        
        # Determine orchestrator
        detected_orchestrator = orchestrator or static_result.orchestrator
        
        # Run platform compliance
        compliance_result = None
        if detected_orchestrator in self.COMPLIANCE_TESTERS:
            tester_class = self.COMPLIANCE_TESTERS[detected_orchestrator]
            tester = tester_class(self.config)
            compliance_result = tester.evaluate(file_path)
        
        # Build combined result
        combined = self._build_combined_result(
            file_path,
            detected_orchestrator,
            static_result,
            compliance_result,
        )
        
        return combined
    
    def _build_combined_result(
        self,
        file_path: Path,
        orchestrator: Orchestrator,
        static_result: EvaluationResult,
        compliance_result: Optional[EvaluationResult],
    ) -> Dict[str, Any]:
        """Build comprehensive combined result."""
        
        combined = {
            "file_path": str(file_path),
            "orchestrator": orchestrator.value,
            "evaluation_timestamp": datetime.now().isoformat(),
            
            # Static Analysis
            "static_analysis": self._format_result(static_result),
            
            # Platform Compliance
            "platform_compliance": self._format_result(compliance_result) if compliance_result else {
                "dimensions": {},
                "overall_score": 0.0,
                "grade": "N/A",
                "total_penalties": 0.0,
                "issues": [],
            },
            
            # Summary
            "summary": {},
        }
        
        # Build summary
        combined["summary"] = self._build_summary(static_result, compliance_result)
        
        return combined
    
    def _format_result(self, result: EvaluationResult) -> Dict[str, Any]:
        """Format evaluation result for output."""
        dimensions = {}
        for name, score in result.scores.items():
            dimensions[name] = {
                "raw_score": round(score.raw_score, 2),
                "penalties": round(score.penalties_applied, 2),
                "final_score": round(score.normalized_score, 2),
                "issue_count": len(score.issues),
                "details": score.details,
            }
        
        return {
            "dimensions": dimensions,
            "gates_passed": result.gates_passed,
            "gate_checks": [g.to_dict() for g in result.gate_checks],
            "overall_score": round(result.overall_score, 2),
            "grade": self._get_grade(result.overall_score),
            "total_penalties": round(result.total_penalties, 2),
            "issues": [i.to_dict() for i in result.all_issues],
            "critical_issues": len(result.critical_issues),
        }
    
    def _build_summary(
        self, 
        static_result: EvaluationResult, 
        compliance_result: Optional[EvaluationResult],
    ) -> Dict[str, Any]:
        """Build summary with weighted scores."""
        
        # Get scores
        static_score = static_result.overall_score if static_result.gates_passed else 0.0
        compliance_score = compliance_result.overall_score if (
            compliance_result and compliance_result.gates_passed
        ) else 0.0
        
        # Calculate weighted combined score
        if compliance_result:
            combined_score = (
                static_score * self.CATEGORY_WEIGHTS["static"] +
                compliance_score * self.CATEGORY_WEIGHTS["compliance"]
            )
        else:
            combined_score = static_score
        
        # Collect all issues
        all_issues = static_result.all_issues.copy()
        if compliance_result:
            all_issues.extend(compliance_result.all_issues)
        
        # Count by severity
        critical = len([i for i in all_issues if i.severity == Severity.CRITICAL])
        major = len([i for i in all_issues if i.severity == Severity.MAJOR])
        minor = len([i for i in all_issues if i.severity == Severity.MINOR])
        
        # Total penalties
        total_penalties = static_result.total_penalties
        if compliance_result:
            total_penalties += compliance_result.total_penalties
        
        # Determine pass/fail
        all_gates_passed = static_result.gates_passed and (
            compliance_result.gates_passed if compliance_result else True
        )
        passed = all_gates_passed and combined_score >= 5.0
        
        summary = {
            # Category scores
            "static_score": round(static_score, 2),
            "static_grade": self._get_grade(static_score),
            
            "compliance_score": round(compliance_score, 2) if compliance_result else None,
            "compliance_grade": self._get_grade(compliance_score) if compliance_result else "N/A",
            
            # Combined
            "combined_score": round(combined_score, 2),
            "combined_grade": self._get_grade(combined_score),
            "passed": passed,
            
            # Penalties
            "total_penalties": round(total_penalties, 2),
            
            # Issues
            "total_issues": len(all_issues),
            "critical_issues": critical,
            "major_issues": major,
            "minor_issues": minor,
            
            # Gates
            "gates_passed": all_gates_passed,
            
            # Dimension breakdown
            "static_dimensions": {
                name: round(score.normalized_score, 2)
                for name, score in static_result.scores.items()
            },
        }
        
        if compliance_result:
            summary["compliance_dimensions"] = {
                name: round(score.normalized_score, 2)
                for name, score in compliance_result.scores.items()
            }
        
        return summary
    
    def _get_grade(self, score: float) -> str:
        """Convert score to letter grade."""
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
    
    def save_results(
        self,
        results: Dict[str, Any],
        output_dir: Path,
        suffix: str = ""
    ) -> Path:
        """Save results to JSON."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_stem = Path(results["file_path"]).stem
        
        filename = f"evaluation_{file_stem}"
        if suffix:
            filename += f"_{suffix}"
        filename += f"_{timestamp}.json"
        
        output_path = output_dir / filename
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to: {output_path}")
        return output_path
    
    def print_summary(self, results: Dict[str, Any]):
        """Print evaluation summary to console."""
        summary = results.get("summary", {})
        
        print("\n" + "=" * 70)
        print("UNIFIED EVALUATION SUMMARY")
        print("=" * 70)
        
        print(f"\nFile: {results['file_path']}")
        print(f"Orchestrator: {results['orchestrator']}")
        print(f"Gates Passed: {'✓' if summary.get('gates_passed') else '✗'}")
        
        print("\n" + "-" * 40)
        print("SCORES")
        print("-" * 40)
        
        print(f"Static Analysis:     {summary.get('static_score', 0):5.2f}/10 ({summary.get('static_grade', 'F')})")
        if summary.get('compliance_score') is not None:
            print(f"Platform Compliance: {summary.get('compliance_score', 0):5.2f}/10 ({summary.get('compliance_grade', 'F')})")
        
        print(f"\nCOMBINED SCORE:      {summary.get('combined_score', 0):5.2f}/10 ({summary.get('combined_grade', 'F')})")
        print(f"STATUS: {'✓ PASSED' if summary.get('passed') else '✗ FAILED'}")
        
        print("\n" + "-" * 40)
        print("ISSUES & PENALTIES")
        print("-" * 40)
        print(f"Total Issues: {summary.get('total_issues', 0)}")
        print(f"  Critical: {summary.get('critical_issues', 0)}")
        print(f"  Major:    {summary.get('major_issues', 0)}")
        print(f"  Minor:    {summary.get('minor_issues', 0)}")
        print(f"Total Penalties: {summary.get('total_penalties', 0):.2f}")
        
        print("\n" + "=" * 70)