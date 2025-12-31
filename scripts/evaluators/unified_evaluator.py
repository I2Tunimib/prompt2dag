#!/usr/bin/env python3
"""
Unified evaluator combining SAT + penalty-free PCT.

Paper scoring:
- SAT: from EnhancedStaticAnalyzer
- PCT: from penalty-free platform compliance testers (pct_base)
- Combined score S_code:
      S_code = alpha*SAT + (1-alpha)*PCT
  gated by:
      - platform gate must pass
      - yaml_valid must be True if explicitly provided

CLI:
  python scripts/evaluators/unified_evaluator.py path/to/generated.py --print-summary
"""

# -------------------------------------------------------------------
# Bootstrap imports so this file works as a standalone CLI
# -------------------------------------------------------------------
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]  # .../scripts
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
# -------------------------------------------------------------------

from datetime import datetime
from typing import Any, Dict, Optional, List
import json
import logging

import yaml  # used for optional reference yaml loading

from evaluators.base_evaluator import (
    EvaluationResult,
    Orchestrator,
    BaseEvaluator,
)
from evaluators.enhanced_static_analyzer import EnhancedStaticAnalyzer
from evaluators.platform_compliance.airflow_compliance import AirflowComplianceTester
from evaluators.platform_compliance.prefect_compliance import PrefectComplianceTester
from evaluators.platform_compliance.dagster_compliance import DagsterComplianceTester


def _mean(values: List[float]) -> float:
    vals = [float(v) for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _clamp10(x: float) -> float:
    return max(0.0, min(10.0, float(x)))


def _default_sidecar_path(code_file: Path) -> Path:
    """
    Default output path when user doesn't specify --out or --out-dir:
      foo.py -> foo.py.unified.json
    """
    return code_file.with_name(code_file.name + ".unified.json")


def _flatten_issues(result: Optional[EvaluationResult]) -> List[Dict[str, Any]]:
    if result is None:
        return []
    return [i.to_dict() for i in result.all_issues]


def _issue_summary(issues: List[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "total": len(issues),
        "critical": sum(1 for i in issues if i.get("severity") == "critical"),
        "major": sum(1 for i in issues if i.get("severity") == "major"),
        "minor": sum(1 for i in issues if i.get("severity") == "minor"),
        "info": sum(1 for i in issues if i.get("severity") == "info"),
    }


class UnifiedEvaluator:
    """
    Unified evaluation:
      - SAT: EnhancedStaticAnalyzer
      - PCT: penalty-free platform compliance (gate-based)
    """

    COMPLIANCE_TESTERS = {
        Orchestrator.AIRFLOW: AirflowComplianceTester,
        Orchestrator.PREFECT: PrefectComplianceTester,
        Orchestrator.DAGSTER: DagsterComplianceTester,
    }

    SAT_DIMS = ["correctness", "code_quality", "best_practices", "maintainability", "robustness"]
    PCT_DIMS = ["loadability", "structure_validity", "configuration_validity", "task_validity", "executability"]

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        intermediate_yaml: Optional[Dict[str, Any]] = None,
        alpha: float = 0.5,
        yaml_valid: Optional[bool] = None,
    ):
        self.config = config or {}
        self.intermediate_yaml = intermediate_yaml
        self.alpha = float(alpha)
        self.yaml_valid = yaml_valid  # None => no YAML gate

        self.logger = logging.getLogger(self.__class__.__name__)

        self.static_analyzer = EnhancedStaticAnalyzer(self.config)
        if intermediate_yaml:
            self.static_analyzer.set_reference(intermediate_yaml)

    def set_reference(self, intermediate_yaml: Dict[str, Any]):
        self.intermediate_yaml = intermediate_yaml
        self.static_analyzer.set_reference(intermediate_yaml)

    def set_yaml_valid(self, yaml_valid: bool):
        self.yaml_valid = bool(yaml_valid)

    def load_reference_from_file(self, yaml_path: Path):
        with open(yaml_path, "r") as f:
            self.intermediate_yaml = yaml.safe_load(f)
        self.static_analyzer.set_reference(self.intermediate_yaml)

    # ---------------------------------------------------------------------
    # Core evaluation
    # ---------------------------------------------------------------------

    def evaluate(self, file_path: Path, orchestrator: Optional[Orchestrator] = None) -> Dict[str, Any]:
        file_path = Path(file_path)
        self.logger.info(f"Running unified evaluation on: {file_path}")

        # SAT
        static_result = self.static_analyzer.evaluate(file_path)

        # Orchestrator selection
        detected_orchestrator = orchestrator or static_result.orchestrator

        # PCT
        compliance_result: Optional[EvaluationResult] = None
        if detected_orchestrator in self.COMPLIANCE_TESTERS:
            tester_class = self.COMPLIANCE_TESTERS[detected_orchestrator]
            tester = tester_class(self.config)
            compliance_result = tester.evaluate(file_path)
        else:
            self.logger.warning(
                f"No compliance tester available for orchestrator={detected_orchestrator}. "
                "Compliance will be treated as failed gate."
            )

        combined_payload = self._build_combined_result(
            file_path=file_path,
            orchestrator=detected_orchestrator,
            static_result=static_result,
            compliance_result=compliance_result,
        )
        return combined_payload

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    def _get_sat(self, static_result: EvaluationResult) -> float:
        sat = static_result.metadata.get("SAT")
        if sat is not None:
            return _clamp10(float(sat))
        vals = [static_result.scores[d].raw_score for d in self.SAT_DIMS if d in static_result.scores]
        return _clamp10(_mean(vals))

    def _get_pct(self, compliance_result: Optional[EvaluationResult]) -> float:
        if compliance_result is None or not compliance_result.gates_passed:
            return 0.0
        pct = compliance_result.metadata.get("PCT")
        if pct is not None:
            return _clamp10(float(pct))
        vals = [compliance_result.scores[d].raw_score for d in self.PCT_DIMS if d in compliance_result.scores]
        return _clamp10(_mean(vals))

    def _format_eval_result(self, result: Optional[EvaluationResult], kind: str) -> Dict[str, Any]:
        """
        Return a full JSON payload for a result, including flattened issues and summary.
        """
        if result is None:
            return {
                "note": f"{kind} not executed (no result)",
                "evaluation_type": kind,
                "file_path": None,
                "orchestrator": Orchestrator.UNKNOWN.value,
                "timestamp": datetime.now().isoformat(),
                "metadata": {"error": f"{kind} not executed (no result)"},
                "gates_passed": False,
                "gate_checks": [],
                "scores": {},
                "issues": [],
                "issue_summary": _issue_summary([]),
            }

        payload = result.to_dict()
        flat = _flatten_issues(result)
        payload["issues"] = flat
        payload["issue_summary"] = _issue_summary(flat)

        # convenience overall score
        if kind.upper() == "SAT":
            overall = result.metadata.get("SAT", None)
            if overall is None:
                overall = _mean([result.scores[d].raw_score for d in self.SAT_DIMS if d in result.scores])
            payload["overall_score"] = _clamp10(float(overall))
        elif kind.upper() == "PCT":
            overall = 0.0 if not result.gates_passed else float(result.metadata.get("PCT", 0.0))
            payload["overall_score"] = _clamp10(float(overall))

        return payload

    def _build_combined_result(
        self,
        file_path: Path,
        orchestrator: Orchestrator,
        static_result: EvaluationResult,
        compliance_result: Optional[EvaluationResult],
    ) -> Dict[str, Any]:

        sat_value = self._get_sat(static_result)
        pct_value = self._get_pct(compliance_result)

        platform_gate = bool(compliance_result.gates_passed) if compliance_result is not None else False
        yaml_gate_ok = True if self.yaml_valid is None else bool(self.yaml_valid)

        # Paper scoring
        if (not yaml_gate_ok) or (not platform_gate):
            combined_score = 0.0
        else:
            combined_score = self.alpha * sat_value + (1.0 - self.alpha) * pct_value
        combined_score = _clamp10(combined_score)

        passed = platform_gate  # gate-only

        # Issues: SAT + PCT only
        sat_issues = _flatten_issues(static_result)
        pct_issues = _flatten_issues(compliance_result) if compliance_result is not None else []
        paper_issues = sat_issues + pct_issues

        combined: Dict[str, Any] = {
            "file_path": str(file_path),
            "orchestrator": orchestrator.value,
            "evaluation_timestamp": datetime.now().isoformat(),
            "alpha": float(self.alpha),
            "yaml_valid": yaml_gate_ok,

            "static_analysis": self._format_eval_result(static_result, kind="SAT"),
            "platform_compliance": self._format_eval_result(compliance_result, kind="PCT"),

            "summary": {
                # SAT/PCT paper scoring
                "static_score": round(float(sat_value), 4),
                "compliance_score": round(float(pct_value), 4),
                "combined_score": round(float(combined_score), 4),

                # gating / pass-fail
                "platform_gate_passed": platform_gate,
                "passed": passed,

                # Issue counts (SAT + PCT only)
                "issues": _issue_summary(paper_issues),
            },
        }

        return combined

    def print_summary(self, unified_payload: Dict[str, Any]) -> None:
        s = unified_payload.get("summary", {}) or {}
        issues = s.get("issues", {}) or {}

        print("\n" + "=" * 80)
        print("UNIFIED EVALUATION SUMMARY")
        print("Paper scoring: SAT + PCT")
        print("=" * 80)
        print(f"File:         {unified_payload.get('file_path')}")
        print(f"Orchestrator: {unified_payload.get('orchestrator')}")
        print(f"Alpha:        {unified_payload.get('alpha')}")
        print(f"YAML valid:   {unified_payload.get('yaml_valid')}")

        print("\nPAPER SCORES")
        print(f"  SAT:    {s.get('static_score', 0.0):.2f}/10")
        print(f"  PCT:    {s.get('compliance_score', 0.0):.2f}/10")
        print(f"  S_code: {s.get('combined_score', 0.0):.2f}/10")

        print("\nGATES")
        print(f"  Platform gate passed: {s.get('platform_gate_passed')}")
        print(f"  Passed (gate-only):   {s.get('passed')}")

        print("\nISSUES (SAT+PCT, penalty-free)")
        print(f"  total={issues.get('total', 0)} "
              f"critical={issues.get('critical', 0)} "
              f"major={issues.get('major', 0)} "
              f"minor={issues.get('minor', 0)} "
              f"info={issues.get('info', 0)}")
        print("=" * 80)


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run unified evaluation (SAT + PCT) on a generated workflow file.")
    parser.add_argument("file", help="Path to generated workflow Python file")

    parser.add_argument("--orchestrator", default="auto", choices=["auto", "airflow", "prefect", "dagster"],
                        help="Force orchestrator for PCT. Default: auto-detect from code.")
    parser.add_argument("--alpha", type=float, default=0.5, help="Weight for SAT in combined score")
    parser.add_argument("--yaml-valid", default="none", choices=["true", "false", "none"],
                        help="YAML gate: true/false/none. If false => combined_score forced to 0.")
    parser.add_argument("--reference-yaml", default=None, help="Optional intermediate YAML reference for SAT semantic checks")

    # Output behavior
    parser.add_argument("--out", default=None, help="Write unified JSON to this exact path")
    parser.add_argument("--out-dir", default=None, help="Write unified JSON into this directory (auto filename)")
    parser.add_argument("--stdout", action="store_true", help="Also print JSON to stdout")
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s - %(message)s")

    file_path = Path(args.file)

    # Parse yaml_valid
    if args.yaml_valid == "none":
        yaml_valid = None
    elif args.yaml_valid == "true":
        yaml_valid = True
    else:
        yaml_valid = False

    ue = UnifiedEvaluator(
        alpha=args.alpha,
        yaml_valid=yaml_valid,
    )

    if args.reference_yaml:
        ue.load_reference_from_file(Path(args.reference_yaml))

    # Determine orchestrator arg
    if args.orchestrator == "auto":
        code = ""
        try:
            code = file_path.read_text(encoding="utf-8")
        except Exception:
            code = ""
        orch = BaseEvaluator().detect_orchestrator(code)
    else:
        orch = Orchestrator(args.orchestrator)

    unified_payload = ue.evaluate(file_path, orchestrator=orch)

    if args.print_summary:
        ue.print_summary(unified_payload)

    # Decide output path:
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
    elif args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"unified_{orch.value}_{file_path.stem}_{ts}.json"
    else:
        out_path = _default_sidecar_path(file_path)

    out_path.write_text(json.dumps(unified_payload, indent=2, default=str), encoding="utf-8")
    print(f"Wrote: {out_path}")

    if args.stdout:
        print(json.dumps(unified_payload, indent=2, default=str))


if __name__ == "__main__":
    main()