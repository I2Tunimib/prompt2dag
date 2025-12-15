"""
Enhanced static analyzer with meaningful differentiation metrics.
Measures not just "does it work?" but "how well does it work?"
"""

import ast
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from evaluators.base_evaluator import (
    BaseEvaluator,
    EvaluationResult,
    EvaluationScore,
    Issue,
    Severity,
    Orchestrator,
)

# Optional imports
try:
    from pylint.lint import Run as PylintRun
    from pylint.reporters import JSONReporter
    PYLINT_AVAILABLE = True
except ImportError:
    PYLINT_AVAILABLE = False

try:
    from radon.complexity import cc_visit
    from radon.metrics import mi_visit
    RADON_AVAILABLE = True
except ImportError:
    RADON_AVAILABLE = False

try:
    from bandit.core import manager as bandit_manager
    from bandit.core import config as bandit_config
    import bandit
    BANDIT_AVAILABLE = True
except ImportError:
    BANDIT_AVAILABLE = False


class EnhancedStaticAnalyzer(BaseEvaluator):
    """
    Enhanced static analyzer with 5-dimensional quality assessment.
    
    Dimensions:
    1. Correctness (0-10): Does it work as intended?
    2. Code Quality (0-10): How well is it written?
    3. Best Practices (0-10): Does it follow conventions?
    4. Maintainability (0-10): How easy to maintain?
    5. Robustness (0-10): How resilient to failures?
    """
    
    EVALUATION_TYPE = "enhanced_static_analysis"
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.intermediate_yaml: Optional[Dict] = None
    
    def set_reference(self, intermediate_yaml: Dict):
        """Set the intermediate YAML for semantic comparison."""
        self.intermediate_yaml = intermediate_yaml
    
    def evaluate(self, file_path: Path) -> EvaluationResult:
        """Run enhanced static analysis."""
        self.logger.info(f"Running enhanced static analysis on: {file_path}")
        
        try:
            code = file_path.read_text(encoding="utf-8")
        except Exception as e:
            return self._error_result(file_path, f"Failed to read file: {e}")
        
        orchestrator = self.detect_orchestrator(code)
        self.logger.info(f"Detected orchestrator: {orchestrator.value}")
        
        # Parse AST once for reuse
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return self._syntax_error_result(file_path, e)
        
        result = EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=orchestrator,
            timestamp=datetime.now().isoformat(),
            metadata={
                "file_size_bytes": len(code.encode("utf-8")),
                "line_count": len(code.splitlines()),
            }
        )
        
        # Run all 5 dimensions
        result.scores["correctness"] = self._evaluate_correctness(code, tree, orchestrator)
        result.scores["code_quality"] = self._evaluate_code_quality(code, tree, file_path)
        result.scores["best_practices"] = self._evaluate_best_practices(code, tree, orchestrator)
        result.scores["maintainability"] = self._evaluate_maintainability(code, tree)
        result.scores["robustness"] = self._evaluate_robustness(code, tree, file_path, orchestrator)
        
        return result
    
    def _error_result(self, file_path: Path, error: str) -> EvaluationResult:
        return EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=Orchestrator.UNKNOWN,
            timestamp=datetime.now().isoformat(),
            scores={"error": EvaluationScore(name="error", raw_score=0.0, error=error)},
        )
    
    def _syntax_error_result(self, file_path: Path, error: SyntaxError) -> EvaluationResult:
        return EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=Orchestrator.UNKNOWN,
            timestamp=datetime.now().isoformat(),
            scores={
                "correctness": EvaluationScore(name="correctness", raw_score=0.0, 
                    issues=[Issue(Severity.CRITICAL, "syntax", f"Syntax error: {error.msg}", error.lineno)]),
                "code_quality": EvaluationScore(name="code_quality", raw_score=0.0),
                "best_practices": EvaluationScore(name="best_practices", raw_score=0.0),
                "maintainability": EvaluationScore(name="maintainability", raw_score=0.0),
                "robustness": EvaluationScore(name="robustness", raw_score=0.0),
            },
        )
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 1: CORRECTNESS (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_correctness(
        self, 
        code: str, 
        tree: ast.AST, 
        orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate correctness - does it work as intended?
        
        Components:
        - Syntax validity (0-2): Already passed if we're here
        - Import feasibility (0-2): Can imports resolve?
        - Structure completeness (0-2): Has required constructs?
        - Semantic accuracy (0-4): Matches intent from intermediate YAML?
        """
        self.logger.info("Evaluating correctness...")
        
        score = 0.0
        details = {}
        issues = []
        
        # 1. Syntax validity (0-2) - Already passed
        score += 2.0
        details["syntax_valid"] = True
        
        # 2. Import feasibility (0-2)
        import_score, import_issues = self._check_imports(code, tree)
        score += import_score
        issues.extend(import_issues)
        details["import_score"] = import_score
        
        # 3. Structure completeness (0-2)
        structure_score, structure_issues = self._check_structure_completeness(
            code, tree, orchestrator
        )
        score += structure_score
        issues.extend(structure_issues)
        details["structure_score"] = structure_score
        
        # 4. Semantic accuracy (0-4)
        if self.intermediate_yaml:
            semantic_score, semantic_issues = self._check_semantic_accuracy(
                code, tree, orchestrator
            )
            score += semantic_score
            issues.extend(semantic_issues)
            details["semantic_score"] = semantic_score
        else:
            # No reference - give partial credit
            score += 2.0
            details["semantic_score"] = 2.0
            details["semantic_note"] = "No intermediate YAML reference provided"
        
        return EvaluationScore(
            name="correctness",
            raw_score=min(10.0, score),
            issues=issues,
            details=details,
        )
    
    def _check_imports(self, code: str, tree: ast.AST) -> Tuple[float, List[Issue]]:
        """Check import statements for common issues."""
        issues = []
        
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        # Check for problematic imports
        problematic = ["*"]  # Star imports are bad
        relative_imports = [i for i in imports if i and i.startswith(".")]
        
        score = 2.0
        
        if any("*" in code.split("import")[i] if i > 0 else False 
               for i in range(len(code.split("import")))):
            issues.append(Issue(
                Severity.MINOR, "import", "Star import detected - reduces clarity"
            ))
            score -= 0.3
        
        if relative_imports:
            issues.append(Issue(
                Severity.INFO, "import", 
                f"Relative imports detected: {relative_imports}"
            ))
        
        return max(0.0, score), issues
    
    def _check_structure_completeness(
        self, 
        code: str, 
        tree: ast.AST, 
        orchestrator: Orchestrator
    ) -> Tuple[float, List[Issue]]:
        """Check if code has complete orchestrator structure."""
        issues = []
        score = 0.0
        
        if orchestrator == Orchestrator.AIRFLOW:
            # Must have: DAG definition, at least one task, dependencies
            has_dag = "DAG(" in code or "@dag" in code.lower()
            has_tasks = any(op in code for op in ["Operator(", "operator(", "@task"])
            has_deps = ">>" in code or "<<" in code or "chain(" in code
            
            if has_dag:
                score += 0.8
            else:
                issues.append(Issue(Severity.CRITICAL, "structure", "No DAG definition"))
            
            if has_tasks:
                score += 0.7
            else:
                issues.append(Issue(Severity.CRITICAL, "structure", "No tasks defined"))
            
            if has_deps:
                score += 0.5
            else:
                issues.append(Issue(Severity.MAJOR, "structure", "No task dependencies"))
        
        elif orchestrator == Orchestrator.PREFECT:
            has_flow = "@flow" in code
            has_tasks = "@task" in code
            
            if has_flow:
                score += 1.0
            else:
                issues.append(Issue(Severity.CRITICAL, "structure", "No @flow decorator"))
            
            if has_tasks:
                score += 1.0
            else:
                issues.append(Issue(Severity.MAJOR, "structure", "No @task decorators"))
        
        elif orchestrator == Orchestrator.DAGSTER:
            has_job = "@job" in code
            has_ops = "@op" in code
            
            if has_job:
                score += 1.0
            else:
                issues.append(Issue(Severity.CRITICAL, "structure", "No @job decorator"))
            
            if has_ops:
                score += 1.0
            else:
                issues.append(Issue(Severity.MAJOR, "structure", "No @op decorators"))
        
        return min(2.0, score), issues
    
    def _check_semantic_accuracy(
        self, 
        code: str, 
        tree: ast.AST, 
        orchestrator: Orchestrator
    ) -> Tuple[float, List[Issue]]:
        """Check if generated code matches the intent from intermediate YAML."""
        issues = []
        score = 0.0
        
        if not self.intermediate_yaml:
            return 2.0, issues
        
        # Get expected tasks from intermediate YAML
        expected_tasks = self.intermediate_yaml.get("tasks", [])
        expected_task_ids = {t.get("task_id") for t in expected_tasks}
        
        # Find tasks in generated code
        found_tasks = set()
        
        # Look for task_id assignments
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        if target.id in expected_task_ids or any(
                            tid in target.id for tid in expected_task_ids
                        ):
                            found_tasks.add(target.id)
            
            # Also check function definitions
            if isinstance(node, ast.FunctionDef):
                if node.name in expected_task_ids:
                    found_tasks.add(node.name)
        
        # Calculate coverage
        if expected_task_ids:
            coverage = len(found_tasks) / len(expected_task_ids)
            score = 4.0 * coverage
            
            missing = expected_task_ids - found_tasks
            if missing:
                issues.append(Issue(
                    Severity.MAJOR, "semantic",
                    f"Missing tasks from spec: {missing}"
                ))
        else:
            score = 2.0  # No expected tasks - partial credit
        
        return min(4.0, score), issues
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 2: CODE QUALITY (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_code_quality(
        self, 
        code: str, 
        tree: ast.AST, 
        file_path: Path
    ) -> EvaluationScore:
        """
        Evaluate code quality - how well is it written?
        
        Components:
        - Style compliance (0-2.5): Flake8 score
        - Linting score (0-2.5): Pylint score
        - Documentation coverage (0-2.5): Docstrings presence
        - Naming quality (0-2.5): PEP8 naming conventions
        """
        self.logger.info("Evaluating code quality...")
        
        score = 0.0
        details = {}
        issues = []
        
        # 1. Style compliance - Flake8 (0-2.5)
        style_score, style_issues = self._run_flake8(code, file_path)
        score += style_score * 2.5 / 10.0  # Normalize to 2.5
        issues.extend(style_issues)
        details["style_score"] = round(style_score, 2)
        
        # 2. Linting - Pylint (0-2.5)
        lint_score, lint_issues = self._run_pylint(code, file_path)
        score += lint_score * 2.5 / 10.0
        issues.extend(lint_issues)
        details["lint_score"] = round(lint_score, 2)
        
        # 3. Documentation coverage (0-2.5)
        doc_score, doc_issues = self._evaluate_documentation(tree)
        score += doc_score
        issues.extend(doc_issues)
        details["documentation_score"] = round(doc_score, 2)
        
        # 4. Naming quality (0-2.5)
        naming_score, naming_issues = self._evaluate_naming(tree)
        score += naming_score
        issues.extend(naming_issues)
        details["naming_score"] = round(naming_score, 2)
        
        return EvaluationScore(
            name="code_quality",
            raw_score=min(10.0, score),
            issues=issues,
            details=details,
        )
    
    def _run_flake8(self, code: str, file_path: Path) -> Tuple[float, List[Issue]]:
        """Run Flake8 and return normalized score."""
        issues = []
        
        try:
            result = subprocess.run(
                ["flake8", "--format=json", str(file_path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            
            violations = []
            if result.stdout:
                try:
                    output = json.loads(result.stdout)
                    violations = output.get(str(file_path), [])
                except json.JSONDecodeError:
                    pass
            
            # Calculate score based on violations per 100 lines
            lines = len(code.splitlines()) or 1
            violations_per_100 = (len(violations) / lines) * 100
            
            if violations_per_100 == 0:
                score = 10.0
            elif violations_per_100 < 1:
                score = 9.0
            elif violations_per_100 < 3:
                score = 7.5
            elif violations_per_100 < 5:
                score = 6.0
            elif violations_per_100 < 10:
                score = 4.0
            else:
                score = 2.0
            
            # Add issues for significant violations
            for v in violations[:5]:  # Limit to top 5
                issues.append(Issue(
                    Severity.MINOR, "style",
                    f"[{v.get('code')}] {v.get('text')}",
                    v.get('line_number'),
                    tool="flake8"
                ))
            
            return score, issues
        
        except Exception as e:
            self.logger.warning(f"Flake8 failed: {e}")
            return 5.0, []  # Neutral score
    
    def _run_pylint(self, code: str, file_path: Path) -> Tuple[float, List[Issue]]:
        """Run Pylint and return normalized score."""
        if not PYLINT_AVAILABLE:
            return 5.0, []
        
        issues = []
        
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False
            ) as f:
                f.write(code)
                temp_path = f.name
            
            output_buffer = StringIO()
            reporter = JSONReporter(output_buffer)
            
            try:
                PylintRun([temp_path], reporter=reporter, exit=False)
            except SystemExit:
                pass
            
            output_buffer.seek(0)
            messages = []
            if output_buffer.getvalue():
                try:
                    messages = json.loads(output_buffer.getvalue())
                except json.JSONDecodeError:
                    pass
            
            # Count by type
            errors = sum(1 for m in messages if m.get("type") in ["error", "fatal"])
            warnings = sum(1 for m in messages if m.get("type") == "warning")
            conventions = sum(1 for m in messages if m.get("type") == "convention")
            
            # Calculate score
            if errors == 0 and warnings == 0:
                score = 10.0 - (conventions * 0.1)
            elif errors == 0:
                score = 8.0 - (warnings * 0.3)
            else:
                score = 5.0 - (errors * 1.0)
            
            score = max(0.0, min(10.0, score))
            
            # Add significant issues
            for m in messages[:3]:
                issues.append(Issue(
                    Severity.MAJOR if m.get("type") in ["error", "warning"] else Severity.MINOR,
                    "lint",
                    f"[{m.get('symbol')}] {m.get('message')}",
                    m.get("line"),
                    tool="pylint"
                ))
            
            return score, issues
        
        except Exception as e:
            self.logger.warning(f"Pylint failed: {e}")
            return 5.0, []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def _evaluate_documentation(self, tree: ast.AST) -> Tuple[float, List[Issue]]:
        """Evaluate documentation coverage."""
        issues = []
        
        # Count documentable items and documented items
        functions = []
        classes = []
        module_docstring = ast.get_docstring(tree)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                functions.append({
                    "name": node.name,
                    "has_docstring": ast.get_docstring(node) is not None,
                    "lineno": node.lineno,
                })
            elif isinstance(node, ast.ClassDef):
                classes.append({
                    "name": node.name,
                    "has_docstring": ast.get_docstring(node) is not None,
                    "lineno": node.lineno,
                })
        
        # Calculate coverage
        total_items = 1 + len(functions) + len(classes)  # +1 for module
        documented_items = (1 if module_docstring else 0) + \
                          sum(1 for f in functions if f["has_docstring"]) + \
                          sum(1 for c in classes if c["has_docstring"])
        
        coverage = documented_items / total_items if total_items > 0 else 0
        
        # Score: 0-2.5 based on coverage
        if coverage >= 0.9:
            score = 2.5
        elif coverage >= 0.7:
            score = 2.0
        elif coverage >= 0.5:
            score = 1.5
        elif coverage >= 0.3:
            score = 1.0
        elif coverage > 0:
            score = 0.5
        else:
            score = 0.0
            issues.append(Issue(
                Severity.MINOR, "documentation",
                "No documentation found (no docstrings)"
            ))
        
        # Note undocumented functions
        undocumented = [f["name"] for f in functions if not f["has_docstring"]]
        if undocumented and len(undocumented) <= 3:
            issues.append(Issue(
                Severity.INFO, "documentation",
                f"Functions without docstrings: {undocumented}"
            ))
        
        return score, issues
    
    def _evaluate_naming(self, tree: ast.AST) -> Tuple[float, List[Issue]]:
        """Evaluate naming conventions (PEP8)."""
        issues = []
        
        # Patterns
        snake_case = re.compile(r'^[a-z_][a-z0-9_]*$')
        upper_snake = re.compile(r'^[A-Z_][A-Z0-9_]*$')
        pascal_case = re.compile(r'^[A-Z][a-zA-Z0-9]*$')
        
        violations = 0
        total_names = 0
        
        for node in ast.walk(tree):
            # Function names should be snake_case
            if isinstance(node, ast.FunctionDef):
                total_names += 1
                if not snake_case.match(node.name) and not node.name.startswith("_"):
                    violations += 1
                    if violations <= 2:
                        issues.append(Issue(
                            Severity.MINOR, "naming",
                            f"Function '{node.name}' should be snake_case",
                            node.lineno
                        ))
            
            # Class names should be PascalCase
            elif isinstance(node, ast.ClassDef):
                total_names += 1
                if not pascal_case.match(node.name):
                    violations += 1
                    issues.append(Issue(
                        Severity.MINOR, "naming",
                        f"Class '{node.name}' should be PascalCase",
                        node.lineno
                    ))
            
            # Constants (module-level uppercase) - check assignments
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Module-level uppercase is OK for constants
                        total_names += 1
        
        # Calculate score
        if total_names == 0:
            score = 2.5
        else:
            violation_rate = violations / total_names
            if violation_rate == 0:
                score = 2.5
            elif violation_rate < 0.1:
                score = 2.0
            elif violation_rate < 0.2:
                score = 1.5
            elif violation_rate < 0.3:
                score = 1.0
            else:
                score = 0.5
        
        return score, issues
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 3: BEST PRACTICES (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_best_practices(
        self, 
        code: str, 
        tree: ast.AST, 
        orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate best practices - does it follow conventions?
        
        Components:
        - Configuration patterns (0-3): Proper config handling
        - Operator/Task usage (0-3): Correct operator patterns
        - Dependency patterns (0-2): Clean dependency definition
        - Resource management (0-2): Proper resource handling
        """
        self.logger.info("Evaluating best practices...")
        
        score = 0.0
        details = {}
        issues = []
        
        if orchestrator == Orchestrator.AIRFLOW:
            bp_score, bp_issues = self._evaluate_airflow_best_practices(code, tree)
        elif orchestrator == Orchestrator.PREFECT:
            bp_score, bp_issues = self._evaluate_prefect_best_practices(code, tree)
        elif orchestrator == Orchestrator.DAGSTER:
            bp_score, bp_issues = self._evaluate_dagster_best_practices(code, tree)
        else:
            bp_score, bp_issues = 5.0, []
        
        return EvaluationScore(
            name="best_practices",
            raw_score=min(10.0, bp_score),
            issues=bp_issues,
            details={"orchestrator": orchestrator.value},
        )
    
    def _evaluate_airflow_best_practices(
        self, code: str, tree: ast.AST
    ) -> Tuple[float, List[Issue]]:
        """Evaluate Airflow-specific best practices."""
        score = 0.0
        issues = []
        
        # 1. Configuration patterns (0-3)
        # - Uses default_args
        # - Uses context manager for DAG
        # - Externalized config
        
        if "default_args" in code:
            score += 1.0
        else:
            issues.append(Issue(
                Severity.MINOR, "best_practice",
                "Consider using default_args for common task parameters"
            ))
        
        if "with DAG" in code or "with dag" in code:
            score += 1.0
        elif "DAG(" in code:
            score += 0.5
            issues.append(Issue(
                Severity.INFO, "best_practice",
                "Consider using DAG context manager (with DAG...)"
            ))
        
        if "os.getenv" in code or "os.environ" in code or "Variable.get" in code:
            score += 1.0
        else:
            issues.append(Issue(
                Severity.MINOR, "best_practice",
                "Consider externalizing configuration using environment variables"
            ))
        
        # 2. Operator usage (0-3)
        # - Uses appropriate operators
        # - Sets task_id properly
        # - Uses meaningful names
        
        operator_count = code.count("Operator(")
        if operator_count > 0:
            score += 1.5
        
        if "task_id=" in code:
            score += 1.0
        
        # Check for meaningful task_ids
        task_id_pattern = re.compile(r"task_id=['\"]([^'\"]+)['\"]")
        task_ids = task_id_pattern.findall(code)
        if task_ids:
            # Check if task_ids are descriptive (not just task1, task2)
            generic_names = sum(1 for t in task_ids if re.match(r'^task\d*$', t))
            if generic_names == 0:
                score += 0.5
            else:
                issues.append(Issue(
                    Severity.INFO, "best_practice",
                    "Use descriptive task_id names instead of generic ones"
                ))
        
        # 3. Dependency patterns (0-2)
        if ">>" in code or "<<" in code:
            score += 1.0
            # Check for chain() usage for long chains
            dep_count = code.count(">>") + code.count("<<")
            if dep_count > 5 and "chain(" not in code:
                issues.append(Issue(
                    Severity.INFO, "best_practice",
                    "Consider using chain() for long dependency chains"
                ))
            else:
                score += 0.5
        elif "chain(" in code:
            score += 1.5
        
        # 4. Resource management (0-2)
        if "pool=" in code or "queue=" in code:
            score += 1.0
        
        if "execution_timeout" in code or "timeout" in code:
            score += 0.5
        
        if "trigger_rule" in code:
            score += 0.5
        
        return min(10.0, score), issues
    
    def _evaluate_prefect_best_practices(
        self, code: str, tree: ast.AST
    ) -> Tuple[float, List[Issue]]:
        """Evaluate Prefect-specific best practices."""
        score = 0.0
        issues = []
        
        # 1. Configuration patterns (0-3)
        if "@flow" in code:
            score += 1.5
        
        if "task_runner=" in code:
            score += 0.75
        
        if "os.getenv" in code or "prefect.variables" in code:
            score += 0.75
        
        # 2. Task usage (0-3)
        task_count = code.count("@task")
        if task_count > 0:
            score += 1.5
        
        if "name=" in code:
            score += 0.75
        
        if "description=" in code:
            score += 0.75
        
        # 3. Dependency patterns (0-2)
        # In Prefect, dependencies are implicit through function calls
        # Check for proper flow structure
        if "def " in code and "return" in code:
            score += 1.5
        
        # 4. Resource management (0-2)
        if "retries=" in code:
            score += 1.0
        
        if "retry_delay" in code:
            score += 0.5
        
        if "timeout_seconds" in code:
            score += 0.5
        
        return min(10.0, score), issues
    
    def _evaluate_dagster_best_practices(
        self, code: str, tree: ast.AST
    ) -> Tuple[float, List[Issue]]:
        """Evaluate Dagster-specific best practices."""
        score = 0.0
        issues = []
        
        # 1. Configuration patterns (0-3)
        if "@job" in code:
            score += 1.0
        
        if "@repository" in code:
            score += 1.0
        
        if "config_schema" in code or "OpConfig" in code:
            score += 1.0
        
        # 2. Op usage (0-3)
        op_count = code.count("@op")
        if op_count > 0:
            score += 1.5
        
        if "In(" in code or "Out(" in code:
            score += 1.0
        
        if "description=" in code:
            score += 0.5
        
        # 3. Dependency patterns (0-2)
        # Dagster uses function parameters for dependencies
        if "context:" in code:
            score += 1.0
        
        if "context.log" in code:
            score += 0.5
        
        # 4. Resource management (0-2)
        if "retry_policy" in code or "RetryPolicy" in code:
            score += 1.0
        
        if "required_resource_keys" in code:
            score += 0.5
        
        if "tags=" in code:
            score += 0.5
        
        return min(10.0, score), issues
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 4: MAINTAINABILITY (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_maintainability(self, code: str, tree: ast.AST) -> EvaluationScore:
        """
        Evaluate maintainability - how easy to maintain?
        
        Components:
        - Complexity (0-3): Cyclomatic complexity
        - Modularity (0-2.5): Function/class organization
        - Configuration externalization (0-2): Config separation
        - Code organization (0-2.5): Structure and layout
        """
        self.logger.info("Evaluating maintainability...")
        
        score = 0.0
        details = {}
        issues = []
        
        # 1. Complexity (0-3)
        complexity_score, complexity_issues = self._evaluate_complexity_for_maintainability(code)
        score += complexity_score
        issues.extend(complexity_issues)
        details["complexity_score"] = round(complexity_score, 2)
        
        # 2. Modularity (0-2.5)
        modularity_score, modularity_issues = self._evaluate_modularity(tree)
        score += modularity_score
        issues.extend(modularity_issues)
        details["modularity_score"] = round(modularity_score, 2)
        
        # 3. Configuration externalization (0-2)
        config_score, config_issues = self._evaluate_config_externalization(code)
        score += config_score
        issues.extend(config_issues)
        details["config_externalization_score"] = round(config_score, 2)
        
        # 4. Code organization (0-2.5)
        org_score, org_issues = self._evaluate_code_organization(code, tree)
        score += org_score
        issues.extend(org_issues)
        details["organization_score"] = round(org_score, 2)
        
        return EvaluationScore(
            name="maintainability",
            raw_score=min(10.0, score),
            issues=issues,
            details=details,
        )
    
    def _evaluate_complexity_for_maintainability(
        self, code: str
    ) -> Tuple[float, List[Issue]]:
        """Evaluate complexity using Radon."""
        issues = []
        
        if not RADON_AVAILABLE:
            return 1.5, []
        
        try:
            results = list(cc_visit(code))
            
            if not results:
                return 3.0, []  # No functions = simple
            
            complexities = [r.complexity for r in results]
            avg = sum(complexities) / len(complexities)
            max_c = max(complexities)
            
            # Score based on average complexity
            if avg <= 3:
                score = 3.0
            elif avg <= 5:
                score = 2.5
            elif avg <= 8:
                score = 2.0
            elif avg <= 12:
                score = 1.5
            else:
                score = 0.5
                issues.append(Issue(
                    Severity.MAJOR, "complexity",
                    f"High average complexity: {avg:.1f}"
                ))
            
            # Penalize very complex functions
            if max_c > 15:
                score -= 0.5
                issues.append(Issue(
                    Severity.MAJOR, "complexity",
                    f"Function with very high complexity: {max_c}"
                ))
            
            return max(0.0, score), issues
        
        except Exception:
            return 1.5, []
    
    def _evaluate_modularity(self, tree: ast.AST) -> Tuple[float, List[Issue]]:
        """Evaluate code modularity."""
        issues = []
        
        functions = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        classes = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        
        # Count lines per function
        func_sizes = []
        for func in functions:
            size = func.end_lineno - func.lineno if hasattr(func, 'end_lineno') else 10
            func_sizes.append(size)
        
        score = 0.0
        
        # Has functions (modular)
        if len(functions) > 0:
            score += 1.0
        
        # Functions are reasonably sized
        if func_sizes:
            avg_size = sum(func_sizes) / len(func_sizes)
            if avg_size <= 20:
                score += 1.0
            elif avg_size <= 40:
                score += 0.5
            else:
                issues.append(Issue(
                    Severity.MINOR, "modularity",
                    f"Large functions detected (avg {avg_size:.0f} lines)"
                ))
        else:
            score += 0.5
        
        # Not too many things in global scope
        module_body = tree.body if hasattr(tree, 'body') else []
        global_assignments = sum(1 for n in module_body if isinstance(n, ast.Assign))
        
        if global_assignments <= 10:
            score += 0.5
        else:
            issues.append(Issue(
                Severity.INFO, "modularity",
                f"Many global assignments ({global_assignments})"
            ))
        
        return min(2.5, score), issues
    
    def _evaluate_config_externalization(self, code: str) -> Tuple[float, List[Issue]]:
        """Evaluate configuration externalization."""
        issues = []
        score = 0.0
        
        # Environment variables
        if "os.getenv" in code or "os.environ" in code:
            score += 1.0
        
        # Airflow Variables
        if "Variable.get" in code:
            score += 0.5
        
        # No hardcoded passwords/tokens
        sensitive_patterns = [
            r'password\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][^"\']+["\']',
            r'secret\s*=\s*["\'][^"\']+["\']',
            r'api_key\s*=\s*["\'][^"\']+["\']',
        ]
        
        hardcoded_secrets = False
        for pattern in sensitive_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                hardcoded_secrets = True
                break
        
        if not hardcoded_secrets:
            score += 0.5
        else:
            issues.append(Issue(
                Severity.MAJOR, "config",
                "Possible hardcoded secrets detected"
            ))
        
        return min(2.0, score), issues
    
    def _evaluate_code_organization(
        self, code: str, tree: ast.AST
    ) -> Tuple[float, List[Issue]]:
        """Evaluate code organization and layout."""
        issues = []
        score = 0.0
        lines = code.splitlines()
        
        # Has imports at top
        first_non_comment_line = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                first_non_comment_line = i
                break
        
        # Check if early lines are imports
        early_imports = any(
            "import" in lines[i] 
            for i in range(first_non_comment_line, min(len(lines), first_non_comment_line + 10))
        )
        if early_imports:
            score += 1.0
        
        # Has header comment
        if lines and lines[0].strip().startswith("#"):
            score += 0.5
        
        # Reasonable line lengths
        long_lines = sum(1 for line in lines if len(line) > 120)
        if long_lines == 0:
            score += 0.5
        elif long_lines < 5:
            score += 0.25
        else:
            issues.append(Issue(
                Severity.INFO, "organization",
                f"{long_lines} lines exceed 120 characters"
            ))
        
        # Has sections/comments
        comment_lines = sum(1 for line in lines if line.strip().startswith("#"))
        comment_ratio = comment_lines / len(lines) if lines else 0
        if comment_ratio >= 0.05:  # At least 5% comments
            score += 0.5
        
        return min(2.5, score), issues
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 5: ROBUSTNESS (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_robustness(
        self, 
        code: str, 
        tree: ast.AST, 
        file_path: Path,
        orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate robustness - how resilient to failures?
        
        Components:
        - Error handling (0-3): try/except usage
        - Retry configuration (0-2.5): Retry setup
        - Timeout configuration (0-2): Timeout setup
        - Security (0-2.5): Security issues
        """
        self.logger.info("Evaluating robustness...")
        
        score = 0.0
        details = {}
        issues = []
        
        # 1. Error handling (0-3)
        error_score, error_issues = self._evaluate_error_handling(tree)
        score += error_score
        issues.extend(error_issues)
        details["error_handling_score"] = round(error_score, 2)
        
        # 2. Retry configuration (0-2.5)
        retry_score, retry_issues = self._evaluate_retry_config(code, orchestrator)
        score += retry_score
        issues.extend(retry_issues)
        details["retry_score"] = round(retry_score, 2)
        
        # 3. Timeout configuration (0-2)
        timeout_score, timeout_issues = self._evaluate_timeout_config(code, orchestrator)
        score += timeout_score
        issues.extend(timeout_issues)
        details["timeout_score"] = round(timeout_score, 2)
        
        # 4. Security (0-2.5)
        security_score, security_issues = self._evaluate_security(code, file_path)
        score += security_score
        issues.extend(security_issues)
        details["security_score"] = round(security_score, 2)
        
        return EvaluationScore(
            name="robustness",
            raw_score=min(10.0, score),
            issues=issues,
            details=details,
        )
    
    def _evaluate_error_handling(self, tree: ast.AST) -> Tuple[float, List[Issue]]:
        """Evaluate error handling patterns."""
        issues = []
        
        try_blocks = [n for n in ast.walk(tree) if isinstance(n, ast.Try)]
        functions = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        
        # Score based on try/except presence
        if len(try_blocks) == 0:
            if len(functions) > 0:
                score = 1.0
                issues.append(Issue(
                    Severity.INFO, "error_handling",
                    "Consider adding try/except for error handling"
                ))
            else:
                score = 1.5  # No functions, might be OK
        else:
            # Check quality of exception handling
            score = 2.0
            
            bare_excepts = sum(
                1 for t in try_blocks 
                for h in t.handlers 
                if h.type is None
            )
            
            if bare_excepts == 0:
                score += 1.0
            else:
                issues.append(Issue(
                    Severity.MINOR, "error_handling",
                    f"Avoid bare 'except:' clauses ({bare_excepts} found)"
                ))
        
        return min(3.0, score), issues
    
    def _evaluate_retry_config(
        self, code: str, orchestrator: Orchestrator
    ) -> Tuple[float, List[Issue]]:
        """Evaluate retry configuration."""
        issues = []
        score = 0.0
        
        retry_patterns = {
            Orchestrator.AIRFLOW: ["retries=", "retry_delay=", "retry_exponential_backoff"],
            Orchestrator.PREFECT: ["retries=", "retry_delay_seconds="],
            Orchestrator.DAGSTER: ["retry_policy", "RetryPolicy", "max_retries"],
        }
        
        patterns = retry_patterns.get(orchestrator, ["retries=", "retry"])
        
        for pattern in patterns:
            if pattern in code:
                score += 0.8
        
        if score == 0:
            issues.append(Issue(
                Severity.INFO, "robustness",
                "Consider configuring retry behavior"
            ))
        
        return min(2.5, score), issues
    
    def _evaluate_timeout_config(
        self, code: str, orchestrator: Orchestrator
    ) -> Tuple[float, List[Issue]]:
        """Evaluate timeout configuration."""
        issues = []
        score = 0.0
        
        timeout_patterns = {
            Orchestrator.AIRFLOW: ["execution_timeout", "timeout=", "dagrun_timeout"],
            Orchestrator.PREFECT: ["timeout_seconds=", "timeout="],
            Orchestrator.DAGSTER: ["timeout="],
        }
        
        patterns = timeout_patterns.get(orchestrator, ["timeout"])
        
        for pattern in patterns:
            if pattern in code:
                score += 1.0
        
        if score == 0:
            issues.append(Issue(
                Severity.INFO, "robustness",
                "Consider configuring timeouts"
            ))
        else:
            score = min(2.0, score)
        
        return score, issues
    
    def _evaluate_security(self, code: str, file_path: Path) -> Tuple[float, List[Issue]]:
        """Evaluate security using Bandit."""
        issues = []
        
        if not BANDIT_AVAILABLE:
            return 1.25, []
        
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False
            ) as f:
                f.write(code)
                temp_path = f.name
            
            b_mgr = bandit_manager.BanditManager(
                bandit_config.BanditConfig(), None
            )
            b_mgr.discover_files([temp_path], recursive=False)
            b_mgr.run_tests()
            
            bandit_issues = b_mgr.get_issue_list()
            
            high = sum(1 for i in bandit_issues if i.severity == bandit.HIGH)
            medium = sum(1 for i in bandit_issues if i.severity == bandit.MEDIUM)
            low = sum(1 for i in bandit_issues if i.severity == bandit.LOW)
            
            # Score calculation
            if high == 0 and medium == 0 and low == 0:
                score = 2.5
            elif high == 0 and medium == 0:
                score = 2.0
            elif high == 0:
                score = 1.5
            else:
                score = 0.5
            
            # Add issues
            for bi in bandit_issues[:3]:
                issues.append(Issue(
                    Severity.CRITICAL if bi.severity == bandit.HIGH else Severity.MAJOR,
                    "security",
                    f"[{bi.test_id}] {bi.text}",
                    bi.lineno,
                    tool="bandit"
                ))
            
            return score, issues
        
        except Exception:
            return 1.25, []
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)