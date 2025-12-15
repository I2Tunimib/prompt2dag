"""
Semantic evaluator for comparing generated code against reference descriptions.
This is the KEY differentiator between Direct prompting and structured approaches.
"""

import ast
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from evaluators.base_evaluator import (
    BaseEvaluator,
    EvaluationResult,
    EvaluationScore,
    GateCheckResult,
    Issue,
    Severity,
    Orchestrator,
    extract_key_terms,
    extract_action_verbs,
)


class SemanticEvaluator(BaseEvaluator):
    """
    Evaluates semantic accuracy of generated code against reference.
    
    This evaluator answers: "Does the generated code do what was asked?"
    
    Dimensions:
    1. Task Coverage (0-10): Are all required tasks present?
    2. Dependency Accuracy (0-10): Are task dependencies correct?
    3. Configuration Accuracy (0-10): Are configurations correct?
    4. Semantic Alignment (0-10): Does the code match the description?
    """
    
    EVALUATION_TYPE = "semantic_analysis"
    
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        reference_description: Optional[str] = None,
        intermediate_yaml: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(config, **kwargs)
        self.reference_description = reference_description
        self.intermediate_yaml = intermediate_yaml
        
        # Extract reference data
        self.expected_tasks: List[Dict] = []
        self.expected_task_ids: Set[str] = set()
        self.expected_dependencies: List[Tuple[str, str]] = []
        self.expected_configurations: Dict[str, Any] = {}
        self.description_terms: Set[str] = set()
        self.description_actions: Set[str] = set()
        
        if intermediate_yaml:
            self._parse_intermediate_yaml(intermediate_yaml)
        
        if reference_description:
            self._parse_description(reference_description)
    
    def set_reference(
        self,
        description: Optional[str] = None,
        intermediate_yaml: Optional[Dict[str, Any]] = None
    ):
        """Set or update reference data."""
        if description:
            self.reference_description = description
            self._parse_description(description)
        
        if intermediate_yaml:
            self.intermediate_yaml = intermediate_yaml
            self._parse_intermediate_yaml(intermediate_yaml)
    
    def _parse_intermediate_yaml(self, yaml_data: Dict[str, Any]):
        """Parse intermediate YAML to extract expected elements."""
        # Extract tasks
        self.expected_tasks = yaml_data.get("tasks", [])
        self.expected_task_ids = {t.get("task_id", "") for t in self.expected_tasks}
        
        # Extract dependencies
        self.expected_dependencies = []
        for task in self.expected_tasks:
            task_id = task.get("task_id", "")
            for upstream in task.get("upstream_task_ids", []):
                self.expected_dependencies.append((upstream, task_id))
        
        # Extract configurations
        self.expected_configurations = {
            "schedule": yaml_data.get("schedule", {}),
            "connections": yaml_data.get("connections", []),
            "metadata": yaml_data.get("metadata", {}),
        }
    
    def _parse_description(self, description: str):
        """Parse description to extract key terms and actions."""
        self.description_terms = extract_key_terms(description)
        self.description_actions = extract_action_verbs(description)
    
    def evaluate(self, file_path: Path) -> EvaluationResult:
        """Run semantic evaluation."""
        self.logger.info(f"Running semantic evaluation on: {file_path}")
        
        try:
            code = file_path.read_text(encoding="utf-8")
        except Exception as e:
            return self._error_result(file_path, f"Failed to read file: {e}")
        
        orchestrator = self.detect_orchestrator(code)
        
        # Parse AST
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return self._syntax_error_result(file_path, e, orchestrator)
        
        result = EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=orchestrator,
            timestamp=datetime.now().isoformat(),
            metadata={
                "has_reference_description": self.reference_description is not None,
                "has_intermediate_yaml": self.intermediate_yaml is not None,
                "expected_task_count": len(self.expected_task_ids),
                "expected_dependency_count": len(self.expected_dependencies),
            },
            reference_data={
                "expected_task_ids": list(self.expected_task_ids),
                "expected_dependencies": self.expected_dependencies,
                "description_terms_count": len(self.description_terms),
                "description_actions": list(self.description_actions),
            }
        )
        
        # Gate checks
        result.gate_checks = self._run_gate_checks(code, tree)
        
        if not result.gates_passed:
            # Apply zero scores if gates fail
            for dim in ["task_coverage", "dependency_accuracy", 
                        "configuration_accuracy", "semantic_alignment"]:
                result.scores[dim] = EvaluationScore(
                    name=dim, raw_score=0.0, weight=self.get_weight(dim)
                )
            return result
        
        # Run semantic evaluations
        result.scores["task_coverage"] = self._evaluate_task_coverage(code, tree, orchestrator)
        result.scores["dependency_accuracy"] = self._evaluate_dependency_accuracy(code, tree, orchestrator)
        result.scores["configuration_accuracy"] = self._evaluate_configuration_accuracy(code, tree, orchestrator)
        result.scores["semantic_alignment"] = self._evaluate_semantic_alignment(code, tree)
        
        # Apply penalties
        self.apply_penalties_to_result(result)
        
        return result
    
    def _error_result(self, file_path: Path, error: str) -> EvaluationResult:
        """Create error result."""
        result = EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=Orchestrator.UNKNOWN,
            timestamp=datetime.now().isoformat(),
        )
        result.gate_checks.append(GateCheckResult(
            name="file_readable",
            passed=False,
            message=error,
            is_critical=True,
        ))
        return result
    
    def _syntax_error_result(
        self, file_path: Path, error: SyntaxError, orchestrator: Orchestrator
    ) -> EvaluationResult:
        """Create syntax error result."""
        result = EvaluationResult(
            evaluation_type=self.EVALUATION_TYPE,
            file_path=str(file_path),
            orchestrator=orchestrator,
            timestamp=datetime.now().isoformat(),
        )
        result.gate_checks.append(GateCheckResult(
            name="syntax_valid",
            passed=False,
            message=f"Syntax error: {error.msg} at line {error.lineno}",
            is_critical=True,
        ))
        return result
    
    def _run_gate_checks(self, code: str, tree: ast.AST) -> List[GateCheckResult]:
        """Run gate checks."""
        gates = []
        
        # Gate 1: Has code
        gates.append(GateCheckResult(
            name="has_code",
            passed=len(code.strip()) > 0,
            message="Code is not empty" if code.strip() else "Code is empty",
            is_critical=True,
        ))
        
        # Gate 2: Has expected structure
        has_structure = self._has_pipeline_structure(code)
        gates.append(GateCheckResult(
            name="has_pipeline_structure",
            passed=has_structure,
            message="Pipeline structure detected" if has_structure else "No pipeline structure found",
            is_critical=True,
        ))
        
        return gates
    
    def _has_pipeline_structure(self, code: str) -> bool:
        """Check if code has basic pipeline structure."""
        # Check for any orchestrator patterns
        patterns = [
            r"DAG\s*\(", r"@dag", r"@flow", r"@task", r"@job", r"@op",
            r"Operator\s*\(", r"def\s+\w+\s*\(",
        ]
        return any(re.search(p, code, re.IGNORECASE) for p in patterns)
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 1: TASK COVERAGE (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_task_coverage(
        self, code: str, tree: ast.AST, orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate task coverage - are all required tasks present?
        
        This is CRITICAL for differentiating approaches.
        Direct prompting often misses tasks or uses wrong names.
        """
        issues = []
        details = {
            "expected_tasks": list(self.expected_task_ids),
            "found_tasks": [],
            "missing_tasks": [],
            "extra_tasks": [],
            "coverage_ratio": 0.0,
        }
        
        # Extract tasks from generated code
        found_tasks = self._extract_task_ids_from_code(code, orchestrator)
        details["found_tasks"] = list(found_tasks)
        
        if not self.expected_task_ids:
            # No reference - give partial credit
            score = 5.0 if found_tasks else 2.0
            details["note"] = "No expected tasks specified"
            return EvaluationScore(
                name="task_coverage",
                raw_score=score,
                weight=self.get_weight("task_coverage"),
                issues=issues,
                details=details,
            )
        
        # Calculate coverage
        matched_tasks = self.expected_task_ids & found_tasks
        missing_tasks = self.expected_task_ids - found_tasks
        extra_tasks = found_tasks - self.expected_task_ids
        
        details["missing_tasks"] = list(missing_tasks)
        details["extra_tasks"] = list(extra_tasks)
        
        coverage_ratio = len(matched_tasks) / len(self.expected_task_ids)
        details["coverage_ratio"] = round(coverage_ratio, 3)
        
        # Calculate score based on coverage
        if coverage_ratio == 1.0:
            score = 10.0
        elif coverage_ratio >= 0.9:
            score = 9.0
        elif coverage_ratio >= 0.8:
            score = 8.0
        elif coverage_ratio >= 0.7:
            score = 7.0
        elif coverage_ratio >= 0.5:
            score = 5.0
        elif coverage_ratio >= 0.3:
            score = 3.0
        else:
            score = 1.0
        
        # Add issues for missing tasks (CRITICAL penalty)
        for task_id in missing_tasks:
            issues.append(Issue(
                severity=Severity.CRITICAL,
                category="task_coverage",
                message=f"Missing required task: {task_id}",
                penalty=2.0,  # Heavy penalty per missing task
            ))
        
        # Add issues for extra tasks (MINOR - informational)
        if extra_tasks:
            issues.append(Issue(
                severity=Severity.INFO,
                category="task_coverage",
                message=f"Extra tasks not in specification: {extra_tasks}",
                penalty=0.0,
            ))
        
        return EvaluationScore(
            name="task_coverage",
            raw_score=score,
            weight=self.get_weight("task_coverage"),
            issues=issues,
            details=details,
        )
    
    def _extract_task_ids_from_code(
        self, code: str, orchestrator: Orchestrator
    ) -> Set[str]:
        """Extract task IDs from generated code."""
        task_ids = set()
        
        if orchestrator == Orchestrator.AIRFLOW:
            # Pattern: task_id='...' or task_id="..."
            pattern = r"task_id\s*=\s*['\"]([^'\"]+)['\"]"
            task_ids.update(re.findall(pattern, code))
            
            # Pattern: variable = SomeOperator(
            pattern2 = r"(\w+)\s*=\s*\w*Operator\s*\("
            task_ids.update(re.findall(pattern2, code))
        
        elif orchestrator == Orchestrator.PREFECT:
            # Pattern: @task decorated functions
            pattern = r"@task[^)]*\)\s*\ndef\s+(\w+)"
            task_ids.update(re.findall(pattern, code, re.MULTILINE))
            
            # Pattern: @task with name parameter
            pattern2 = r'@task\s*\([^)]*name\s*=\s*["\']([^"\']+)["\']'
            task_ids.update(re.findall(pattern2, code))
            
            # Simpler pattern for @task\ndef
            pattern3 = r"@task\s*\n\s*def\s+(\w+)"
            task_ids.update(re.findall(pattern3, code))
        
        elif orchestrator == Orchestrator.DAGSTER:
            # Pattern: @op decorated functions
            pattern = r"@op[^)]*\)\s*\ndef\s+(\w+)"
            task_ids.update(re.findall(pattern, code, re.MULTILINE))
            
            # Simpler pattern
            pattern2 = r"@op\s*\n\s*def\s+(\w+)"
            task_ids.update(re.findall(pattern2, code))
        
        # Also check for expected task IDs mentioned anywhere
        for expected_id in self.expected_task_ids:
            if expected_id in code:
                task_ids.add(expected_id)
        
        return task_ids
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 2: DEPENDENCY ACCURACY (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_dependency_accuracy(
        self, code: str, tree: ast.AST, orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate dependency accuracy - are task dependencies correct?
        
        This is CRITICAL for semantic correctness.
        Wrong dependencies = wrong execution order = wrong results.
        """
        issues = []
        details = {
            "expected_dependencies": self.expected_dependencies,
            "found_dependencies": [],
            "correct_dependencies": [],
            "missing_dependencies": [],
            "wrong_dependencies": [],
            "accuracy_ratio": 0.0,
        }
        
        # Extract dependencies from code
        found_deps = self._extract_dependencies_from_code(code, orchestrator)
        details["found_dependencies"] = found_deps
        
        if not self.expected_dependencies:
            # No reference - give partial credit
            score = 5.0 if found_deps else 3.0
            details["note"] = "No expected dependencies specified"
            return EvaluationScore(
                name="dependency_accuracy",
                raw_score=score,
                weight=self.get_weight("dependency_accuracy"),
                issues=issues,
                details=details,
            )
        
        # Compare dependencies
        expected_set = set(self.expected_dependencies)
        found_set = set(found_deps)
        
        correct_deps = expected_set & found_set
        missing_deps = expected_set - found_set
        wrong_deps = found_set - expected_set
        
        details["correct_dependencies"] = list(correct_deps)
        details["missing_dependencies"] = list(missing_deps)
        details["wrong_dependencies"] = list(wrong_deps)
        
        # Calculate accuracy
        if len(expected_set) > 0:
            accuracy = len(correct_deps) / len(expected_set)
        else:
            accuracy = 1.0 if not found_deps else 0.5
        
        details["accuracy_ratio"] = round(accuracy, 3)
        
        # Calculate score
        if accuracy == 1.0 and not wrong_deps:
            score = 10.0
        elif accuracy >= 0.9:
            score = 9.0 - (len(wrong_deps) * 0.5)
        elif accuracy >= 0.8:
            score = 8.0 - (len(wrong_deps) * 0.5)
        elif accuracy >= 0.6:
            score = 6.0
        elif accuracy >= 0.4:
            score = 4.0
        else:
            score = 2.0
        
        score = max(0.0, score)
        
        # Add issues
        for dep in missing_deps:
            issues.append(Issue(
                severity=Severity.MAJOR,
                category="dependency_accuracy",
                message=f"Missing dependency: {dep[0]} → {dep[1]}",
                penalty=1.5,
            ))
        
        for dep in wrong_deps:
            issues.append(Issue(
                severity=Severity.MAJOR,
                category="dependency_accuracy",
                message=f"Unexpected dependency: {dep[0]} → {dep[1]}",
                penalty=1.0,
            ))
        
        return EvaluationScore(
            name="dependency_accuracy",
            raw_score=score,
            weight=self.get_weight("dependency_accuracy"),
            issues=issues,
            details=details,
        )
    
    def _extract_dependencies_from_code(
        self, code: str, orchestrator: Orchestrator
    ) -> List[Tuple[str, str]]:
        """Extract task dependencies from code."""
        deps = []
        
        if orchestrator == Orchestrator.AIRFLOW:
            # Pattern: task1 >> task2
            pattern = r"(\w+)\s*>>\s*(\w+)"
            for match in re.finditer(pattern, code):
                deps.append((match.group(1), match.group(2)))
            
            # Pattern: task2 << task1
            pattern2 = r"(\w+)\s*<<\s*(\w+)"
            for match in re.finditer(pattern2, code):
                deps.append((match.group(2), match.group(1)))
            
            # Pattern: chain(t1, t2, t3)
            chain_pattern = r"chain\s*\(\s*([^)]+)\)"
            for match in re.finditer(chain_pattern, code):
                tasks = [t.strip() for t in match.group(1).split(",")]
                for i in range(len(tasks) - 1):
                    deps.append((tasks[i], tasks[i + 1]))
        
        elif orchestrator == Orchestrator.PREFECT:
            # In Prefect, dependencies are implicit through function calls
            # Look for patterns like: result = task1(); task2(result)
            # This is harder to detect statically
            pass
        
        elif orchestrator == Orchestrator.DAGSTER:
            # In Dagster, dependencies are through function parameters
            # Look for op calls within job definitions
            pass
        
        return deps
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 3: CONFIGURATION ACCURACY (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_configuration_accuracy(
        self, code: str, tree: ast.AST, orchestrator: Orchestrator
    ) -> EvaluationScore:
        """
        Evaluate configuration accuracy - are configurations correct?
        """
        issues = []
        details = {
            "schedule_configured": False,
            "environment_vars_found": [],
            "connection_refs_found": [],
            "configuration_checks": {},
        }
        
        score = 5.0  # Base score
        
        # Check schedule configuration
        if self._has_schedule_config(code, orchestrator):
            details["schedule_configured"] = True
            score += 1.0
        
        # Check environment variables
        env_vars = self._extract_environment_vars(code)
        details["environment_vars_found"] = list(env_vars)
        
        # Compare with expected environment from intermediate YAML
        if self.intermediate_yaml:
            expected_env = set()
            for task in self.expected_tasks:
                config = task.get("config", {})
                env = config.get("environment", {})
                expected_env.update(env.keys())
                
                # Also check nested structures
                if "infrastructure" in config:
                    infra_env = config["infrastructure"].get("config", {}).get("env", {})
                    expected_env.update(infra_env.keys())
            
            if expected_env:
                env_coverage = len(env_vars & expected_env) / len(expected_env)
                details["configuration_checks"]["env_coverage"] = round(env_coverage, 3)
                score += 2.0 * env_coverage
                
                missing_env = expected_env - env_vars
                if missing_env:
                    issues.append(Issue(
                        severity=Severity.MINOR,
                        category="configuration",
                        message=f"Missing environment variables: {missing_env}",
                        penalty=0.5,
                    ))
        
        # Check for hardcoded secrets (MAJOR issue)
        if self._has_hardcoded_secrets(code):
            score -= 2.0
            issues.append(Issue(
                severity=Severity.MAJOR,
                category="configuration",
                message="Potential hardcoded secrets detected",
                penalty=2.0,
            ))
        else:
            score += 1.0
            details["configuration_checks"]["no_hardcoded_secrets"] = True
        
        # Check connection references
        conn_refs = self._extract_connection_refs(code)
        details["connection_refs_found"] = list(conn_refs)
        
        score = max(0.0, min(10.0, score))
        
        return EvaluationScore(
            name="configuration_accuracy",
            raw_score=score,
            weight=self.get_weight("configuration_accuracy"),
            issues=issues,
            details=details,
        )
    
    def _has_schedule_config(self, code: str, orchestrator: Orchestrator) -> bool:
        """Check if code has schedule configuration."""
        patterns = [
            r"schedule[_interval]*\s*=",
            r"schedule\s*:",
            r"cron\s*[=:]",
            r"@daily",
            r"@hourly",
            r"timedelta\s*\(",
        ]
        return any(re.search(p, code, re.IGNORECASE) for p in patterns)
    
    def _extract_environment_vars(self, code: str) -> Set[str]:
        """Extract environment variable references."""
        env_vars = set()
        
        # Pattern: os.getenv('VAR') or os.environ['VAR']
        patterns = [
            r"os\.getenv\s*\(\s*['\"]([^'\"]+)['\"]",
            r"os\.environ\s*\[\s*['\"]([^'\"]+)['\"]",
            r"os\.environ\.get\s*\(\s*['\"]([^'\"]+)['\"]",
        ]
        
        for pattern in patterns:
            env_vars.update(re.findall(pattern, code))
        
        # Pattern: 'VAR': value in environment dict
        env_dict_pattern = r"['\"]([A-Z_][A-Z0-9_]*)['\"]:\s*"
        if "environment" in code.lower() or "env" in code.lower():
            env_vars.update(re.findall(env_dict_pattern, code))
        
        return env_vars
    
    def _extract_connection_refs(self, code: str) -> Set[str]:
        """Extract connection references."""
        conn_refs = set()
        
        patterns = [
            r"conn_id\s*=\s*['\"]([^'\"]+)['\"]",
            r"connection_id\s*=\s*['\"]([^'\"]+)['\"]",
        ]
        
        for pattern in patterns:
            conn_refs.update(re.findall(pattern, code))
        
        return conn_refs
    
    def _has_hardcoded_secrets(self, code: str) -> bool:
        """Check for potential hardcoded secrets."""
        secret_patterns = [
            r"password\s*=\s*['\"][^'\"]{4,}['\"]",
            r"api_key\s*=\s*['\"][^'\"]{8,}['\"]",
            r"token\s*=\s*['\"][^'\"]{8,}['\"]",
            r"secret\s*=\s*['\"][^'\"]{4,}['\"]",
        ]
        
        for pattern in secret_patterns:
            matches = re.findall(pattern, code, re.IGNORECASE)
            for match in matches:
                # Check if it's not a placeholder
                if not any(p in match.lower() for p in [
                    "{{", "your_", "placeholder", "xxx", "***", "<", ">"
                ]):
                    return True
        
        return False
    
    # ═══════════════════════════════════════════════════════════════════════
    # DIMENSION 4: SEMANTIC ALIGNMENT (0-10)
    # ═══════════════════════════════════════════════════════════════════════
    def _evaluate_semantic_alignment(
        self, code: str, tree: ast.AST
    ) -> EvaluationScore:
        """
        Evaluate semantic alignment with description.
        
        Does the generated code reflect the intent of the original description?
        """
        issues = []
        details = {
            "term_coverage": 0.0,
            "action_coverage": 0.0,
            "terms_found": [],
            "terms_missing": [],
            "actions_found": [],
            "actions_missing": [],
        }
        
        if not self.description_terms and not self.description_actions:
            # No description reference
            return EvaluationScore(
                name="semantic_alignment",
                raw_score=5.0,
                weight=self.get_weight("semantic_alignment"),
                issues=issues,
                details={"note": "No reference description provided"},
            )
        
        code_lower = code.lower()
        
        # Check term coverage
        if self.description_terms:
            terms_found = {t for t in self.description_terms if t in code_lower}
            terms_missing = self.description_terms - terms_found
            
            term_coverage = len(terms_found) / len(self.description_terms)
            details["term_coverage"] = round(term_coverage, 3)
            details["terms_found"] = list(terms_found)[:20]  # Limit output
            details["terms_missing"] = list(terms_missing)[:10]
        else:
            term_coverage = 0.5
        
        # Check action coverage
        if self.description_actions:
            actions_found = {a for a in self.description_actions if a in code_lower}
            actions_missing = self.description_actions - actions_found
            
            action_coverage = len(actions_found) / len(self.description_actions)
            details["action_coverage"] = round(action_coverage, 3)
            details["actions_found"] = list(actions_found)
            details["actions_missing"] = list(actions_missing)
        else:
            action_coverage = 0.5
        
        # Calculate score (weighted average)
        score = (term_coverage * 4.0) + (action_coverage * 6.0)
        
        # Add issues for low coverage
        if term_coverage < 0.5:
            issues.append(Issue(
                severity=Severity.MAJOR,
                category="semantic_alignment",
                message=f"Low term coverage: {term_coverage*100:.1f}% of description terms found in code",
                penalty=2.0,
            ))
        
        if action_coverage < 0.5:
            issues.append(Issue(
                severity=Severity.MAJOR,
                category="semantic_alignment",
                message=f"Low action coverage: {action_coverage*100:.1f}% of expected actions found",
                penalty=1.5,
            ))
        
        return EvaluationScore(
            name="semantic_alignment",
            raw_score=score,
            weight=self.get_weight("semantic_alignment"),
            issues=issues,
            details=details,
        )