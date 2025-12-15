"""
Base validator for intermediate YAML.
Provides common validation logic and abstract methods for orchestrator-specific checks.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Tuple
from pathlib import Path
import logging

from utils.prompt2dag_schema import Step2IntermediateYAML, validate_intermediate_yaml

class ValidationResult:
    """Container for validation results."""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        
    def add_error(self, message: str):
        self.errors.append(message)
        
    def add_warning(self, message: str):
        self.warnings.append(message)
        
    def add_info(self, message: str):
        self.info.append(message)
        
    def is_valid(self) -> bool:
        """Returns True if no errors (warnings OK)."""
        return len(self.errors) == 0
    
    def summary(self) -> str:
        """Returns formatted summary."""
        return (
            f"Validation Results:\n"
            f"  Errors: {len(self.errors)}\n"
            f"  Warnings: {len(self.warnings)}\n"
            f"  Info: {len(self.info)}\n"
        )
    
    def format_issues(self) -> str:
        """Returns formatted list of all issues."""
        output = []
        
        if self.errors:
            output.append("\n❌ ERRORS:")
            for i, err in enumerate(self.errors, 1):
                output.append(f"  {i}. {err}")
        
        if self.warnings:
            output.append("\n⚠️  WARNINGS:")
            for i, warn in enumerate(self.warnings, 1):
                output.append(f"  {i}. {warn}")
        
        if self.info:
            output.append("\nℹ️  INFO:")
            for i, info_msg in enumerate(self.info, 1):
                output.append(f"  {i}. {info_msg}")
        
        return "\n".join(output) if output else "\n✅ No issues found"


class BaseValidator(ABC):
    """Abstract base class for orchestrator-specific validators."""
    
    def __init__(self, intermediate_yaml: Step2IntermediateYAML):
        """
        Initialize validator with validated intermediate YAML.
        
        Args:
            intermediate_yaml: Pydantic-validated intermediate YAML object
        """
        self.yaml_data = intermediate_yaml
        self.result = ValidationResult()
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def get_orchestrator_name(self) -> str:
        """Return orchestrator name (airflow, prefect, dagster)."""
        pass
    
    def validate(self) -> ValidationResult:
        """
        Run full validation suite.
        
        Returns:
            ValidationResult with errors, warnings, info
        """
        self.logger.info(f"Starting validation for {self.get_orchestrator_name()}")
        
        # Common validations
        self._validate_metadata()
        self._validate_tasks()
        self._validate_connections()
        self._validate_dependencies()
        
        # Orchestrator-specific validations
        self.validate_orchestrator_specific()
        
        # Summary
        self.logger.info(self.result.summary())
        
        return self.result
    
    def _validate_metadata(self):
        """Validate metadata section."""
        metadata = self.yaml_data.metadata
        
        # Check orchestrator matches
        if metadata.target_orchestrator != self.get_orchestrator_name():
            self.result.add_error(
                f"Target orchestrator mismatch: expected {self.get_orchestrator_name()}, "
                f"got {metadata.target_orchestrator}"
            )
        
        # Check pipeline name
        if not metadata.pipeline_name or metadata.pipeline_name == "unnamed_pipeline":
            self.result.add_warning("Pipeline has no meaningful name")
        
        # Check description
        if metadata.pipeline_description == "No description provided.":
            self.result.add_info("Pipeline has no description")
        
        # Check orchestrator-specific metadata
        if not metadata.orchestrator_specific:
            self.result.add_warning(f"Missing {self.get_orchestrator_name()}-specific metadata")
    
    def _validate_tasks(self):
        """Validate tasks section."""
        tasks = self.yaml_data.tasks
        
        if not tasks:
            self.result.add_error("No tasks defined in pipeline")
            return
        
        self.result.add_info(f"Pipeline has {len(tasks)} tasks")
        
        # Check for duplicate task IDs
        task_ids = [t.task_id for t in tasks]
        duplicates = [tid for tid in task_ids if task_ids.count(tid) > 1]
        if duplicates:
            self.result.add_error(f"Duplicate task IDs found: {set(duplicates)}")
        
        # Validate each task
        for task in tasks:
            self._validate_task(task)
    
    def _validate_task(self, task):
        """Validate individual task."""
        # Check required fields
        if not task.task_id:
            self.result.add_error(f"Task missing task_id: {task}")
        
        if not task.operator_class:
            self.result.add_error(f"Task '{task.task_id}' missing operator_class")
        
        # Check config
        if not task.config:
            self.result.add_warning(f"Task '{task.task_id}' has empty config")
        
        # Check validation warnings
        if task.validation_warnings:
            for warning in task.validation_warnings:
                self.result.add_info(f"Task '{task.task_id}': {warning}")
    
    def _validate_connections(self):
        """Validate connections section."""
        connections = self.yaml_data.connections
        
        if not connections:
            self.result.add_info("No connections defined")
            return
        
        self.result.add_info(f"Pipeline has {len(connections)} connections")
        
        # Check for duplicate connection IDs
        conn_ids = [c.conn_id for c in connections]
        duplicates = [cid for cid in conn_ids if conn_ids.count(cid) > 1]
        if duplicates:
            self.result.add_error(f"Duplicate connection IDs: {set(duplicates)}")
    
    def _validate_dependencies(self):
        """Validate task dependencies form valid DAG."""
        tasks = self.yaml_data.tasks
        task_ids = {t.task_id for t in tasks}
        
        # Check all upstream references are valid
        for task in tasks:
            for upstream_id in task.upstream_task_ids:
                if upstream_id not in task_ids:
                    self.result.add_error(
                        f"Task '{task.task_id}' references unknown upstream task '{upstream_id}'"
                    )
        
        # Check for cycles (basic)
        if self._has_cycles(tasks):
            self.result.add_error("Task dependencies contain cycles (not a valid DAG)")
        
        # Check for orphaned tasks
        entry_tasks = [t for t in tasks if not t.upstream_task_ids]
        if not entry_tasks:
            self.result.add_warning("No entry point tasks found (all tasks have upstream dependencies)")
        elif len(entry_tasks) > 1:
            self.result.add_info(f"Pipeline has {len(entry_tasks)} entry point tasks")
    
    def _has_cycles(self, tasks) -> bool:
        """Simple cycle detection using DFS."""
        task_dict = {t.task_id: t for t in tasks}
        visited = set()
        rec_stack = set()
        
        def dfs(task_id):
            visited.add(task_id)
            rec_stack.add(task_id)
            
            task = task_dict.get(task_id)
            if task:
                for upstream in task.upstream_task_ids:
                    if upstream not in visited:
                        if dfs(upstream):
                            return True
                    elif upstream in rec_stack:
                        return True
            
            rec_stack.remove(task_id)
            return False
        
        for task in tasks:
            if task.task_id not in visited:
                if dfs(task.task_id):
                    return True
        
        return False
    
    @abstractmethod
    def validate_orchestrator_specific(self):
        """Perform orchestrator-specific validations. Must be implemented by subclasses."""
        pass