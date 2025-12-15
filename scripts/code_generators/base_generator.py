"""
Base generator class defining the interface for all code generation strategies.
All strategies (template, LLM, hybrid) inherit from this base.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
import logging
import yaml
import json  # NEW: needed for tojson filter

from jinja2 import Environment, FileSystemLoader, select_autoescape


@dataclass
class GenerationResult:
    """Result of a code generation attempt."""
    strategy: str                          # 'template', 'llm', 'hybrid'
    orchestrator: str                      # 'airflow', 'prefect', 'dagster'
    success: bool
    code: Optional[str] = None             # Generated code
    output_path: Optional[Path] = None     # Where code was saved
    generation_time_ms: float = 0.0        # Time taken
    token_usage: Dict[str, int] = field(default_factory=dict)  # For LLM strategies
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'strategy': self.strategy,
            'orchestrator': self.orchestrator,
            'success': self.success,
            'output_path': str(self.output_path) if self.output_path else None,
            'generation_time_ms': self.generation_time_ms,
            'token_usage': self.token_usage,
            'warnings': self.warnings,
            'errors': self.errors,
            'metadata': self.metadata
        }


@dataclass
class IntermediateYAML:
    """Parsed intermediate YAML structure."""
    metadata: Dict[str, Any]
    schedule: Dict[str, Any]
    connections: List[Dict[str, Any]]
    tasks: List[Dict[str, Any]]
    raw_data: Dict[str, Any]
    
    @property
    def orchestrator(self) -> str:
        return self.metadata.get('target_orchestrator', 'unknown')
    
    @property
    def pipeline_name(self) -> str:
        return self.metadata.get('pipeline_name', 'unnamed_pipeline')
    
    @property
    def pipeline_description(self) -> str:
        return self.metadata.get('pipeline_description', '')
    
    @property
    def orchestrator_specific(self) -> Dict[str, Any]:
        return self.metadata.get('orchestrator_specific', {})
    
    def get_task_order(self) -> List[str]:
        """Get tasks in execution order based on dependencies."""
        # Build dependency graph
        task_deps = {t['task_id']: set(t.get('upstream_task_ids', [])) for t in self.tasks}
        
        # Topological sort
        ordered = []
        remaining = set(task_deps.keys())
        
        while remaining:
            # Find tasks with no unprocessed dependencies
            ready = [t for t in remaining if task_deps[t].issubset(set(ordered))]
            if not ready:
                # Circular dependency or error; pick one arbitrarily
                ready = [list(remaining)[0]]
            ordered.extend(sorted(ready))
            remaining -= set(ready)
        
        return ordered
    
    def detect_pattern(self) -> str:
        """Detect the structural pattern of the pipeline."""
        if not self.tasks:
            return 'empty'
        
        # Analyze task dependencies
        task_count = len(self.tasks)
        upstream_counts = [len(t.get('upstream_task_ids', [])) for t in self.tasks]
        downstream_counts = {}
        
        for task in self.tasks:
            for upstream in task.get('upstream_task_ids', []):
                downstream_counts[upstream] = downstream_counts.get(upstream, 0) + 1
        
        # Check for patterns
        has_multiple_downstreams = any(c > 1 for c in downstream_counts.values())
        has_multiple_upstreams = any(c > 1 for c in upstream_counts)
        
        # Check for sensors
        has_sensors = any(
            t.get('config', {}).get('sensor_config') or 
            'sensor' in t.get('operator_class', '').lower()
            for t in self.tasks
        )
        
        # Check for branches
        has_branches = any(
            t.get('config', {}).get('branch_config') or
            'branch' in t.get('operator_class', '').lower()
            for t in self.tasks
        )
        
        # Determine pattern
        if has_branches:
            return 'branching'
        elif has_sensors:
            return 'sensor_driven'
        elif has_multiple_downstreams and has_multiple_upstreams:
            return 'fanout_fanin'
        elif has_multiple_downstreams:
            return 'fanout'
        elif has_multiple_upstreams:
            return 'fanin'
        else:
            return 'sequential'


class BaseCodeGenerator(ABC):
    """
    Abstract base class for code generators.
    All generation strategies must implement this interface.
    """
    
    STRATEGY_NAME: str = "base"  # Override in subclasses
    
    def __init__(
        self,
        templates_dir: Path,
        output_dir: Path,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize generator.
        
        Args:
            templates_dir: Root directory containing templates
            output_dir: Base output directory
            config: Optional configuration overrides
        """
        self.templates_dir = Path(templates_dir)
        self.output_dir = Path(output_dir)
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize Jinja2 environment (used by template and hybrid)
        self.jinja_env = self._setup_jinja_environment()
    
    def _setup_jinja_environment(self) -> Environment:
        """Setup Jinja2 environment with custom filters."""
        env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
            extensions=['jinja2.ext.do'],  # Enable {% do %} and related behavior
        )
        
        # Add custom filters
        env.filters['repr'] = repr
        env.filters['to_python_dict'] = self._to_python_dict
        env.filters['to_python_list'] = self._to_python_list
        env.filters['snake_case'] = self._to_snake_case
        env.filters['format_env_dict'] = self._format_env_dict
        env.filters['format_timedelta'] = self._format_timedelta
        env.filters['schedule_repr'] = self._schedule_repr

        # NEW: JSON serializer for templates that use |tojson
        env.filters['tojson'] = lambda v: json.dumps(v)

        return env
    
    # --- Custom Jinja2 Filters ---
    
    @staticmethod
    def _to_python_dict(value: Dict) -> str:
        """Convert dict to Python dict literal."""
        if not value:
            return '{}'
        lines = ['{']
        for k, v in value.items():
            if isinstance(v, str):
                lines.append(f"    {repr(k)}: {repr(v)},")
            else:
                lines.append(f"    {repr(k)}: {v!r},")
        lines.append('}')
        return '\n'.join(lines)
    
    @staticmethod
    def _to_python_list(value: List) -> str:
        """Convert list to Python list literal."""
        if not value:
            return '[]'
        return repr(value)
    
    @staticmethod
    def _to_snake_case(value: str) -> str:
        """Convert string to snake_case."""
        import re
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', value)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower().replace('-', '_').replace(' ', '_')
    
    @staticmethod
    def _format_env_dict(value: Dict) -> str:
        """Format environment variables dict."""
        if not value:
            return '{}'
        lines = ['{']
        for k, v in value.items():
            # Handle environment variable references
            if isinstance(v, str) and v.startswith('os.getenv'):
                lines.append(f"        {repr(k)}: {v},")
            else:
                lines.append(f"        {repr(k)}: {repr(v)},")
        lines.append('    }')
        return '\n'.join(lines)
    
    @staticmethod
    def _format_timedelta(seconds: int) -> str:
        """Format seconds as timedelta."""
        if seconds <= 0:
            return 'timedelta(seconds=0)'
        if seconds % 3600 == 0:
            return f'timedelta(hours={seconds // 3600})'
        if seconds % 60 == 0:
            return f'timedelta(minutes={seconds // 60})'
        return f'timedelta(seconds={seconds})'
    
    @staticmethod
    def _schedule_repr(value: Optional[str]) -> str:
        """Format schedule expression."""
        if value is None:
            return 'None'
        return repr(value)
    
    # --- Core Interface Methods ---
    
    @abstractmethod
    def generate(
        self,
        intermediate_yaml: IntermediateYAML,
        **kwargs
    ) -> GenerationResult:
        """
        Generate code from intermediate YAML.
        
        Args:
            intermediate_yaml: Parsed intermediate YAML
            **kwargs: Additional generation options
            
        Returns:
            GenerationResult with generated code and metadata
        """
        pass
    
    def load_intermediate_yaml(self, yaml_path: Path) -> IntermediateYAML:
        """Load and parse intermediate YAML file."""
        self.logger.info(f"Loading intermediate YAML: {yaml_path}")
        
        with open(yaml_path, 'r', encoding='utf-8') as f:
            raw_data = yaml.safe_load(f)
        
        return IntermediateYAML(
            metadata=raw_data.get('metadata', {}),
            schedule=raw_data.get('schedule', {}),
            connections=raw_data.get('connections', []),
            tasks=raw_data.get('tasks', []),
            raw_data=raw_data
        )
    
    def save_code(
        self,
        code: str,
        intermediate_yaml: IntermediateYAML,
        suffix: str = ""
    ) -> Path:
        """
        Save generated code to file.
        
        Args:
            code: Generated code string
            intermediate_yaml: Source YAML data
            suffix: Optional filename suffix
            
        Returns:
            Path to saved file
        """
        orchestrator = intermediate_yaml.orchestrator
        pipeline_name = intermediate_yaml.pipeline_name
        
        # Build output path: outputs_step3/{orchestrator}/{strategy}/
        output_subdir = self.output_dir / orchestrator / self.STRATEGY_NAME
        output_subdir.mkdir(parents=True, exist_ok=True)
        
        # Filename
        filename = f"{pipeline_name}_{self.STRATEGY_NAME}{suffix}.py"
        output_path = output_subdir / filename
        
        self.logger.info(f"Saving generated code to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(code)
        
        return output_path
    
    def get_template_path(self, orchestrator: str, template_name: str) -> Path:
        """Get path to a template file."""
        return self.templates_dir / orchestrator / template_name
    
    def render_template(
        self,
        orchestrator: str,
        template_name: str,
        context: Dict[str, Any]
    ) -> str:
        """
        Render a Jinja2 template.
        
        Args:
            orchestrator: Target orchestrator (airflow, prefect, dagster)
            template_name: Name of template file
            context: Template context variables
            
        Returns:
            Rendered template string
        """
        template_path = f"{orchestrator}/{template_name}"
        self.logger.debug(f"Rendering template: {template_path}")
        
        try:
            template = self.jinja_env.get_template(template_path)
            return template.render(**context)
        except Exception as e:
            self.logger.error(f"Template rendering failed: {e}")
            raise
    
    def build_base_context(self, intermediate_yaml: IntermediateYAML) -> Dict[str, Any]:
        """Build base context dict from intermediate YAML."""
        return {
            # Metadata
            'pipeline_name': intermediate_yaml.pipeline_name,
            'pipeline_description': intermediate_yaml.pipeline_description,
            'orchestrator': intermediate_yaml.orchestrator,
            'generation_timestamp': datetime.now().isoformat(),
            'generator_strategy': self.STRATEGY_NAME,
            
            # Schedule
            'schedule': intermediate_yaml.schedule,
            'schedule_enabled': intermediate_yaml.schedule.get('enabled', False),
            'schedule_expression': intermediate_yaml.schedule.get('schedule_expression'),
            'timezone': intermediate_yaml.schedule.get('timezone', 'UTC'),
            'catchup': intermediate_yaml.schedule.get('catchup', False),
            
            # Connections
            'connections': intermediate_yaml.connections,
            
            # Tasks
            'tasks': intermediate_yaml.tasks,
            'task_order': intermediate_yaml.get_task_order(),
            'task_count': len(intermediate_yaml.tasks),
            
            # Pattern info
            'detected_pattern': intermediate_yaml.detect_pattern(),
            
            # Orchestrator-specific
            'orchestrator_specific': intermediate_yaml.orchestrator_specific,
        }