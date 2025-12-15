"""
Abstract base class for orchestrator-specific mappers.
Defines the contract that all mappers must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime
import yaml
import logging

from utils.prompt2dag_schema import (
    Step1Output, Step2IntermediateYAML, Component, FlowNode,
    OrchestratorTask, OrchestratorConnection, OrchestratorSchedule,
    OrchestratorMetadata
)

class BaseOrchestratorMapper(ABC):
    """Abstract base class for mapping Step 1 output to orchestrator-specific format."""
    
    def __init__(self, step1_data: Step1Output, mapping_config_path: Path):
        """
        Initialize mapper with validated Step 1 data and mapping config.
        
        Args:
            step1_data: Validated Step 1 output
            mapping_config_path: Path to orchestrator-specific mapping YAML
        """
        # Initialize logger FIRST (before any method calls)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Store data
        self.step1_data = step1_data
        
        # Load mapping config (can now use self.logger)
        self.mapping_config = self._load_mapping_config(mapping_config_path)
        
        # Build lookup maps
        self.component_map = {c.id: c for c in step1_data.components}
        self.node_map = step1_data.flow_structure.nodes
        
        self.logger.info(f"Initialized {self.__class__.__name__} with {len(self.component_map)} components")
        
    def _load_mapping_config(self, config_path: Path) -> Dict:
        """Load and validate mapping configuration."""
        self.logger.info(f"Loading mapping config from {config_path}")
        
        if not config_path.exists():
            raise FileNotFoundError(f"Mapping config not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError(f"Empty or invalid mapping config: {config_path}")
        
        self.logger.debug(f"Loaded mapping config with keys: {list(config.keys())}")
        return config
    
    @abstractmethod
    def get_orchestrator_name(self) -> str:
        """Return the orchestrator name (e.g., 'airflow', 'prefect')."""
        pass
    
    @abstractmethod
    def map_component_to_task(self, component: Component, node: FlowNode) -> OrchestratorTask:
        """
        Map component+node to an orchestrator-specific task representation.
        Subclasses must implement this.
        """
        raise NotImplementedError
    
    @abstractmethod
    def map_upstream_policy(self, policy_type: str) -> str:
        """
        Map orchestrator-agnostic upstream policy to orchestrator-specific rule.
        
        Args:
            policy_type: Upstream policy type from Step 1
            
        Returns:
            Orchestrator-specific trigger rule/policy
        """
        pass
    
    @abstractmethod
    def map_connection(self, connection: Any) -> OrchestratorConnection:
        """
        Map a connection to orchestrator-specific format.
        
        Args:
            connection: Connection from Step 1
            
        Returns:
            Orchestrator-specific connection definition
        """
        pass
    
    @abstractmethod
    def map_schedule(self) -> OrchestratorSchedule:
        """
        Map schedule parameters to orchestrator-specific format.
        
        Returns:
            Orchestrator-specific schedule definition
        """
        pass
    
    def generate_intermediate_yaml(self) -> Step2IntermediateYAML:
        """
        Main orchestration method: convert Step 1 output to intermediate YAML.

        CURRENT SIMPLIFICATION:
        - Treat all concrete nodes (Task, Branch, Sensor, Parallel, Join, Virtual)
          as plain tasks/ops, as long as we can associate them with a component.
        - If component_type_id is missing, we fall back to node_id for lookup.
        """
        self.logger.info(f"Generating intermediate YAML for {self.get_orchestrator_name()}")
        
        # Map metadata
        metadata = OrchestratorMetadata(
            target_orchestrator=self.get_orchestrator_name(),
            generated_at=datetime.now(),
            source_analysis_file=self.step1_data.metadata.source_file,
            pipeline_name=self.step1_data.pipeline_summary.name,
            pipeline_description=self.step1_data.pipeline_summary.description
        )
        
        # Map schedule
        schedule = self.map_schedule()
        
        # Map connections
        connections = [
            self.map_connection(conn) 
            for conn in self.step1_data.integrations.connections
        ]
        
        # Map tasks
        tasks: List[OrchestratorTask] = []
        node_kinds_to_map = {"Task", "Branch", "Sensor", "Parallel", "Join", "Virtual"}
        
        for node_id, node in self.node_map.items():
            if node.kind not in node_kinds_to_map:
                continue
            
            ctid = node.component_type_id
            component: Component | None = None
            
            # Primary: use component_type_id if present
            if ctid and ctid in self.component_map:
                component = self.component_map[ctid]
            # Fallback: use node_id as component id
            elif node_id in self.component_map:
                component = self.component_map[node_id]
                self.logger.debug(
                    f"Node '{node_id}' (kind={node.kind}) has no or unknown "
                    f"component_type_id '{ctid}', using component '{node_id}' as fallback."
                )
            else:
                self.logger.warning(
                    f"Node '{node_id}' (kind={node.kind}) has no matching component "
                    f"(component_type_id='{ctid}'); skipping this node."
                )
                continue
            
            try:
                task = self.map_component_to_task(component, node)
                # Add upstream dependencies
                task.upstream_task_ids = self._get_upstream_task_ids(node_id)
                # Map trigger rule
                task.trigger_rule = self.map_upstream_policy(node.upstream_policy.type)
                tasks.append(task)
                self.logger.debug(f"Mapped task: {task.task_id} (kind={node.kind})")
            except Exception as e:
                self.logger.error(
                    f"Failed to map node '{node_id}' with component '{component.id}': {e}",
                    exc_info=True
                )
                # Continue mapping other nodes instead of aborting entire pipeline
        
        if not tasks:
            self.logger.warning("No tasks generated - check component/node mappings")
        
        # Build intermediate YAML
        intermediate = Step2IntermediateYAML(
            metadata=metadata,
            schedule=schedule,
            connections=connections,
            tasks=tasks
        )
        
        self.logger.info(f"Generated {len(tasks)} tasks for {self.get_orchestrator_name()}")
        return intermediate
    
    def _get_upstream_task_ids(self, node_id: str) -> List[str]:
        """Get list of upstream task IDs for a given node."""
        upstream = []
        for edge in self.step1_data.flow_structure.edges:
            if edge.to == node_id:
                upstream.append(edge.from_node)
        return upstream
    
    def _extract_param_value(self, param_dict: Dict[str, Any], param_name: str, default=None):
        """
        Safely extract value from a dict of ParameterSpec objects.
        
        Args:
            param_dict: Dict mapping param names to ParameterSpec objects
            param_name: Name of parameter to extract
            default: Default value if not found or None
            
        Returns:
            Extracted value or default
        """
        from utils.prompt2dag_schema import ParameterSpec
        
        param_spec = param_dict.get(param_name)
        
        if param_spec is None:
            return default
        
        # Handle both dict and ParameterSpec (Pydantic model)
        if isinstance(param_spec, ParameterSpec):
            value = param_spec.default
        elif isinstance(param_spec, dict):
            value = param_spec.get('default')
        else:
            self.logger.warning(f"Unexpected param type for '{param_name}': {type(param_spec)}")
            return default
        
        return value if value is not None else default

    def _resolve_env_var(self, var_name: str, orchestrator_template: str) -> str:
        """
        Resolve environment variable to orchestrator-specific template syntax.
        
        Args:
            var_name: Variable name (e.g., "DATA_DIR")
            orchestrator_template: Template string with %s placeholder
            
        Returns:
            Orchestrator-specific variable reference
        """
        return orchestrator_template % var_name
    
    def validate_executor_config(self, component: Component) -> Dict[str, Any]:
        """
        Validate and normalize executor config, adding smart defaults.
        
        Args:
            component: Component to validate
            
        Returns:
            Normalized executor config dict with warnings
        """
        config = component.executor_config.model_dump()
        warnings = []
        
        # Docker/Kubernetes validation
        if component.executor_type in ['docker', 'kubernetes']:
            if config.get('image'):
                if not config.get('command'):
                    warnings.append(
                        f"Component '{component.id}': Docker image specified without command. "
                        f"Will use image default ENTRYPOINT/CMD. "
                        f"If this fails at runtime, add explicit command to pipeline description."
                    )
                    # Add placeholder for code generation
                    config['command_placeholder'] = "# Uses image default entrypoint"
            else:
                warnings.append(
                    f"Component '{component.id}': Docker executor without image specification!"
                )
        
        # Python validation
        elif component.executor_type == 'python':
            if not config.get('entry_point') and not config.get('script_path'):
                warnings.append(
                    f"Component '{component.id}': Python executor without entry_point or script_path. "
                    f"Code generation will need manual intervention."
                )
                config['entry_point_placeholder'] = "# TODO: Specify Python callable"
        
        # Bash validation
        elif component.executor_type == 'bash':
            if not config.get('command'):
                warnings.append(
                    f"Component '{component.id}': Bash executor without command!"
                )
                config['command_placeholder'] = "# TODO: Specify bash command"
        
        if warnings:
            for w in warnings:
                self.logger.warning(w)
        
        return config