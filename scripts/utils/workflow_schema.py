# scripts/utils/workflow_schema.py
from __future__ import annotations # <<<< ADD THIS LINE AT THE TOP

"""
Pydantic models for validating the workflow YAML structure from Step 2.
These models enforce structure, types, and presence of required fields.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, List, Any, Optional, Union, Literal

# --- Reusable Sub-Models ---

from typing import Literal
from pydantic import BaseModel

class ParallelBody(BaseModel):
    entry_points: list[str] = []
    tasks: dict[str, 'WorkflowTask'] = {}

class FlowConstructBase(BaseModel):
    type: str
    depends_on: list[str] = []
    triggers: list[str] = []

class ParallelForEachConstruct(FlowConstructBase):
    type: Literal['parallel_for_each'] = 'parallel_for_each'
    instance_parameter: str  # Required field
    body: ParallelBody

# Union type for all possible constructs
FlowConstructType = ParallelForEachConstruct  # Add other types as needed


# Now, the order doesn't matter for type hints because evaluation is postponed
class ParameterDefinition(BaseModel):
    """Defines the schema for a single parameter."""
    description: Optional[str] = None
    type: Optional[str] = 'string'  # Default type if not specified
    default: Any = None
    required: bool = False
    constraints: Optional[str] = None

# ... (rest of ParameterDefinition, EnvironmentVariableDefinition, etc.)

class EnvironmentVariableDefinition(BaseModel):
    """Defines the schema for an environment variable."""
    description: Optional[str] = None
    default: Any = None
    associated_component_id: Optional[str] = None

class IntegrationConnection(BaseModel):
    """Details for connecting to an integration point."""
    url: Optional[str] = None
    port: Optional[int] = None
    protocol: Optional[str] = None
    path: Optional[str] = None  # For FileSystem
    name: Optional[str] = None  # For Docker Network
    host: Optional[str] = None  # For Database

class IntegrationAuthentication(BaseModel):
    """Authentication details for an integration point."""
    type: Optional[str] = None
    credentials_source: Optional[str] = None
    token: Optional[str] = None  # Specific field seen in example

class IntegrationPoint(BaseModel):
    """Defines a single external integration point."""
    id: str
    name: Optional[str] = None
    type: Optional[str] = None
    connection: Optional[IntegrationConnection] = None
    authentication: Optional[IntegrationAuthentication] = None
    components: List[str] = Field(default_factory=list)
    direction: Optional[str] = None

class WorkflowTask(BaseModel):
    """Represents a single task node within the main workflow or a parallel body."""
    component_type_id: str
    name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    depends_on: List[str] = Field(default_factory=list)
    triggers: List[str] = Field(default_factory=list)

class ParallelBody(BaseModel):
    """Defines the structure of the sub-workflow within a parallel construct."""
    entry_points: List[str] = Field(default_factory=list)
    tasks: Dict[str, WorkflowTask] = Field(default_factory=dict)

    @model_validator(mode='after')
    def check_entry_points_exist(self) -> 'ParallelBody':
        defined_task_ids = set(self.tasks.keys())
        for entry_id in self.entry_points:
            if entry_id not in defined_task_ids:
                raise ValueError(f"Parallel body entry point '{entry_id}' not found in its defined tasks: {defined_task_ids}")
        return self

class FlowConstructBase(BaseModel):
    """Base model for different types of flow constructs."""
    type: str
    depends_on: List[str] = Field(default_factory=list)
    triggers: List[str] = Field(default_factory=list)

class ParallelForEachConstruct(FlowConstructBase):
    """Defines a parallel execution block."""
    type: Literal['parallel_for_each'] = 'parallel_for_each'
    instance_parameter: Optional[str] = None
    iteration_target: Optional[str] = None
    body: ParallelBody

FlowConstructType = Union[ParallelForEachConstruct]

class Workflow(BaseModel):
    """Defines the execution graph of the pipeline."""
    entry_points: List[str] = Field(default_factory=list)
    # Now Python knows these are just annotations to be evaluated later
    tasks: Dict[str, WorkflowTask] = Field(default_factory=dict)
    flow_constructs: Dict[str, FlowConstructType] = Field(default_factory=dict)

    @model_validator(mode='after')
    def check_node_references(self) -> 'Workflow':
        # (validation logic is fine, but access depends_on/triggers correctly)
        all_task_ids = set(self.tasks.keys())
        all_construct_ids = set(self.flow_constructs.keys())
        all_valid_ids = all_task_ids | all_construct_ids

        # Check task dependencies
        for task_id, task_data in self.tasks.items():
            for dep_id in task_data.depends_on: # Access attribute directly
                if dep_id not in all_valid_ids:
                    raise ValueError(f"Task '{task_id}' has invalid depends_on target: '{dep_id}'")
            for trig_id in task_data.triggers: # Access attribute directly
                if trig_id not in all_valid_ids:
                    raise ValueError(f"Task '{task_id}' has invalid triggers target: '{trig_id}'")

        # Check flow construct dependencies
        for construct_id, construct_data in self.flow_constructs.items():
            for dep_id in construct_data.depends_on: # Access attribute directly
                if dep_id not in all_valid_ids:
                    raise ValueError(f"Flow construct '{construct_id}' has invalid depends_on target: '{dep_id}'")
            for trig_id in construct_data.triggers: # Access attribute directly
                if trig_id not in all_valid_ids:
                    raise ValueError(f"Flow construct '{construct_id}' has invalid triggers target: '{trig_id}'")

        # Check entry points exist
        for entry_id in self.entry_points:
            if entry_id not in all_valid_ids:
                raise ValueError(f"Workflow entry point '{entry_id}' not found in defined tasks or constructs.")

        return self


# --- Top-Level Models ---
# (Metadata, ExecutionEnvironment, Parameters, ComponentType, Integrations definitions remain the same)

class Metadata(BaseModel):
    analysis_version: str
    timestamp: str
    source_description_file: Optional[str] = None
    llm_provider: Optional[str] = None
    llm_model_key: Optional[str] = None

class ExecutionEnvironment(BaseModel):
    type: str
    default_docker_network: Optional[str] = None
    schedule_interval: Optional[str] = None

class Parameters(BaseModel):
    global_params: Dict[str, ParameterDefinition] = Field(default_factory=dict, alias='global')
    component_type_defaults: Dict[str, Dict[str, ParameterDefinition]] = Field(default_factory=dict, alias='components')
    environment_variables: Dict[str, EnvironmentVariableDefinition] = Field(default_factory=dict)

class ComponentType(BaseModel):
    id: str
    name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    image: Optional[str] = None
    is_internally_parallelized: bool = False

class Integrations(BaseModel):
    integration_points: List[IntegrationPoint] = Field(default_factory=list)
    data_sources: List[str] = Field(default_factory=list)
    data_sinks: List[str] = Field(default_factory=list)


class PipelineYAML(BaseModel):
    """Root model for validating the entire Step 2 YAML structure."""
    pipeline_definition_version: str
    pipeline_name: str
    description: Optional[str] = None
    metadata: Metadata
    execution_environment: ExecutionEnvironment
    parameters: Parameters = Field(default_factory=Parameters)
    component_types: List[ComponentType] = Field(default_factory=list)
    integrations: Integrations = Field(default_factory=Integrations)
    workflow: Workflow # Workflow is now defined when this is encountered

    @model_validator(mode='after')
    def check_component_references(self) -> 'PipelineYAML':
        defined_component_type_ids = {comp.id for comp in self.component_types}
        # Check workflow tasks
        for task_id, task_data in self.workflow.tasks.items():
            if task_data.component_type_id not in defined_component_type_ids:
                raise ValueError(f"Workflow task '{task_id}' references unknown component_type_id: '{task_data.component_type_id}'")
        # Check tasks within parallel bodies
        for block_id, block_data in self.workflow.flow_constructs.items():
            if isinstance(block_data, ParallelForEachConstruct):
                for sub_task_id, sub_task_data in block_data.body.tasks.items():
                     if sub_task_data.component_type_id not in defined_component_type_ids:
                          raise ValueError(f"Parallel task '{sub_task_id}' in block '{block_id}' refs unknown component_type_id: '{sub_task_data.component_type_id}'")
        return self