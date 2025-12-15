"""
Pydantic models for Step 1 output validation and Step 2 intermediate representation.
Ensures type safety and validates orchestrator-agnostic pipeline definitions.

Compatible with Pydantic v2.x
"""

from typing import List, Dict, Optional, Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import logging

# Setup module logger
logger = logging.getLogger("prompt2dag_schema")

# ============================================================================
# Step 1 Output Schema (Orchestrator-Agnostic)
# ============================================================================

class ExecutorConfig(BaseModel):
    """Configuration for how a component executes."""
    image: Optional[str] = None
    command: Optional[List[str]] = None
    entry_point: Optional[str] = None
    script_path: Optional[str] = None
    environment: Dict[str, str] = Field(default_factory=dict)
    resources: Dict[str, Any] = Field(default_factory=dict)
    network: Optional[str] = None

    # RELAXED: allow null/None for resources and environment and coerce to {}
    @field_validator('resources', mode='before')
    @classmethod
    def normalize_resources(cls, v):
        """
        Accept None/null for resources and normalize to an empty dict.
        """
        if v is None:
            logger.debug("ExecutorConfig.resources is None; normalizing to {}")
            return {}
        return v

    @field_validator('environment', mode='before')
    @classmethod
    def normalize_environment(cls, v):
        """
        Accept None/null for environment and normalize to an empty dict.
        """
        if v is None:
            logger.debug("ExecutorConfig.environment is None; normalizing to {}")
            return {}
        return v


class IOSpec(BaseModel):
    """Input/Output specification for a component."""
    name: str
    direction: Literal["input", "output"]
    kind: Literal["file", "table", "object", "api", "stream"]
    format: str
    path_pattern: Optional[str] = None
    connection_id: Optional[str] = None

    @field_validator('path_pattern', mode='before')
    @classmethod
    def normalize_path_pattern(cls, v):
        """
        Accept None/null for path_pattern and normalize to an empty string.
        """
        if v is None:
            return ""
        return v


class UpstreamPolicy(BaseModel):
    """Defines how a component waits for upstream dependencies."""
    type: Literal["all_success", "any_success", "all_done", "one_success", "none_failed", "custom"]
    description: str = ""
    timeout_seconds: Optional[int] = None


class RetryPolicy(BaseModel):
    """Retry behavior for failed components."""
    max_attempts: int = 0
    delay_seconds: int = 0
    exponential_backoff: bool = False
    retry_on: List[str] = Field(default_factory=list)


class Concurrency(BaseModel):
    """Concurrency and parallelism settings."""
    supports_parallelism: bool = False
    supports_dynamic_mapping: bool = False
    map_over_param: Optional[str] = None
    max_parallel_instances: Optional[int] = None


class Connection(BaseModel):
    """Connection reference in a component."""
    id: str
    type: str
    purpose: str


class Datasets(BaseModel):
    """Dataset lineage for a component."""
    consumes: List[str] = Field(default_factory=list)
    produces: List[str] = Field(default_factory=list)

    @field_validator('consumes', 'produces', mode='before')
    @classmethod
    def normalize_lists(cls, v):
        """
        Accept None/null for dataset lists and normalize to [].
        """
        if v is None:
            return []
        return v


class Component(BaseModel):
    """A reusable component type (task definition)."""
    id: str = Field(..., description="Unique component identifier")
    name: str

    # RELAXED: allow arbitrary category strings instead of strict Literal list
    category: str

    description: str = ""
    inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)
    executor_type: Literal[
        "python", "docker", "kubernetes", "bash", "sql",
        "spark", "http", "custom"
    ]
    executor_config: ExecutorConfig = Field(default_factory=ExecutorConfig)
    io_spec: List[IOSpec] = Field(default_factory=list)
    upstream_policy: UpstreamPolicy = Field(default_factory=lambda: UpstreamPolicy(type="all_success"))
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    concurrency: Concurrency = Field(default_factory=Concurrency)
    connections: List[Connection] = Field(default_factory=list)
    datasets: Datasets = Field(default_factory=Datasets)

    # RELAXED: accept both List[str] and List[dict]/List[IOSpec] for inputs/outputs
    @field_validator('inputs', 'outputs', mode='before')
    @classmethod
    def normalize_io(cls, v):
        """
        Normalize inputs/outputs to a list of string names.

        Acceptable input formats:
          - ["table_data_json", "reconciled_table_json"]
          - [{"name": "table_data_json", ...}, {"name": "reconciled_table_json", ...}]
          - [IOSpec(...), IOSpec(...)]
        """
        if v is None:
            return []

        if not isinstance(v, list):
            # Let Pydantic handle errors if it's not a list
            return v

        normalized: List[str] = []
        for item in v:
            if isinstance(item, str):
                normalized.append(item)
            elif isinstance(item, dict):
                name = item.get("name")
                if not name:
                    raise ValueError(f"IO spec dict missing 'name': {item}")
                normalized.append(name)
            elif isinstance(item, IOSpec):
                normalized.append(item.name)
            else:
                raise TypeError(f"Unsupported IO type {type(item)} in inputs/outputs: {item}")

        logger.debug(f"Normalized IO list: {normalized}")
        return normalized


# ===================== RELAXED ADVANCED NODE CONFIGS ========================

class BranchConfig(BaseModel):
    """
    Configuration for branching nodes.

    RELAXED:
    - `type` is a free-form string (e.g., "conditional", "branch_merge", etc.).
    - `branches` is a list of arbitrary dicts (label/condition/next_node or more).
    """
    type: Optional[str] = "conditional"
    branches: List[Dict[str, Any]] = Field(default_factory=list)


class SensorConfig(BaseModel):
    """
    Configuration for sensor nodes.

    RELAXED:
    - `sensor_type` is a free-form string (e.g., "file", "s3_key_sensor", "external_task").
    - `mode` is a free-form string ("poke", "reschedule", or others).
    """
    sensor_type: Optional[str] = None
    target: str
    poke_interval_seconds: int = 60
    timeout_seconds: int = 3600
    mode: Optional[str] = "poke"


class ParallelConfig(BaseModel):
    """
    Configuration for parallel execution nodes.

    RELAXED:
    - All fields optional so partial parallel configs from the LLM don't break validation.
    """
    type: Optional[str] = None            # e.g., "dynamic_map", "static_parallel", "fan_out_fan_in"
    map_over: Optional[str] = None
    map_source: Optional[str] = None
    max_parallelism: Optional[int] = None
    join_node: Optional[str] = None


class FlowNode(BaseModel):
    """A node in the execution flow."""
    kind: Literal["Task", "Branch", "Sensor", "Parallel", "Join", "Virtual"]
    component_type_id: Optional[str] = None
    upstream_policy: UpstreamPolicy = Field(default_factory=lambda: UpstreamPolicy(type="all_success"))
    next_nodes: List[str] = Field(default_factory=list)
    branch_config: Optional[BranchConfig] = None
    sensor_config: Optional[SensorConfig] = None
    parallel_config: Optional[ParallelConfig] = None

    # NORMALIZE: tolerate variations in kind values (e.g. "task", "branch_node")
    @field_validator('kind', mode='before')
    @classmethod
    def normalize_kind(cls, v):
        if v is None:
            return "Task"
        v_str = str(v).strip().lower()
        mapping = {
            "task": "Task",
            "task_node": "Task",
            "branch": "Branch",
            "branch_node": "Branch",
            "branch_task": "Branch",
            "sensor": "Sensor",
            "sensor_node": "Sensor",
            "file_sensor": "Sensor",
            "parallel": "Parallel",
            "parallel_node": "Parallel",
            "join": "Join",
            "join_node": "Join",
            "virtual": "Virtual",
            "virtual_node": "Virtual",
        }
        if v_str in mapping:
            return mapping[v_str]
        logger.warning(f"Unknown node kind '{v}', defaulting to 'Task'")
        return "Task"

    model_config = {
        "extra": "allow"  # Allow extra fields
    }


class FlowEdge(BaseModel):
    """An edge connecting two nodes in the flow."""
    from_node: str = Field(..., alias="from")
    to: str
    edge_type: Literal["success", "failure", "always", "conditional"] = "success"
    condition: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {
        "populate_by_name": True,
        "extra": "allow"
    }


class FlowStructure(BaseModel):
    """Complete flow definition."""
    pattern: Literal[
        "sequential", "parallel", "branching",
        "sensor_driven", "event_driven", "hybrid"
    ]
    entry_points: List[str]
    nodes: Dict[str, FlowNode]
    edges: List[FlowEdge]

    # NORMALIZE: map arbitrary pattern names (branch_merge, fan_out_fan_in, etc.) into canonical ones
    @field_validator('pattern', mode='before')
    @classmethod
    def normalize_pattern(cls, v):
        if v is None:
            return "sequential"
        v_str = str(v).strip().lower()

        allowed = {
            "sequential", "parallel", "branching",
            "sensor_driven", "event_driven", "hybrid"
        }
        if v_str in allowed:
            return v_str

        # Map common synthetic / descriptive patterns
        if "branch" in v_str:
            # e.g., branch_merge, branching_tree
            return "branching"
        if "fan_out" in v_str or "fanout" in v_str or "fan-in" in v_str or "fan_in" in v_str or "fanin" in v_str:
            return "parallel"
        if "sensor" in v_str or "gated" in v_str:
            return "sensor_driven"
        if "staged" in v_str or "multi_stage" in v_str or "multi-stage" in v_str:
            return "hybrid"
        if "event" in v_str:
            return "event_driven"

        logger.warning(f"Unknown flow pattern '{v}', defaulting to 'sequential'")
        return "sequential"

    @model_validator(mode='after')
    def validate_structure(self):
        """Validate flow structure consistency."""
        # Check nodes exist
        if not self.nodes:
            logger.error("Flow structure has NO nodes defined!")
            return self
        
        logger.info(f"Flow structure validated: {len(self.nodes)} nodes, {len(self.edges)} edges")
        
        # Check entry points
        missing_entry = [ep for ep in self.entry_points if ep not in self.nodes]
        if missing_entry:
            logger.warning(f"Entry points {missing_entry} not found in {list(self.nodes.keys())}")
        
        # Check edges reference valid nodes
        for edge in self.edges:
            if edge.from_node not in self.nodes:
                logger.warning(f"Edge 'from' references unknown node: {edge.from_node}")
            if edge.to not in self.nodes:
                logger.warning(f"Edge 'to' references unknown node: {edge.to}")
        
        return self

    model_config = {
        "extra": "allow"
    }


class ParameterSpec(BaseModel):
    """Parameter definition."""
    description: str = ""
    type: str
    default: Any = None
    required: bool = False
    constraints: Optional[str] = None
    format: Optional[str] = None


class Parameters(BaseModel):
    """All pipeline parameters."""
    pipeline: Dict[str, ParameterSpec] = Field(default_factory=dict)
    schedule: Dict[str, ParameterSpec] = Field(default_factory=dict)
    execution: Dict[str, ParameterSpec] = Field(default_factory=dict)
    components: Dict[str, Dict[str, ParameterSpec]] = Field(default_factory=dict)
    environment: Dict[str, ParameterSpec] = Field(default_factory=dict)


class RateLimit(BaseModel):
    """Rate limiting configuration."""
    requests_per_second: Optional[int] = None
    burst: Optional[int] = None


class IntegrationConnection(BaseModel):
    """External system connection."""
    id: str
    name: str
    type: Literal[
        "filesystem", "database", "api", "object_storage",
        "message_queue", "data_warehouse", "cache", "other"
    ]
    config: Dict[str, Any]
    authentication: Dict[str, Any] = Field(default_factory=dict)
    used_by_components: List[str] = Field(default_factory=list)
    direction: Literal["input", "output", "both"]
    rate_limit: Optional[RateLimit] = None
    datasets: Datasets = Field(default_factory=Datasets)

    # NORMALIZE: map arbitrary connection types ("ftp", "email", "network") into allowed set
    @field_validator('type', mode='before')
    @classmethod
    def normalize_type(cls, v):
        if v is None:
            return "other"
        v_str = str(v).strip().lower()
        allowed = {
            "filesystem", "database", "api", "object_storage",
            "message_queue", "data_warehouse", "cache", "other"
        }
        if v_str in allowed:
            return v_str

        # heuristic mapping
        if "ftp" in v_str or "sftp" in v_str or "nfs" in v_str or "fs" in v_str:
            return "filesystem"
        if "db" in v_str or "database" in v_str or "postgres" in v_str or "mysql" in v_str or "mongo" in v_str:
            return "database"
        if "queue" in v_str or "kafka" in v_str or "mq" in v_str:
            return "message_queue"
        if "bucket" in v_str or "s3" in v_str or "gcs" in v_str or "blob" in v_str:
            return "object_storage"
        if "warehouse" in v_str or "snowflake" in v_str or "bigquery" in v_str or "redshift" in v_str:
            return "data_warehouse"
        if "cache" in v_str or "redis" in v_str or "memcached" in v_str:
            return "cache"
        if "api" in v_str or "http" in v_str or "rest" in v_str:
            return "api"

        logger.warning(f"Unknown integration type '{v}', defaulting to 'other'")
        return "other"

    @field_validator('config', 'authentication', mode='before')
    @classmethod
    def normalize_config_and_auth(cls, v):
        """
        Accept None/null for config/authentication and normalize to {}.
        """
        if v is None:
            return {}
        return v


class DataLineage(BaseModel):
    """Data lineage information."""
    sources: List[str] = Field(default_factory=list)
    sinks: List[str] = Field(default_factory=list)
    intermediate_datasets: List[str] = Field(default_factory=list)


class Integrations(BaseModel):
    """External integrations."""
    connections: List[IntegrationConnection] = Field(default_factory=list)
    data_lineage: DataLineage = Field(default_factory=DataLineage)


class AnalysisResults(BaseModel):
    """Metadata about the analysis."""
    detected_patterns: List[str]
    task_executors_used: List[str]
    has_branching: bool = False
    has_parallelism: bool = False
    has_sensors: bool = False
    total_components: int
    complexity_score: Literal["low", "medium", "high"]


class OrchestratorCompatibility(BaseModel):
    """Compatibility assessment for an orchestrator."""
    supported: bool
    confidence: Literal["low", "medium", "high"]
    notes: List[str] = Field(default_factory=list)


class Metadata(BaseModel):
    """Analysis metadata."""
    schema_version: str
    analysis_timestamp: datetime
    source_file: str
    llm_provider: str
    llm_model: str
    analysis_results: AnalysisResults
    orchestrator_compatibility: Dict[str, OrchestratorCompatibility]
    validation_warnings: List[str] = Field(default_factory=list)


class PipelineSummary(BaseModel):
    """High-level pipeline summary."""
    name: str
    description: str
    flow_patterns: List[str]
    task_executors: List[str]
    complexity: Literal["low", "medium", "high"]


class Step1Output(BaseModel):
    """Complete Step 1 output schema (orchestrator-agnostic)."""
    metadata: Metadata
    pipeline_summary: PipelineSummary
    components: List[Component]
    flow_structure: FlowStructure
    parameters: Parameters
    integrations: Integrations

    @model_validator(mode='after')
    def validate_component_references(self):
        """Log warnings for mismatched component references (non-blocking)."""
        component_ids = {c.id for c in self.components}
        
        # Check if flow_structure has nodes (should be parsed by now)
        if not self.flow_structure.nodes:
            logger.error("Step1Output validation: flow_structure.nodes is empty!")
            return self
        
        issues = []
        for node_id, node in self.flow_structure.nodes.items():
            if node.kind == "Task" and node.component_type_id:
                if node.component_type_id not in component_ids:
                    issues.append(f"Node '{node_id}' → unknown component '{node.component_type_id}'")
        
        if issues:
            logger.warning(f"Component reference issues: {'; '.join(issues)}")
        else:
            logger.info(f"All {len(self.flow_structure.nodes)} task nodes reference valid components")
        
        return self

    model_config = {
        "extra": "allow"
    }

# ============================================================================
# Step 2 Intermediate Schema (Orchestrator-Specific Prep)
# ============================================================================

class OrchestratorTask(BaseModel):
    """Orchestrator-specific task representation."""
    task_id: str
    task_name: str
    operator_class: str
    operator_module: str
    component_ref: str
    config: Dict[str, Any] = Field(default_factory=dict)
    upstream_task_ids: List[str] = Field(default_factory=list)
    trigger_rule: Optional[str] = None
    retries: int = 0
    retry_delay_seconds: int = 0

    # Validation warnings for code generation
    validation_warnings: List[str] = Field(default_factory=list)
    
    model_config = {
        "extra": "allow"
    }


class OrchestratorConnection(BaseModel):
    """Orchestrator-specific connection."""
    conn_id: str
    conn_type: str
    description: str
    config: Dict[str, Any]


class OrchestratorSchedule(BaseModel):
    """Orchestrator-specific schedule."""
    enabled: bool = False
    schedule_expression: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    timezone: str = "UTC"
    catchup: bool = False


class OrchestratorMetadata(BaseModel):
    """Metadata for orchestrator-specific output."""
    target_orchestrator: Literal["airflow", "prefect", "dagster"]
    generated_at: datetime
    source_analysis_file: str
    pipeline_name: str
    pipeline_description: str
    orchestrator_specific: Dict[str, Any] = Field(default_factory=dict)  # Store extra metadata


class Step2IntermediateYAML(BaseModel):
    """Intermediate YAML structure before code generation."""
    metadata: OrchestratorMetadata
    schedule: OrchestratorSchedule
    connections: List[OrchestratorConnection] = Field(default_factory=list)
    tasks: List[OrchestratorTask]

    model_config = {
        "json_encoders": {
            datetime: lambda v: v.isoformat()
        }
    }

# ============================================================================
# Validation Utilities
# ============================================================================

def validate_step1_output(data: Dict) -> Step1Output:
    """
    Validate Step 1 JSON output against schema.
    
    Args:
        data: Raw dictionary from Step 1 JSON
        
    Returns:
        Validated Step1Output model
        
    Raises:
        ValidationError: If data doesn't match schema
    """
    return Step1Output(**data)


def validate_intermediate_yaml(data: Dict) -> Step2IntermediateYAML:
    """
    Validate Step 2 intermediate YAML against schema.
    
    Args:
        data: Raw dictionary from intermediate YAML
        
    Returns:
        Validated Step2IntermediateYAML model
        
    Raises:
        ValidationError: If data doesn't match schema
    """
    return Step2IntermediateYAML(**data)