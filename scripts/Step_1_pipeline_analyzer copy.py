#!/usr/bin/env python3
"""
Pipeline Analysis Tool - Step 1 (Orchestrator-Agnostic)
========================================================
Analyzes pipeline descriptions and produces an orchestrator-neutral
abstraction layer suitable for mapping to Airflow, Prefect, Dagster, etc.

Schema Version: 1.0
"""

import os
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional, Union
import re
import sys
import networkx as nx
import traceback

# --- Add parent directory to Python path for importing utils ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_dir = os.path.dirname(script_dir)
    if project_root_dir not in sys.path:
        sys.path.insert(0, project_root_dir)

    from utils.llm_provider import LLMProvider
    from utils.config_loader import load_config, get_project_root, validate_api_keys
except ImportError as e:
    print(f"FATAL ERROR importing utility modules: {e}")
    sys.exit(1)

# --- Constants (Orchestrator-Neutral) ---
DEFAULT_PIPELINE_CONFIG = {
    "pipeline_settings": {
        "output_directory": "outputs",
    },
    "logging": {
        "level": "INFO",
        "log_file": "outputs/logs/pipeline_analysis_step1.log"
    },
    "analysis": {}
}

# Schema version for orchestrator-agnostic analysis
ANALYSIS_SCHEMA_VERSION = "1.0"

# Execution patterns (instead of environment types)
EXECUTION_PATTERNS = ['sequential', 'parallel', 'branching', 'sensor_driven', 'event_driven', 'hybrid']

# Task executor types (orchestrator-neutral)
TASK_EXECUTORS = ['python', 'docker', 'kubernetes', 'bash', 'sql', 'spark', 'http', 'custom']

# Component categories (semantic, not orchestrator-specific)
SUPPORTED_CATEGORIES = [
    'Extractor', 'Loader', 'Transformer', 'SQLTransform',
    'Reconciliator', 'Enricher', 'QualityCheck', 'Sensor',
    'Splitter', 'Merger', 'Orchestrator', 'Notifier',
    'Aggregator', 'Other'
]

# Upstream policy types (orchestrator-neutral)
UPSTREAM_POLICY_TYPES = [
    'all_success',      # All upstream tasks must succeed
    'any_success',      # At least one upstream succeeds
    'all_done',         # All upstream tasks finish (any state)
    'one_success',      # Exactly one upstream succeeds
    'none_failed',      # No upstream failed (some may be skipped)
    'custom'            # Custom logic
]

# Node kinds (orchestrator-neutral)
NODE_KINDS = ['Task', 'Branch', 'Sensor', 'Parallel', 'Join', 'Virtual']

# Connection types
CONNECTION_TYPES = [
    'filesystem', 'database', 'api', 'object_storage', 
    'message_queue', 'data_warehouse', 'cache', 'other'
]

# Soft scope guardrails for batch ETL
MAX_TOTAL_NODES = 50
MAX_PARALLEL_WIDTH = 10
MAX_BRANCH_DEPTH = 1

# --- Logging Setup ---
def setup_logging(config: Dict):
    """Setup logging based on the provided configuration."""
    try:
        log_config = config.get('logging', DEFAULT_PIPELINE_CONFIG['logging'])
        log_level = log_config.get('level', 'INFO').upper()
        log_file_path = log_config.get('log_file', 'outputs/logs/pipeline_analysis_step1.log')

        if not os.path.isabs(log_file_path):
            project_root = get_project_root()
            log_file_path = os.path.join(project_root, log_file_path)

        log_dir = os.path.dirname(log_file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ],
            force=True
        )
        logging.info(f"Logging setup complete. Level: {log_level}. File: {log_file_path}")
    except Exception as e:
        print(f"FATAL ERROR setting up logging: {e}")
        sys.exit(1)

# --- Helper Functions ---
def clean_and_parse_json(llm_output: str, context: str = "LLM Output") -> Union[Dict, List, None]:
    """Cleans LLM output (removes markdown) and parses it as JSON."""
    if not llm_output:
        logging.warning(f"Received empty LLM output for {context}.")
        return None
    
    content = llm_output.strip()
    
    # Remove markdown code fences
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content, re.IGNORECASE)
    if match:
        content = match.group(1).strip()
    elif (content.startswith('{') and content.endswith('}')) or \
         (content.startswith('[') and content.endswith(']')):
        pass
    else:
        logging.debug(f"Content for {context} doesn't have standard JSON fences or start/end chars.")

    # Remove trailing commas
    content = re.sub(r',(\s*([\]}]))', r'\1', content)

    if not content:
        logging.warning(f"LLM output for {context} was empty after cleaning markdown.")
        return None
    
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON for {context}: {e}")
        log_limit = 2000
        logged_content = content[:log_limit] + ('...' if len(content) > log_limit else '')
        logging.debug(f"Problematic Content (approx first {log_limit} chars):\n{logged_content}")
        return None

def format_components_for_prompt(components_list: List[Dict]) -> str:
    """Formats the component list into a JSON string suitable for LLM prompts."""
    if not components_list:
        return "[]"
    try:
        prompt_components = []
        for comp in components_list:
            prompt_components.append({
                "id": comp.get('id', 'unknown_id'),
                "name": comp.get('name', 'Unknown Name'),
                "category": comp.get('category', 'Other'),
                "executor_type": comp.get('executor_type'),
                "description": (comp.get('description') or 'No description.')[:200]
            })
        return json.dumps(prompt_components, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Could not format components as JSON for prompt: {e}")
        return "[]"

def read_pipeline_description(input_file: str) -> str:
    """Reads pipeline description from file, resolving path relative to project root."""
    if not input_file:
        raise ValueError("Input file path cannot be empty.")
    
    if not os.path.isabs(input_file):
        project_root = get_project_root()
        file_path = os.path.join(project_root, input_file)
    else:
        file_path = input_file
    
    logging.info(f"Attempting to read pipeline description from: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                logging.warning(f"Input file is empty: {file_path}")
            return content
    except FileNotFoundError:
        logging.error(f"Input file not found: {file_path}")
        raise
    except IOError as e:
        logging.error(f"IOError reading input file {file_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error reading input file {file_path}: {e}")
        raise

# --- Normalization and Validation Helpers ---
def normalize_component(comp: Dict, index: int) -> Optional[Dict]:
    """Normalize a single component to ensure required fields and valid values."""
    if not isinstance(comp, dict):
        logging.warning(f"Component at index {index} is not an object. Skipping.")
        return None
    
    cid = comp.get('id')
    name = comp.get('name')
    
    if not cid or not name:
        logging.warning(f"Component at index {index} missing id or name. Skipping.")
        return None
    
    # Normalize category
    category = comp.get('category', 'Other')
    if category not in SUPPORTED_CATEGORIES:
        logging.warning(f"Component '{cid}' has unsupported category '{category}', defaulting to 'Other'")
        category = 'Other'
    comp['category'] = category
    
    # Normalize executor_type
    executor_type = comp.get('executor_type', 'python')
    if executor_type not in TASK_EXECUTORS:
        logging.warning(f"Component '{cid}' has unsupported executor_type '{executor_type}', defaulting to 'custom'")
        executor_type = 'custom'
    comp['executor_type'] = executor_type
    
    # Ensure executor_config exists
    comp.setdefault('executor_config', {})
    exec_cfg = comp['executor_config']
    exec_cfg.setdefault('image', None)
    exec_cfg.setdefault('command', None)
    exec_cfg.setdefault('entry_point', None)
    exec_cfg.setdefault('script_path', None)
    exec_cfg.setdefault('environment', {})
    exec_cfg.setdefault('resources', {})
    exec_cfg.setdefault('network', None)
    
    # Normalize I/O spec
    if not isinstance(comp.get('io_spec'), list):
        comp['io_spec'] = []
    
    # Normalize upstream_policy
    comp.setdefault('upstream_policy', {})
    up_policy = comp['upstream_policy']
    policy_type = up_policy.get('type', 'all_success')
    if policy_type not in UPSTREAM_POLICY_TYPES:
        policy_type = 'all_success'
    up_policy['type'] = policy_type
    up_policy.setdefault('description', '')
    up_policy.setdefault('timeout_seconds', None)
    
    # Normalize retry_policy
    comp.setdefault('retry_policy', {})
    retry = comp['retry_policy']
    retry.setdefault('max_attempts', 0)
    retry.setdefault('delay_seconds', 0)
    retry.setdefault('exponential_backoff', False)
    retry.setdefault('retry_on', [])
    
    # Normalize concurrency
    comp.setdefault('concurrency', {})
    concur = comp['concurrency']
    concur.setdefault('supports_parallelism', False)
    concur.setdefault('supports_dynamic_mapping', False)
    concur.setdefault('map_over_param', None)
    concur.setdefault('max_parallel_instances', None)
    
    # Normalize connections
    if not isinstance(comp.get('connections'), list):
        comp['connections'] = []
    
    # Normalize datasets
    comp.setdefault('datasets', {'consumes': [], 'produces': []})
    ds = comp['datasets']
    ds.setdefault('consumes', [])
    ds.setdefault('produces', [])
    
    # Ensure basic fields
    comp.setdefault('description', '')
    comp.setdefault('inputs', [])
    comp.setdefault('outputs', [])
    
    return comp

def normalize_flow_structure(flow_structure: Dict) -> Dict:
    """
    Normalize flow structure to ensure presence of edges, default kinds, and upstream_policy defaults.
    """
    if not isinstance(flow_structure, dict):
        flow_structure = {}

    fs = {
        "pattern": flow_structure.get("pattern", "sequential"),
        "entry_points": flow_structure.get("entry_points", []) or [],
        "nodes": flow_structure.get("nodes", {}) or {},
        "edges": flow_structure.get("edges", []) or []
    }

    nodes = fs["nodes"]
    edges = fs["edges"]

    # Default node fields
    for nid, nd in nodes.items():
        nd.setdefault("kind", "Task")
        nd.setdefault("component_type_id", None)
        nd.setdefault("next_nodes", [])
        
        # Upstream policy
        nd.setdefault("upstream_policy", {})
        up = nd["upstream_policy"]
        if not up.get("type"):
            up["type"] = "all_success"
        up.setdefault("timeout_seconds", None)
        
        # Branch config
        nd.setdefault("branch_config", None)
        
        # Sensor config
        nd.setdefault("sensor_config", None)
        
        # Parallel config
        nd.setdefault("parallel_config", None)

    # Build edges if not provided
    if not edges:
        built_edges = []
        for nid, nd in nodes.items():
            for nxt in nd.get("next_nodes", []):
                if nxt in nodes:
                    built_edges.append({
                        "from": nid,
                        "to": nxt,
                        "edge_type": "success",
                        "condition": None,
                        "metadata": {}
                    })
        fs["edges"] = built_edges

    # Filter invalid edges
    valid_targets = set(nodes.keys())
    fs["edges"] = [
        e for e in fs["edges"]
        if isinstance(e, dict) and e.get("from") in nodes and e.get("to") in valid_targets
    ]

    # Auto-default upstream_policy for fan-in if missing
    incoming = {nid: 0 for nid in nodes}
    for e in fs["edges"]:
        if e["to"] in nodes:
            incoming[e["to"]] += 1

    for nid, cnt in incoming.items():
        if cnt > 1:
            if not nodes[nid]["upstream_policy"].get("type"):
                nodes[nid]["upstream_policy"]["type"] = "all_success"
                logging.debug(f"Auto-defaulted upstream_policy for fan-in node '{nid}' to 'all_success'")

    fs["nodes"] = nodes
    return fs

def fix_node_id_consistency(flow_structure: Dict, component_types_list: List[Dict]) -> Dict:
    """
    Post-process flow structure to ensure node IDs match component IDs.
    Fixes common LLM mistakes like adding suffixes.
    """
    logging.info("Post-processing flow structure for node ID consistency...")
    
    component_map = {c['id']: c for c in component_types_list}
    nodes = flow_structure.get('nodes', {})
    edges = flow_structure.get('edges', [])
    entry_points = flow_structure.get('entry_points', [])
    
    # Build mapping of incorrect node IDs to correct ones
    node_id_corrections = {}
    
    for node_id, node_data in list(nodes.items()):
        if node_data.get('kind') == 'Task':
            component_type_id = node_data.get('component_type_id')
            
            if component_type_id and node_id != component_type_id:
                # Node ID doesn't match component ID - need correction
                if component_type_id in component_map:
                    logging.warning(
                        f"Correcting node ID mismatch: '{node_id}' → '{component_type_id}'"
                    )
                    node_id_corrections[node_id] = component_type_id
                else:
                    logging.warning(
                        f"Node '{node_id}' references unknown component '{component_type_id}'"
                    )
    
    # Apply corrections if needed
    if node_id_corrections:
        # 1. Rename nodes
        corrected_nodes = {}
        for old_id, node_data in nodes.items():
            new_id = node_id_corrections.get(old_id, old_id)
            corrected_nodes[new_id] = node_data
            
            # Update next_nodes references
            corrected_next = [
                node_id_corrections.get(nxt, nxt) 
                for nxt in node_data.get('next_nodes', [])
            ]
            corrected_nodes[new_id]['next_nodes'] = corrected_next
        
        flow_structure['nodes'] = corrected_nodes
        
        # 2. Update edges
        for edge in edges:
            edge['from'] = node_id_corrections.get(edge.get('from'), edge.get('from'))
            edge['to'] = node_id_corrections.get(edge.get('to'), edge.get('to'))
        
        # 3. Update entry points
        corrected_entry_points = [
            node_id_corrections.get(ep, ep) 
            for ep in entry_points
        ]
        flow_structure['entry_points'] = corrected_entry_points
        
        logging.info(f"Applied {len(node_id_corrections)} node ID corrections")
    
    # Final validation
    nodes = flow_structure['nodes']
    missing_entry_points = [ep for ep in flow_structure['entry_points'] if ep not in nodes]
    if missing_entry_points:
        logging.error(f"Entry points still missing after correction: {missing_entry_points}")
        # Fallback: use first Task node as entry point
        task_nodes = [nid for nid, nd in nodes.items() if nd.get('kind') == 'Task']
        if task_nodes:
            flow_structure['entry_points'] = [task_nodes[0]]
            logging.warning(f"Fallback: Set entry point to first task: {task_nodes[0]}")
    
    return flow_structure

def validate_flow_graph(flow_structure: Dict, component_type_ids: set) -> List[str]:
    """
    Validate flow structure for cycles, unreachable nodes, and consistency.
    """
    warnings = []
    
    if not flow_structure or not flow_structure.get('nodes'):
        return ["Graph validation skipped: No nodes found in flow structure."]

    nodes = flow_structure.get('nodes', {})
    edges = flow_structure.get('edges', []) or []
    entry_points = flow_structure.get('entry_points', []) or []

    # Soft scope checks
    if len(nodes) > MAX_TOTAL_NODES:
        warnings.append(f"Pipeline complexity: {len(nodes)} nodes exceeds soft limit {MAX_TOTAL_NODES}")

    # Node validation
    for nid, nd in nodes.items():
        kind = nd.get('kind', 'Task')
        if kind not in NODE_KINDS:
            warnings.append(f"Node '{nid}' has unsupported kind '{kind}'")
        
        ctid = nd.get('component_type_id')
        if kind == 'Task' and ctid and ctid not in component_type_ids:
            warnings.append(f"Node '{nid}' references unknown component_type_id '{ctid}'")
        
        if kind == 'Sensor' and not nd.get('sensor_config'):
            warnings.append(f"Sensor node '{nid}' missing sensor_config")
        
        if kind == 'Branch' and not nd.get('branch_config'):
            warnings.append(f"Branch node '{nid}' missing branch_config")
        
        if kind == 'Parallel' and not nd.get('parallel_config'):
            warnings.append(f"Parallel node '{nid}' missing parallel_config")

    # Build graph
    G = nx.DiGraph()
    for nid in nodes:
        G.add_node(nid)
    
    for e in edges:
        src, dst = e.get('from'), e.get('to')
        if src in nodes and dst in nodes:
            G.add_edge(src, dst)

    # Check for cycles
    try:
        if not nx.is_directed_acyclic_graph(G):
            cycles = list(nx.simple_cycles(G))
            warnings.append(f"UNSUPPORTED: Flow contains cycles: {cycles}")
    except Exception as ex:
        warnings.append(f"Error during cycle detection: {ex}")

    # Check reachability
    reachable = set()
    for start in entry_points:
        if start in G:
            reachable |= set(nx.descendants(G, start))
            reachable.add(start)
    
    unreachable = [n for n in nodes.keys() if n not in reachable]
    if unreachable:
        warnings.append(f"Unreachable nodes: {sorted(unreachable)}")

    # Fan-in check
    for nid in nodes:
        predecessors = list(G.predecessors(nid))
        if len(predecessors) > 1:
            policy = nodes[nid].get('upstream_policy', {}).get('type')
            if not policy:
                warnings.append(f"Fan-in node '{nid}' missing upstream_policy (defaulted to 'all_success')")

    return warnings

def calculate_complexity_score(components: List[Dict], flow_structure: Dict) -> str:
    """Calculate pipeline complexity: low, medium, high."""
    num_components = len(components)
    num_nodes = len(flow_structure.get('nodes', {}))
    num_edges = len(flow_structure.get('edges', []))
    
    has_branching = any(
        n.get('kind') == 'Branch' 
        for n in flow_structure.get('nodes', {}).values()
    )
    has_parallelism = any(
        n.get('kind') == 'Parallel' 
        for n in flow_structure.get('nodes', {}).values()
    )
    
    score = 0
    if num_components > 10:
        score += 2
    elif num_components > 5:
        score += 1
    
    if num_edges > num_nodes * 1.5:
        score += 1
    
    if has_branching:
        score += 1
    if has_parallelism:
        score += 1
    
    if score >= 4:
        return "high"
    elif score >= 2:
        return "medium"
    else:
        return "low"

# --- Core Analysis Functions ---
def analyze_pipeline_characteristics(description: str, llm_provider: LLMProvider, config: Dict) -> Tuple[Dict, Dict[str, int]]:
    """
    Analyze pipeline characteristics: flow patterns, task executors, complexity.
    This replaces the old analyze_execution_environment function.
    """
    logging.info("Analyzing pipeline characteristics (orchestrator-neutral)...")
    
    system_prompt = """
You are a pipeline pattern analyzer. Analyze the description and identify:

1. Flow patterns present (select all that apply):
   - sequential: A → B → C (linear flow)
   - parallel: A → [B1, B2, B3] → C (fan-out/fan-in)
   - branching: A → Branch → [B if X, C if Y] (conditional routing)
   - sensor_driven: Wait for external condition before starting
   - event_driven: Triggered by external events
   - hybrid: combination of above

2. Task executor types used (select all that apply):
   - python: Python scripts/functions
   - docker: Docker containers
   - kubernetes: K8s pods
   - bash: Shell scripts
   - sql: SQL queries
   - spark: Spark jobs
   - http: HTTP API calls
   - custom: Other execution methods

3. Structural characteristics:
   - has_branching: boolean (conditional routing present)
   - has_parallelism: boolean (parallel task execution)
   - has_sensors: boolean (wait conditions present)
   - estimated_components: integer (rough count of distinct processing steps)

Output ONLY a JSON object:
{
  "detected_patterns": ["sequential", ...],
  "task_executors": ["docker", ...],
  "has_branching": false,
  "has_parallelism": false,
  "has_sensors": false,
  "estimated_components": 5
}

Do NOT:
- Infer specific orchestrator (Airflow, Prefect, Dagster)
- Include implementation details
- Make assumptions not supported by text
"""
    
    user_input = f"Pipeline Description:\n{description}"
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        characteristics = clean_and_parse_json(content, "Pipeline Characteristics")
        
        if not isinstance(characteristics, dict):
            logging.warning("Pipeline characteristics output not a dict; using defaults.")
            characteristics = {
                "detected_patterns": ["sequential"],
                "task_executors": ["python"],
                "has_branching": False,
                "has_parallelism": False,
                "has_sensors": False,
                "estimated_components": 0
            }
        else:
            # Validate and default
            characteristics.setdefault("detected_patterns", ["sequential"])
            characteristics.setdefault("task_executors", ["python"])
            characteristics.setdefault("has_branching", False)
            characteristics.setdefault("has_parallelism", False)
            characteristics.setdefault("has_sensors", False)
            characteristics.setdefault("estimated_components", 0)
        
        model_info = llm_provider.get_model_info()
        logging.info(f"Analyzed pipeline characteristics (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Characteristics): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return characteristics, tokens
    
    except Exception as e:
        logging.error(f"Pipeline characteristics analysis failed: {e}", exc_info=True)
        logging.warning("Defaulting to basic sequential pattern")
        return {
            "detected_patterns": ["sequential"],
            "task_executors": ["python"],
            "has_branching": False,
            "has_parallelism": False,
            "has_sensors": False,
            "estimated_components": 0
        }, {'input_tokens': 0, 'output_tokens': 0}

def identify_pipeline_components(description: str, llm_provider: LLMProvider, config: Dict) -> Tuple[List[Dict], Dict[str, int]]:
    """Identify component types using orchestrator-neutral semantics."""
    logging.info("Identifying pipeline component types (orchestrator-neutral)...")
    
    system_prompt = """
You are an expert pipeline component analyzer. Extract component TYPES using orchestrator-neutral semantics.
Focus on WHAT each component does, not HOW it's orchestrated.

For each component, provide:

REQUIRED:
- id: snake_case unique identifier (e.g., "load_and_modify_data")
      ⚠️ CRITICAL: This ID will be used as the node ID in the flow structure
      ⚠️ Use descriptive names without suffixes like "_task", "_component", "_step"
      ⚠️ Examples: "extract_sales", "transform_data", "load_warehouse" (NOT "extract_sales_task")
- name: human-readable name
- category: Extractor|Loader|Transformer|SQLTransform|Reconciliator|Enricher|QualityCheck|Sensor|Splitter|Merger|Aggregator|Orchestrator|Notifier|Other
- description: concise purpose (1-2 sentences)
- inputs: list of input sources/objects
- outputs: list of output artifacts

EXECUTION (orchestrator-neutral):
- executor_type: python|docker|kubernetes|bash|sql|spark|http|custom
- executor_config: {
    "image": "<docker_image>" or null (if containerized),
    "command": ["cmd", "args"] or null (REQUIRED for docker/kubernetes if not using image default),
    "script_path": "path/to/script.py" or null,
    "entry_point": "module.function" or null,
    "environment": { "ENV_VAR": "value" },
    "resources": { "cpu": "1", "memory": "2Gi", "gpu": null },
    "network": "network_name" or null
  }

CRITICAL RULES FOR DOCKER/KUBERNETES:
1. If "image" is specified, check if the description mentions:
   - "run command X" → set command: ["X"]
   - "execute script Y" → set command: ["python", "Y"] or command: ["bash", "Y"]
   - "start service Z" → command can be null (uses image default)
   - NO mention → set command: null (document that image must have ENTRYPOINT/CMD)

2. If command is null, add a note in "description" field:
   "Uses image default entrypoint/command"

EXAMPLES:
# Explicit command mentioned:
{
  "executor_config": {
    "image": "myapp:latest",
    "command": ["python", "process_data.py"],  // ✅ Explicit
    ...
  }
}

# No command mentioned (uses image default):
{
  "executor_config": {
    "image": "myapp:latest",
    "command": null,  // ✅ Document intent
    ...
  },
  "description": "Processes data using image default entrypoint"
}

# Service container:
{
  "executor_config": {
    "image": "postgres:13",
    "command": null,  // ✅ Service has built-in CMD
    ...
  }
}

I/O SPECIFICATION:
- io_spec: [
    {
      "name": "descriptive_name",
      "direction": "input|output",
      "kind": "file|table|object|api|stream",
      "format": "csv|json|parquet|avro|sql|binary|other",
      "path_pattern": "path or URL pattern with templates",
      "connection_id": "generic connection reference or null"
    }
  ]

DEPENDENCIES:
- upstream_policy: {
    "type": "all_success|any_success|all_done|one_success|none_failed|custom",
    "description": "human-readable explanation",
    "timeout_seconds": null or integer
  }

FAULT TOLERANCE:
- retry_policy: {
    "max_attempts": integer (0 = no retries),
    "delay_seconds": integer,
    "exponential_backoff": boolean,
    "retry_on": ["timeout", "network_error", ...]
  }

CONCURRENCY:
- concurrency: {
    "supports_parallelism": boolean (true if uses Spark/Dask/distributed processing INTERNALLY),
    "supports_dynamic_mapping": boolean (ONLY true if text explicitly says "for each file/partition/region" or "parallel instances" or "map over"),
    "map_over_param": "parameter_name" or null,
    "max_parallel_instances": integer or null
  }

CONNECTIONS:
- connections: [
    { "id": "conn_id", "type": "filesystem|database|api|object_storage|...", "purpose": "..." }
  ]

DATASETS (for lineage):
- datasets: {
    "consumes": ["dataset_name_1", ...],
    "produces": ["dataset_name_2", ...]
  }

RULES:
1. Batch-only semantics (no streaming/Kafka/Flink)
2. Do NOT infer orchestrator type (no Airflow/Prefect/Dagster references)
3. Deduplicate by TYPE: if multiple mentions = same operation, output ONE component
4. Set supports_dynamic_mapping=true ONLY if explicitly stated in text
5. If Docker image is mentioned, set executor_type="docker" and include image in executor_config
6. If no execution details provided, default to executor_type="python"

GUIDANCE:
- "Ingest/Load/Read from file/DB" → likely category=Extractor
- "Write/Save/Export to file/DB" → likely category=Loader
- "Transform/Process/Enrich/Clean" → likely category=Transformer/Enricher
- "Validate/Check/Verify" → likely category=QualityCheck

ID NAMING RULES:
1. Use snake_case (lowercase with underscores)
2. Be descriptive but concise: "enrich_weather_data" not "component_that_enriches_data_with_weather"
3. Start with action verb: "extract_", "transform_", "load_", "validate_", "reconcile_"
4. NO suffixes: Don't add "_task", "_component", "_node", "_step", "_job"
5. Ensure uniqueness: If multiple similar components, differentiate with specifics
   Example: "extract_sales_db", "extract_sales_api" (not "extract_sales_1", "extract_sales_2")

Return ONLY a JSON array of component objects.
"""
    
    user_input = f"Pipeline Description:\n{description}"
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        components_list = clean_and_parse_json(content, "Component Identification")
        
        if components_list is None or not isinstance(components_list, list):
            logging.error("Failed to parse component list from LLM or output was not a list.")
            return [], tokens or {'input_tokens': 0, 'output_tokens': 0}

        # Normalize and validate components
        valid_components = []
        seen_ids = set()
        
        for i, comp in enumerate(components_list):
            normalized = normalize_component(comp, i)
            if normalized is None:
                continue
            
            cid = normalized['id']
            if cid in seen_ids:
                logging.warning(f"Duplicate component id '{cid}' found. Keeping first instance.")
                continue
            
            seen_ids.add(cid)
            valid_components.append(normalized)

        model_info = llm_provider.get_model_info()
        logging.info(f"Identified {len(valid_components)} component types (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Components): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return valid_components, tokens
    
    except Exception as e:
        logging.error(f"Component identification failed: {e}", exc_info=True)
        raise

def analyze_pipeline_flow(description: str, component_types_list: List[Dict], llm_provider: LLMProvider, config: Dict) -> Tuple[Dict, Dict[str, int]]:
    """Determine detailed execution flow using orchestrator-neutral semantics."""
    if not component_types_list:
        logging.warning("Skipping flow analysis: no component types identified.")
        return {
            "pattern": "sequential",
            "entry_points": [],
            "nodes": {},
            "edges": []
        }, {'input_tokens': 0, 'output_tokens': 0}

    logging.info("Analyzing pipeline flow structure (orchestrator-neutral)...")
    
    system_prompt = """
You are an expert pipeline flow analyzer. Create a detailed flow structure using orchestrator-neutral semantics.

SUPPORTED PATTERNS:
- sequential: A → B → C
- parallel: A → [B1, B2, B3] → C (fan-out/fan-in)
- branching: A → Branch → [B if X, C if Y]
- sensor_driven: Sensor → A → B
- hybrid: combination of above

OUTPUT STRUCTURE:
{
  "flow_structure": {
    "pattern": "sequential|parallel|branching|sensor_driven|hybrid",
    "entry_points": ["<node_id>", ...],
    
    "nodes": {
      "<node_id>": {
        "kind": "Task|Branch|Sensor|Parallel|Join|Virtual",
        "component_type_id": "<id from component types>" or null,
        
        // For ALL nodes:
        "upstream_policy": {
          "type": "all_success|any_success|all_done|one_success|none_failed|custom",
          "timeout_seconds": null or integer
        },
        "next_nodes": ["<node_id>", ...],
        
        // For Branch nodes (conditional routing):
        "branch_config": {
          "type": "conditional",
          "branches": [
            {
              "label": "human-readable condition name",
              "condition": "logical expression or 'else' or 'default'",
              "next_node": "<node_id>"
            }
          ]
        },
        
        // For Sensor nodes (wait conditions):
        "sensor_config": {
          "sensor_type": "file|external_task|http|time|dataset|custom",
          "target": "what to wait for (file path, task name, URL, etc.)",
          "poke_interval_seconds": 60,
          "timeout_seconds": 3600,
          "mode": "poke|reschedule"
        },
        
        // For Parallel nodes (fan-out):
        "parallel_config": {
          "type": "dynamic_map|static_parallel",
          "map_over": "parameter_or_dataset_name",
          "map_source": "{{ params.list }} or expression",
          "max_parallelism": integer or null,
          "join_node": "<node_id of join point>"
        }
      }
    },
    
    "edges": [
      {
        "from": "<node_id>",
        "to": "<node_id>",
        "edge_type": "success|failure|always|conditional",
        "condition": "optional condition label",
        "metadata": {}
      }
    ]
  }
}

CRITICAL RULES FOR NODE IDs:
1. **For Task nodes**: The node_id MUST EXACTLY MATCH the component_type_id
   ❌ WRONG: component_type_id="load_and_modify_data", node_id="load_and_modify_data_task"
   ✅ CORRECT: component_type_id="load_and_modify_data", node_id="load_and_modify_data"

2. **Do NOT add suffixes** like "_task", "_node", "_step" to node IDs
   - Use the component ID directly as the node ID

3. **Entry points** must reference node IDs that exist in the "nodes" dictionary
   ❌ WRONG: entry_points: ["load_and_modify_data_task"] when node is "load_and_modify_data"
   ✅ CORRECT: entry_points: ["load_and_modify_data"] matching the node ID

4. **For non-Task nodes** (Branch, Sensor, Parallel, Join):
   - Use descriptive IDs like "wait_for_file", "validate_data_branch", "aggregate_results"
   - Keep them concise and snake_case
   - DO NOT reference component_type_id (set to null)

5. **Edge consistency**: All "from" and "to" fields must reference existing node IDs

EXAMPLE (CORRECT):
{
  "flow_structure": {
    "entry_points": ["extract_data"],
    "nodes": {
      "extract_data": {
        "kind": "Task",
        "component_type_id": "extract_data",
        "next_nodes": ["transform_data"]
      },
      "transform_data": {
        "kind": "Task",
        "component_type_id": "transform_data",
        "next_nodes": ["load_data"]
      },
      "load_data": {
        "kind": "Task",
        "component_type_id": "load_data",
        "next_nodes": []
      }
    },
    "edges": [
      {"from": "extract_data", "to": "transform_data"},
      {"from": "transform_data", "to": "load_data"}
    ]
  }
}

VALIDATION CHECKLIST BEFORE RETURNING:
- [ ] All entry_points exist as keys in nodes
- [ ] All Task nodes have matching node_id and component_type_id
- [ ] All edges reference valid node IDs
- [ ] No "_task", "_node", "_step" suffixes in node IDs
- [ ] All component_type_ids from provided list are used exactly as given

Return ONLY the JSON object with "flow_structure" key.
"""
    
    component_details = format_components_for_prompt(component_types_list)
    user_input = f"""
Pipeline Description:
{description}

Identified Component Types (use these IDs EXACTLY as node IDs for Task nodes):
{component_details}

REMINDER: For Task nodes, node_id MUST EQUAL component_type_id. Do not add suffixes.
"""
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        flow_data = clean_and_parse_json(content, "Flow Analysis")

        flow_structure = {
            "pattern": "sequential",
            "entry_points": [],
            "nodes": {},
            "edges": []
        }

        if isinstance(flow_data, dict) and 'flow_structure' in flow_data:
            fs = flow_data['flow_structure']
            if isinstance(fs, dict):
                flow_structure = normalize_flow_structure(fs)
            else:
                logging.error("flow_structure is not a dict. Using defaults.")
        else:
            logging.error("LLM did not return valid 'flow_structure'. Using defaults.")

        # POST-PROCESSING: Validate and fix node ID consistency
        flow_structure = fix_node_id_consistency(flow_structure, component_types_list)

        model_info = llm_provider.get_model_info()
        logging.info(f"Analyzed flow structure (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Flow): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return flow_structure, tokens
    
    except Exception as e:
        logging.error(f"Flow analysis failed: {e}", exc_info=True)
        raise

def extract_parameter_schema(description: str, components_list: List[Dict], llm_provider: LLMProvider, config: Dict) -> Tuple[Dict, Dict[str, int]]:
    """Extract parameter schema using orchestrator-neutral semantics."""
    logging.info("Extracting parameter schema (orchestrator-neutral)...")
    
    system_prompt = """
You are an ETL parameter schema analyzer. Extract a comprehensive parameter schema using orchestrator-neutral semantics.

OUTPUT STRUCTURE:
{
  "pipeline": {
    "name": {
      "description": "Pipeline identifier",
      "type": "string",
      "default": null or value,
      "required": boolean,
      "constraints": null or description
    },
    "description": { ... },
    "tags": {
      "description": "Classification tags",
      "type": "array",
      "default": [],
      "required": false
    }
  },
  
  "schedule": {
    "enabled": {
      "description": "Whether pipeline runs on schedule",
      "type": "boolean",
      "default": null,
      "required": false
    },
    "cron_expression": {
      "description": "Cron or preset (e.g., @daily, 0 0 * * *)",
      "type": "string",
      "default": null,
      "required": false
    },
    "start_date": {
      "description": "When to start scheduling",
      "type": "datetime",
      "default": null,
      "required": false,
      "format": "ISO8601"
    },
    "end_date": {
      "description": "When to stop scheduling",
      "type": "datetime",
      "default": null,
      "required": false
    },
    "timezone": {
      "description": "Schedule timezone",
      "type": "string",
      "default": null,
      "required": false
    },
    "catchup": {
      "description": "Run missed intervals",
      "type": "boolean",
      "default": null,
      "required": false
    },
    "batch_window": {
      "description": "Batch window parameter name (e.g., ds, execution_date)",
      "type": "string",
      "default": null,
      "required": false
    },
    "partitioning": {
      "description": "Data partitioning strategy (e.g., daily, hourly, monthly)",
      "type": "string",
      "default": null,
      "required": false
    }
  },
  
  "execution": {
    "max_active_runs": {
      "description": "Max concurrent pipeline runs",
      "type": "integer",
      "default": null,
      "required": false
    },
    "timeout_seconds": {
      "description": "Pipeline execution timeout",
      "type": "integer",
      "default": null,
      "required": false
    },
    "retry_policy": {
      "description": "Pipeline-level retry behavior",
      "type": "object",
      "default": null,
      "required": false
    },
    "depends_on_past": {
      "description": "Whether execution depends on previous run success",
      "type": "boolean",
      "default": null,
      "required": false
    }
  },
  
  "components": {
    "<component_id>": {
      "<param_name>": {
        "description": "...",
        "type": "string|integer|float|boolean|array|object|file|directory",
        "default": null or value,
        "required": boolean,
        "constraints": null or description
      }
    }
  },
  
  "environment": {
    "<ENV_VAR_NAME>": {
      "description": "...",
      "type": "string|...",
      "default": null or value,
      "required": boolean,
      "associated_component_id": "<component_id>" or null
    }
  }
}

CRITICAL RULES:
1. If a field is NOT explicitly mentioned in the description, set default=null and required=false
2. Do NOT infer likely defaults unless clearly stated in text
3. Use generic field names (not orchestrator-specific like schedule_interval, dag_id, flow_name)
4. If schedule info is mentioned, populate schedule section; otherwise leave as null
5. For required=true, there MUST be explicit text saying "required" or "must provide"
6. Component-specific parameters use component IDs from provided list
7. Environment variables should be UPPER_SNAKE_CASE

Return ONLY a JSON object with these top-level keys: pipeline, schedule, execution, components, environment.
"""
    
    component_details = format_components_for_prompt(components_list)
    user_input = f"""
Pipeline Description:
{description}

Component IDs (use these for 'components' keys):
{component_details}
"""
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        params_data = clean_and_parse_json(content, "Parameter Schema")
        
        if not isinstance(params_data, dict):
            logging.warning("Parameter schema output not a dict; using empty defaults.")
            params_data = {}
        
        # Ensure top-level keys exist
        params_data.setdefault("pipeline", {})
        params_data.setdefault("schedule", {})
        params_data.setdefault("execution", {})
        params_data.setdefault("components", {})
        params_data.setdefault("environment", {})
        
        model_info = llm_provider.get_model_info()
        logging.info(f"Extracted parameter schema (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Parameters): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return params_data, tokens
    
    except Exception as e:
        logging.error(f"Parameter schema extraction failed: {e}", exc_info=True)
        logging.warning("Defaulting parameters to empty structure")
        return {
            "pipeline": {},
            "schedule": {},
            "execution": {},
            "components": {},
            "environment": {}
        }, {'input_tokens': 0, 'output_tokens': 0}

def analyze_integration_points(description: str, components_list: List[Dict], llm_provider: LLMProvider, config: Dict) -> Tuple[Dict, Dict[str, int]]:
    """Identify integration points using orchestrator-neutral semantics."""
    logging.info("Analyzing integration points (orchestrator-neutral)...")
    
    system_prompt = """
You are an integration point analyzer. Identify all external systems, data sources, and connections.

OUTPUT STRUCTURE:
{
  "connections": [
    {
      "id": "short_snake_case_id",
      "name": "Human-readable name",
      "type": "filesystem|database|api|object_storage|message_queue|data_warehouse|cache|other",
      "config": {
        "base_path": "/path" or null,
        "base_url": "https://..." or null,
        "host": "hostname" or null,
        "port": integer or null,
        "protocol": "file|http|https|s3|gcs|azure_blob|jdbc|..." or null,
        "database": "db_name" or null,
        "schema": "schema_name" or null,
        "bucket": "bucket_name" or null,
        "queue_name": "queue_name" or null
      },
      "authentication": {
        "type": "none|basic|token|oauth|iam|key_pair|certificate",
        "token_env_var": "ENV_VAR_NAME" or null,
        "username_env_var": "ENV_VAR_NAME" or null,
        "password_env_var": "ENV_VAR_NAME" or null,
        "credentials_path": "/path/to/creds" or null
      },
      "used_by_components": ["component_id_1", "component_id_2"],
      "direction": "input|output|both",
      "rate_limit": {
        "requests_per_second": integer or null,
        "burst": integer or null
      },
      "datasets": {
        "produces": ["dataset_name_1", ...],
        "consumes": ["dataset_name_2", ...]
      }
    }
  ],
  
  "data_lineage": {
    "sources": [
      "Human-readable description of data source 1",
      "Human-readable description of data source 2"
    ],
    "sinks": [
      "Human-readable description of output destination 1"
    ],
    "intermediate_datasets": [
      "file_pattern_or_table_1",
      "file_pattern_or_table_2"
    ]
  }
}

RULES:
1. Do NOT use orchestrator-specific connection terminology (airflow_conn_id, prefect_block, dagster_resource)
2. Use generic connection IDs that can be mapped to any orchestrator
3. Include infrastructure even if not tied to specific component (e.g., Docker network, shared volumes)
4. For APIs, include base URL, authentication method, rate limits if mentioned
5. For databases, include host, port, database name, schema if mentioned
6. For filesystems, include base path, protocol (file, s3, gcs, etc.)
7. Component references should use IDs from provided component list
8. Direction: "input" = read from, "output" = write to, "both" = read and write

Return ONLY a JSON object with keys: connections, data_lineage.
"""
    
    component_details = format_components_for_prompt(components_list)
    user_input = f"""
Pipeline Description:
{description}

Components in the pipeline (use these IDs):
{component_details}
"""
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        integration_data = clean_and_parse_json(content, "Integration Points")
        
        if not isinstance(integration_data, dict):
            logging.warning("Integration points output not a dict; using empty defaults.")
            integration_data = {}
        
        # Ensure keys exist
        integration_data.setdefault("connections", [])
        integration_data.setdefault("data_lineage", {
            "sources": [],
            "sinks": [],
            "intermediate_datasets": []
        })
        
        # Validate connections
        valid_connections = []
        for conn in integration_data.get("connections", []):
            if isinstance(conn, dict) and conn.get("id") and conn.get("type"):
                conn.setdefault("config", {})
                conn.setdefault("authentication", {})
                conn.setdefault("used_by_components", [])
                conn.setdefault("direction", "both")
                conn.setdefault("datasets", {"produces": [], "consumes": []})
                valid_connections.append(conn)
        
        integration_data["connections"] = valid_connections
        
        model_info = llm_provider.get_model_info()
        num_connections = len(valid_connections)
        logging.info(f"Identified {num_connections} connections (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Integrations): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return integration_data, tokens
    
    except Exception as e:
        logging.error(f"Integration analysis failed: {e}", exc_info=True)
        logging.warning("Defaulting integrations to empty structure")
        return {
            "connections": [],
            "data_lineage": {
                "sources": [],
                "sinks": [],
                "intermediate_datasets": []
            }
        }, {'input_tokens': 0, 'output_tokens': 0}

def generate_textual_analysis(description: str, characteristics: Dict, components: List[Dict],
                              flow_structure: Dict, parameters: Dict, integrations: Dict,
                              llm_provider: LLMProvider, config: Dict) -> Tuple[str, Dict[str, int]]:
    """Generate a narrative textual analysis using orchestrator-neutral language."""
    logging.info("Generating textual analysis report (orchestrator-neutral)...")
    
    system_prompt = """
You are an expert ETL pipeline analyst. Create a clear, well-structured textual report using orchestrator-neutral language.
Base the report ONLY on the structured data provided.

STRUCTURE:
1. Executive Summary
   - Overall purpose and high-level flow
   - Key patterns and complexity

2. Pipeline Architecture
   - Flow Patterns: Describe detected patterns (sequential, parallel, branching, etc.)
   - Execution Characteristics: Task executor types used
   - Component Overview: List categories and their roles
   - Flow Description: Entry points, main sequence, branching/parallelism/sensors if present

3. Detailed Component Analysis
   - For each component type:
     * Purpose and category
     * Executor type and configuration
     * Inputs and outputs
     * Retry policy and concurrency settings
     * Connected systems

4. Parameter Schema
   - Pipeline-level parameters
   - Schedule configuration (if defined)
   - Execution settings
   - Component-specific parameters
   - Environment variables

5. Integration Points
   - External systems and connections
   - Data sources and sinks
   - Authentication methods
   - Data lineage

6. Implementation Notes
   - Complexity assessment
   - Upstream dependency policies
   - Retry and timeout configurations
   - Potential risks or considerations

7. Orchestrator Compatibility
   - Assessment for Airflow, Prefect, Dagster
   - Any pattern-specific considerations

8. Conclusion

CRITICAL RULES:
1. Do NOT use orchestrator-specific terms (DAG, Flow, Job, Operator, Task decorator, etc.)
2. Use neutral language: "pipeline", "component", "task", "upstream policy", "executor type"
3. Do NOT recommend a specific orchestrator
4. Focus on WHAT the pipeline does, not HOW it's implemented in a specific tool

Output ONLY the textual report.
"""
    
    user_input = f"""
Structured Analysis Data:

Pipeline Characteristics:
{json.dumps(characteristics, indent=2)}

Components:
{json.dumps(components, indent=2)}

Flow Structure:
{json.dumps(flow_structure, indent=2)}

Parameters:
{json.dumps(parameters, indent=2)}

Integrations:
{json.dumps(integrations, indent=2)}

Original Pipeline Description (for context only; base report on structured data above):
{description[:2000]}...
"""
    
    try:
        content, tokens = llm_provider.generate_completion(system_prompt, user_input)
        text_analysis = (content or "").strip()
        
        if not text_analysis:
            logging.warning("LLM generated an empty textual analysis report.")
            text_analysis = "Textual analysis could not be generated by the LLM."
        
        model_info = llm_provider.get_model_info()
        logging.info(f"Generated textual analysis report (model {model_info.get('model_name', 'N/A')})")
        logging.debug(f"Token usage (Textual Analysis): In={tokens.get('input_tokens', 0)} Out={tokens.get('output_tokens', 0)}")
        
        return text_analysis, tokens
    
    except Exception as e:
        logging.error(f"Textual analysis generation failed: {e}", exc_info=True)
        return "Textual analysis generation failed.", {'input_tokens': 0, 'output_tokens': 0}

# --- Output Assembly ---
def assess_orchestrator_compatibility(characteristics: Dict, flow_structure: Dict) -> Dict:
    """Assess compatibility with major orchestrators."""
    patterns = characteristics.get("detected_patterns", [])
    has_branching = characteristics.get("has_branching", False)
    has_parallelism = characteristics.get("has_parallelism", False)
    has_sensors = characteristics.get("has_sensors", False)
    
    compatibility = {
        "airflow": {
            "supported": True,
            "confidence": "high",
            "notes": []
        },
        "prefect": {
            "supported": True,
            "confidence": "high",
            "notes": []
        },
        "dagster": {
            "supported": True,
            "confidence": "high",
            "notes": []
        }
    }
    
    # Airflow assessment
    if "sequential" in patterns:
        compatibility["airflow"]["notes"].append("Sequential pattern fully supported")
    if has_branching:
        compatibility["airflow"]["notes"].append("Branching via BranchPythonOperator")
    if has_parallelism:
        compatibility["airflow"]["notes"].append("Parallelism via TaskFlow API expand()")
    if has_sensors:
        compatibility["airflow"]["notes"].append("Native sensor support")
    
    # Prefect assessment
    if "sequential" in patterns:
        compatibility["prefect"]["notes"].append("Sequential flow with task dependencies")
    if has_branching:
        compatibility["prefect"]["notes"].append("Conditional logic via Python control flow")
    if has_parallelism:
        compatibility["prefect"]["notes"].append("map() for parallel execution")
    if has_sensors:
        compatibility["prefect"]["notes"].append("wait_for() or custom sensors")
    
    # Dagster assessment
    if "sequential" in patterns:
        compatibility["dagster"]["notes"].append("Op graph with dependencies")
    if has_branching:
        compatibility["dagster"]["notes"].append("Dynamic graphs or branching ops")
    if has_parallelism:
        compatibility["dagster"]["notes"].append("DynamicOutput for fan-out")
    if has_sensors:
        compatibility["dagster"]["notes"].append("Sensors via run_status_sensor")
    
    return compatibility

def construct_final_json(characteristics: Dict, components: List[Dict], flow_structure: Dict,
                         parameters: Dict, integrations: Dict, source_file: str,
                         provider: str, model_key: Optional[str], 
                         validation_warnings: List[str]) -> Dict:
    """Build final JSON output with schema 2.0."""
    timestamp = datetime.now().isoformat()
    project_root = get_project_root()
    
    try:
        relative_source_file = os.path.relpath(source_file, project_root) if os.path.isabs(source_file) else source_file
    except ValueError:
        relative_source_file = source_file

    # Extract pipeline name and description from parameters if available
    pipeline_name = "unnamed_pipeline"
    pipeline_desc = "No description provided."
    
    if isinstance(parameters.get('pipeline'), dict):
        name_param = parameters['pipeline'].get('name')
        desc_param = parameters['pipeline'].get('description')
        
        if isinstance(name_param, dict) and name_param.get('default'):
            pipeline_name = str(name_param['default'])
        if isinstance(desc_param, dict) and desc_param.get('default'):
            pipeline_desc = str(desc_param['default'])
    
    # If still unnamed, use first component ID
    if pipeline_name == "unnamed_pipeline" and components:
        pipeline_name = f"{components[0].get('id', 'pipeline')}_pipeline"

    # Calculate complexity
    complexity = calculate_complexity_score(components, flow_structure)
    
    # Assess orchestrator compatibility
    orchestrator_compat = assess_orchestrator_compatibility(characteristics, flow_structure)
    
    # Build analysis results
    analysis_results = {
        "detected_patterns": characteristics.get("detected_patterns", []),
        "task_executors_used": characteristics.get("task_executors", []),
        "has_branching": characteristics.get("has_branching", False),
        "has_parallelism": characteristics.get("has_parallelism", False),
        "has_sensors": characteristics.get("has_sensors", False),
        "total_components": len(components),
        "complexity_score": complexity
    }
    
    final_data = {
        "metadata": {
            "schema_version": ANALYSIS_SCHEMA_VERSION,
            "analysis_timestamp": timestamp,
            "source_file": relative_source_file,
            "llm_provider": provider,
            "llm_model": model_key,
            "analysis_results": analysis_results,
            "orchestrator_compatibility": orchestrator_compat,
            "validation_warnings": validation_warnings
        },
        "pipeline_summary": {
            "name": pipeline_name,
            "description": pipeline_desc,
            "flow_patterns": characteristics.get("detected_patterns", []),
            "task_executors": characteristics.get("task_executors", []),
            "complexity": complexity
        },
        "components": components,
        "flow_structure": flow_structure,
        "parameters": parameters,
        "integrations": integrations
    }
    
    logging.debug("Constructed final JSON structure (schema 2.0).")
    return final_data

# --- Output Saving ---
def save_output(text_analysis: str, json_data: Dict, config: Dict, model_key: Optional[str], output_dir: str) -> Tuple[str, str]:
    """Save analysis output to JSON and text files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = get_project_root()

    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = Path(project_root) / output_dir

    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Failed to create output directory '{output_path}': {e}.")
        raise

    provider_suffix = json_data.get("metadata", {}).get("llm_provider", "unknown_provider")
    model_suffix = f"_{model_key}" if model_key else ""
    safe_model_suffix = re.sub(r'[^\w\-_\.]', '_', model_suffix)
    base_filename = f"pipeline_analysis_s1_{provider_suffix}{safe_model_suffix}_{timestamp}"

    # Save JSON
    json_file = output_path / f"{base_filename}.json"
    logging.info(f"Saving structured analysis JSON to: {json_file}")
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        logging.info("Structured analysis JSON saved.")
    except Exception as e:
        logging.error(f"Error saving JSON: {e}")
        raise

    # Save text report
    text_file = output_path / f"{base_filename}.txt"
    logging.info(f"Saving textual analysis report to: {text_file}")
    try:
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(f"# Pipeline Analysis Report (Step 1 - Schema {ANALYSIS_SCHEMA_VERSION})\n")
            f.write(f"# Generated: {json_data.get('metadata', {}).get('analysis_timestamp', 'N/A')}\n")
            f.write(f"# Provider: {json_data.get('metadata', {}).get('llm_provider', 'N/A')}\n")
            f.write(f"# Model: {json_data.get('metadata', {}).get('llm_model', 'N/A')}\n")
            f.write(f"# Source: {json_data.get('metadata', {}).get('source_file', 'N/A')}\n")
            f.write(f"# Orchestrator-Agnostic Analysis\n")
            f.write("="*80 + "\n\n")
            f.write(text_analysis if text_analysis else "[No textual analysis generated]")
        logging.info("Textual analysis report saved.")
    except Exception as e:
        logging.error(f"Error saving text report: {e}")
        raise

    return str(json_file), str(text_file)

def save_token_usage(token_details_by_step: dict, config: Dict, model_key: str = None, output_dir: str = 'outputs') -> str:
    """Save aggregated token usage details as a JSON file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    provider = config.get('model_settings', {}).get('active_provider', 'unknown_provider')
    project_root = get_project_root()

    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = Path(project_root) / output_dir

    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Failed to create output directory '{output_path}' for token usage: {e}.")
        return ""

    total_input = 0
    total_output = 0
    processed_steps = {}
    
    for step_name, usage_data in token_details_by_step.items():
        if isinstance(usage_data, dict):
            inp = usage_data.get('input_tokens', 0)
            out = usage_data.get('output_tokens', 0)
            total_input += inp
            total_output += out
            processed_steps[step_name] = {'input_tokens': inp, 'output_tokens': out}
        else:
            processed_steps[step_name] = None

    output_data = {
        "step_script": script_name,
        "provider": provider,
        "model_key": model_key,
        "timestamp": datetime.now().isoformat(),
        "steps": processed_steps,
        "totals": {
            "input_tokens": total_input,
            "output_tokens": total_output,
            "total_tokens": total_input + total_output
        }
    }

    model_part = f"_{model_key}" if model_key else ""
    safe_model_part = re.sub(r'[^\w\-_\.]', '_', model_part)
    filename = f"{script_name}_{provider}{safe_model_part}_tokens_{timestamp}.json"
    token_file_path = output_path / filename

    logging.info(f"Saving token usage details to: {token_file_path}")
    try:
        with open(token_file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        logging.info("Token usage saved.")
        return str(token_file_path)
    except Exception as e:
        logging.error(f"Error saving token usage: {e}")
        return ""

# --- Main Orchestration ---
def main():
    parser = argparse.ArgumentParser(
        description='Pipeline Analysis Tool (Step 1) - Orchestrator-Agnostic Analysis (Schema 2.0)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--config', default=None,
                        help='Path to pipeline configuration file (optional)')
    parser.add_argument('--llm-config', default=os.path.join(get_project_root(), 'config_llm.json'),
                        help='Path to LLM configuration file')
    parser.add_argument('--input', required=True,
                        help='Path to pipeline description text file (relative to project root or absolute)')
    parser.add_argument('--provider', choices=['deepinfra', 'openai', 'claude', 'azureopenai', 'ollama'],
                        help='Override the LLM provider specified in llm-config')
    parser.add_argument('--model',
                        help='Override the model key to use')
    parser.add_argument('--output-dir', default='outputs',
                        help='Directory to save all output files.')
    args = parser.parse_args()

    # Load config and setup logging
    pipeline_config_path = args.config
    pipeline_config = load_config(pipeline_config_path, DEFAULT_PIPELINE_CONFIG)
    setup_logging(pipeline_config)
    logging.info(f"Starting Step 1: Orchestrator-Agnostic Pipeline Analysis (Schema {ANALYSIS_SCHEMA_VERSION})")

    llm_config = load_config(args.llm_config)
    if not llm_config:
        logging.critical(f"LLM configuration missing or empty at {args.llm_config}. Exiting.")
        sys.exit(1)

    config = {**pipeline_config, **llm_config}

    active_provider = config.get('model_settings', {}).get('active_provider')
    if not active_provider:
        logging.critical("Missing 'active_provider' in LLM config 'model_settings'. Exiting.")
        sys.exit(1)
    
    if args.provider and args.provider != active_provider:
        logging.info(f"Overriding provider from '{active_provider}' to '{args.provider}'")
        active_provider = args.provider
        config['model_settings']['active_provider'] = active_provider

    model_key = args.model
    if not model_key:
        provider_settings = config.get('model_settings', {}).get(active_provider, {})
        model_key = provider_settings.get('active_model') if 'active_model' in provider_settings else None

    provider_settings = config.get('model_settings', {}).get(active_provider, {})
    if 'models' in provider_settings:
        if model_key not in provider_settings.get('models', {}):
            logging.critical(f"Selected model key '{model_key}' is not defined for provider '{active_provider}'.")
            sys.exit(1)

    logging.info(f"Using LLM Provider: {active_provider}, Model Key: {model_key or 'N/A'}")
    
    if not validate_api_keys(config):
        logging.critical("API key validation failed. Check config/environment.")
        sys.exit(1)
    logging.info("API key validation passed.")

    try:
        llm_provider = LLMProvider(config, model_key)
        total_token_usage = {}
        validation_warnings = []

        # Read pipeline description
        logging.info(f"Reading pipeline description from: {args.input}")
        pipeline_description = read_pipeline_description(args.input)
        if not pipeline_description:
            raise ValueError(f"Description file '{args.input}' is empty.")

        # Step A: Analyze characteristics (replaces environment detection)
        characteristics, char_tokens = analyze_pipeline_characteristics(
            pipeline_description, llm_provider, config
        )
        total_token_usage['characteristics_analysis'] = char_tokens

        # Step B: Identify components
        components_list, comp_tokens = identify_pipeline_components(
            pipeline_description, llm_provider, config
        )
        total_token_usage['component_identification'] = comp_tokens
        
        if not components_list:
            logging.warning("No component types identified; subsequent steps may be limited.")

        # Step C: Analyze flow structure
        flow_structure, flow_tokens = analyze_pipeline_flow(
            pipeline_description, components_list, llm_provider, config
        )
        flow_structure = normalize_flow_structure(flow_structure)
        total_token_usage['flow_analysis'] = flow_tokens

        # Validate flow
        graph_warnings = validate_flow_graph(flow_structure, {c['id'] for c in components_list})
        if graph_warnings:
            logging.warning("Graph-level validation issues found:")
            for w in graph_warnings:
                logging.warning(f"  - {w}")
            validation_warnings.extend(graph_warnings)

        # Step D: Extract parameters
        parameters, param_tokens = extract_parameter_schema(
            pipeline_description, components_list, llm_provider, config
        )
        total_token_usage['parameter_extraction'] = param_tokens

        # Step E: Analyze integrations
        integrations, integ_tokens = analyze_integration_points(
            pipeline_description, components_list, llm_provider, config
        )
        total_token_usage['integration_analysis'] = integ_tokens

        # Step F: Generate textual analysis
        text_analysis, analysis_tokens = generate_textual_analysis(
            pipeline_description, characteristics, components_list, flow_structure,
            parameters, integrations, llm_provider, config
        )
        total_token_usage['textual_analysis'] = analysis_tokens

        # Assemble final JSON
        final_json_data = construct_final_json(
            characteristics=characteristics,
            components=components_list,
            flow_structure=flow_structure,
            parameters=parameters,
            integrations=integrations,
            source_file=args.input,
            provider=active_provider,
            model_key=model_key,
            validation_warnings=validation_warnings
        )

        # Save outputs
        logging.info("Saving analysis results...")
        json_file, text_file = save_output(text_analysis, final_json_data, config, model_key, args.output_dir)
        token_file = save_token_usage(total_token_usage, config, model_key, args.output_dir)

        logging.info("="*60)
        logging.info("Pipeline Analysis Step 1 Completed Successfully!")
        logging.info(f"Schema Version: {ANALYSIS_SCHEMA_VERSION} (Orchestrator-Agnostic)")
        logging.info(f"Structured JSON: {json_file}")
        logging.info(f"Text Report:     {text_file}")
        logging.info(f"Token Usage:     {token_file or 'Not saved'}")
        logging.info("="*60)

        print("\nAnalysis complete. Check log file and output directory for details.")
        print(f"Results saved to output directory: {args.output_dir}")
        print(f"\nOrchestrator Compatibility:")
        compat = final_json_data['metadata']['orchestrator_compatibility']
        for orch, info in compat.items():
            status = "✓" if info['supported'] else "✗"
            print(f"  {status} {orch.capitalize()}: {info['confidence']} confidence")

    except Exception as e:
        print(f"\nFATAL SCRIPT-LEVEL ERROR: {e}", file=sys.stderr)
        print("--------------------------------------------------", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("--------------------------------------------------", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except Exception as e:
        print(f"\nFATAL SCRIPT-LEVEL ERROR: {e}", file=sys.stderr)
        print("--------------------------------------------------", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("--------------------------------------------------", file=sys.stderr)
        sys.exit(1)