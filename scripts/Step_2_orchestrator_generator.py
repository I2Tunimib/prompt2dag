#!/usr/bin/env python3
"""
Step 2: Orchestrator-Specific Pipeline Generation
==================================================
Takes Step 1 orchestrator-agnostic JSON and generates:
1. Validated intermediate YAML (orchestrator-specific)
2. Executable code (Airflow DAG / Prefect Flow / Dagster Job)

Usage:
    python Step_2_orchestrator_generator.py \
        --input outputs_step1/pipeline_analysis_s1_*.json \
        --target airflow \
        --output-dir outputs_step2
"""

import os
import sys
import json
import argparse
import logging
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# Add parent directory to path
script_dir = Path(__file__).parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.config_loader import load_config, get_project_root
from utils.prompt2dag_schema import validate_step1_output, Step1Output, Step2IntermediateYAML
from utils.yaml_helpers import save_yaml
from orchestrator_mappers.airflow_mapper import AirflowMapper
from orchestrator_mappers.prefect_mapper import PrefectMapper  
from orchestrator_mappers.dagster_mapper import DagsterMapper 

# --- Constants ---
DEFAULT_CONFIG = {
    "logging": {
        "level": "INFO",
        "log_file": "outputs/logs/step2_orchestrator_generation.log"
    }
}

SUPPORTED_ORCHESTRATORS = ['airflow', 'prefect', 'dagster']

MAPPER_CLASSES = {
    'airflow': AirflowMapper,
    'prefect': PrefectMapper,
    'dagster': DagsterMapper
}

# --- Logging Setup ---
def setup_logging(config: Dict, output_dir: str):
    """Setup logging."""
    log_config = config.get('logging', DEFAULT_CONFIG['logging'])
    log_level = log_config.get('level', 'INFO').upper()
    
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = Path(get_project_root()) / output_dir
    
    log_file = output_path / "step2_generation.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    logging.info(f"Logging initialized. Level: {log_level}, File: {log_file}")

# --- Core Functions ---
def load_and_validate_step1(input_file: Path) -> Step1Output:
    """Load and validate Step 1 JSON output."""
    logging.info(f"Loading Step 1 output from: {input_file}")
    
    if not input_file.exists():
        raise FileNotFoundError(f"Step 1 output file not found: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    # DEBUG: Check raw data before validation
    flow_nodes_raw = raw_data.get('flow_structure', {}).get('nodes', {})
    logging.info(f"Raw JSON has {len(flow_nodes_raw)} nodes before validation")
    
    logging.info("Validating Step 1 output against schema...")
    validated_data = validate_step1_output(raw_data)
    
    # DEBUG: Check after validation
    logging.info(f"After validation: {len(validated_data.flow_structure.nodes)} nodes")
    
    logging.info(f"Step 1 output validated successfully. Pipeline: {validated_data.pipeline_summary.name}")
    return validated_data

def get_mapper(target: str, step1_data: Step1Output, mappings_dir: Path):
    """Instantiate the appropriate mapper for the target orchestrator."""
    if target not in MAPPER_CLASSES:
        raise ValueError(f"Orchestrator '{target}' not yet implemented. Supported: {list(MAPPER_CLASSES.keys())}")
    
    mapping_file = mappings_dir / f"{target}_mapping.yaml"
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    
    mapper_class = MAPPER_CLASSES[target]
    logging.info(f"Initializing {mapper_class.__name__} with {mapping_file}")
    
    return mapper_class(step1_data, mapping_file)

def generate_intermediate_yaml(mapper, output_dir: Path, pipeline_name: str) -> Path:
    """Generate and save intermediate YAML."""
    logging.info("Generating intermediate YAML...")
    
    intermediate = mapper.generate_intermediate_yaml()
    
    # Save YAML
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    yaml_filename = f"{pipeline_name}_{mapper.get_orchestrator_name()}_intermediate_{timestamp}.yaml"
    yaml_path = output_dir / yaml_filename
    
    save_yaml(intermediate.model_dump(), yaml_path)
    
    logging.info(f"Intermediate YAML saved: {yaml_path}")
    return yaml_path

def generate_code(mapper, intermediate_yaml_path: Path, output_dir: Path):
    """Generate executable orchestrator code from intermediate YAML."""
    logging.info("Code generation step (TODO in next phase)")
    # This will be implemented with Jinja2 templates in the next phase
    # For now, just log
    logging.warning("Code generation not yet implemented. Intermediate YAML is ready for manual review.")

# --- Main ---
def main():
    parser = argparse.ArgumentParser(
        description='Step 2: Generate orchestrator-specific pipeline from Step 1 analysis',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--input', required=True,
                        help='Path to Step 1 JSON output file')
    parser.add_argument('--target', required=True, choices=SUPPORTED_ORCHESTRATORS,
                        help='Target orchestrator')
    parser.add_argument('--output-dir', default='outputs_step2',
                        help='Output directory for generated files')
    parser.add_argument('--config', default=None,
                        help='Optional config file for logging/settings')
    parser.add_argument('--mappings-dir', default=None,
                        help='Directory containing mapping YAML files (default: project_root/mappings)')
    
    args = parser.parse_args()
    
    # Setup
    config = load_config(args.config, DEFAULT_CONFIG)
    setup_logging(config, args.output_dir)
    
    logging.info("="*60)
    logging.info("Step 2: Orchestrator-Specific Pipeline Generation")
    logging.info(f"Target Orchestrator: {args.target}")
    logging.info("="*60)
    
    try:
        # Resolve paths
        input_file = Path(args.input)
        if not input_file.is_absolute():
            input_file = Path(get_project_root()) / input_file
        
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = Path(get_project_root()) / output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        mappings_dir = Path(args.mappings_dir) if args.mappings_dir else Path(get_project_root()) / 'mappings'
        
        # Step 2A: Load and validate Step 1 output
        step1_data = load_and_validate_step1(input_file)
        
        # Step 2B: Initialize mapper
        mapper = get_mapper(args.target, step1_data, mappings_dir)
        
        # Step 2C: Generate intermediate YAML
        intermediate_yaml_path = generate_intermediate_yaml(
            mapper,
            output_dir,
            step1_data.pipeline_summary.name
        )
        
        # Step 2D: Generate code (TODO)
        generate_code(mapper, intermediate_yaml_path, output_dir)
        
        # Success
        logging.info("="*60)
        logging.info("Step 2 Completed Successfully!")
        logging.info(f"Intermediate YAML: {intermediate_yaml_path}")
        logging.info(f"Target Orchestrator: {args.target}")
        logging.info("="*60)
        
        print(f"\n✓ Intermediate YAML generated: {intermediate_yaml_path}")
        print(f"✓ Target orchestrator: {args.target}")
        print(f"✓ Next: Code generation (coming soon)")
        
    except Exception as e:
        logging.error(f"FATAL ERROR: {e}", exc_info=True)
        print(f"\n✗ Error: {e}", file=sys.stderr)
        print("See log file for details.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()