#!/usr/bin/env python3
"""
Step 3: Code Generation
========================
Generates executable orchestrator code from intermediate YAML using multiple strategies.

Strategies:
1. Template-based: Pure Jinja2 templates (deterministic)
2. LLM-based: Pure LLM generation (flexible)
3. Hybrid: Templates for tasks + LLM for assembly (balanced)

Usage:
    # Run all strategies:
    python Step_3_code_generator.py \
        --input outputs_step2/airflow/pipeline_intermediate.yaml \
        --output-dir outputs_step3 \
        --strategies all

    # Run specific strategy:
    python Step_3_code_generator.py \
        --input outputs_step2/airflow/pipeline_intermediate.yaml \
        --output-dir outputs_step3 \
        --strategies template

    # Run multiple strategies:
    python Step_3_code_generator.py \
        --input outputs_step2/airflow/pipeline_intermediate.yaml \
        --output-dir outputs_step3 \
        --strategies template llm
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path
script_dir = Path(__file__).parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from utils.config_loader import load_config, get_project_root
from utils.llm_provider import LLMProvider  # Updated import

from code_generators.base_generator import (
    BaseCodeGenerator, GenerationResult, IntermediateYAML
)
from code_generators.template_generator import TemplateGenerator
from code_generators.llm_generator import LLMGenerator
from code_generators.hybrid_generator import HybridGenerator


# --- Constants ---
AVAILABLE_STRATEGIES = ['template', 'llm', 'hybrid']

DEFAULT_CONFIG = {
    "logging": {
        "level": "INFO"
    },
    "generation": {
        "default_retries": 1,
        "default_retry_delay": 5,
        "host_data_dir": "/tmp/airflow/data"
    }
}


def setup_logging(log_level: str, output_dir: Path):
    """Setup logging configuration."""
    log_file = output_dir / "step3_generation.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    logging.info(f"Logging initialized. Level: {log_level}, File: {log_file}")


def load_intermediate_yaml(yaml_path: Path) -> IntermediateYAML:
    """Load intermediate YAML file."""
    import yaml
    
    logging.info(f"Loading intermediate YAML: {yaml_path}")
    
    with open(yaml_path, 'r', encoding='utf-8') as f:
        raw_data = yaml.safe_load(f)
    
    return IntermediateYAML(
        metadata=raw_data.get('metadata', {}),
        schedule=raw_data.get('schedule', {}),
        connections=raw_data.get('connections', []),
        tasks=raw_data.get('tasks', []),
        raw_data=raw_data
    )


def initialize_llm_provider(
    llm_config_path: Path,
    provider_name: Optional[str],
    model_alias: Optional[str]
) -> Optional[LLMProvider]:
    """
    Initialize LLM provider with proper configuration.
    
    Args:
        llm_config_path: Path to LLM config JSON
        provider_name: Provider name override (e.g., 'deepinfra')
        model_alias: Model alias override (e.g., 'qwen')
        
    Returns:
        Initialized LLMProvider or None if initialization fails
    """
    try:
        # Load LLM config
        with open(llm_config_path, 'r') as f:
            llm_config = json.load(f)
        
        # Override active provider if specified
        if provider_name:
            llm_config['model_settings']['active_provider'] = provider_name
            logging.info(f"Overriding active provider to: {provider_name}")
        
        # Create LLMProvider instance
        llm_provider = LLMProvider(llm_config, model_key=model_alias)
        
        model_info = llm_provider.get_model_info()
        logging.info(
            f"Initialized LLM provider: {model_info['provider']} / "
            f"{model_info['model_key'] or 'default'} / {model_info['model_name']}"
        )
        
        return llm_provider
        
    except FileNotFoundError:
        logging.error(f"LLM config file not found: {llm_config_path}")
        return None
    except Exception as e:
        logging.error(f"Failed to initialize LLM provider: {e}", exc_info=True)
        return None


def initialize_generators(
    strategies: List[str],
    templates_dir: Path,
    output_dir: Path,
    llm_provider: Optional[LLMProvider],
    config: Dict
) -> Dict[str, BaseCodeGenerator]:
    """Initialize requested generators."""
    generators = {}
    
    for strategy in strategies:
        if strategy == 'template':
            generators['template'] = TemplateGenerator(
                templates_dir=templates_dir,
                output_dir=output_dir,
                config=config.get('generation', {})
            )
        
        elif strategy == 'llm':
            if llm_provider is None:
                logging.warning("LLM provider not configured, skipping LLM strategy")
                continue
            generators['llm'] = LLMGenerator(
                templates_dir=templates_dir,
                output_dir=output_dir,
                llm_provider=llm_provider,
                config=config.get('generation', {})
            )
        
        elif strategy == 'hybrid':
            if llm_provider is None:
                logging.warning("LLM provider not configured, skipping Hybrid strategy")
                continue
            generators['hybrid'] = HybridGenerator(
                templates_dir=templates_dir,
                output_dir=output_dir,
                llm_provider=llm_provider,
                config=config.get('generation', {})
            )
    
    return generators


def run_all_generators(
    generators: Dict[str, BaseCodeGenerator],
    intermediate_yaml: IntermediateYAML
) -> Dict[str, GenerationResult]:
    """Run all initialized generators and collect results."""
    results = {}
    
    for strategy_name, generator in generators.items():
        logging.info(f"\n{'='*60}")
        logging.info(f"Running {strategy_name.upper()} generator")
        logging.info(f"{'='*60}")
        
        try:
            result = generator.generate(intermediate_yaml)
            results[strategy_name] = result
            
            if result.success:
                logging.info(f"✓ {strategy_name} generation successful")
                logging.info(f"  Output: {result.output_path}")
                logging.info(f"  Time: {result.generation_time_ms:.2f}ms")
            else:
                logging.error(f"✗ {strategy_name} generation failed")
                for error in result.errors:
                    logging.error(f"  Error: {error}")
        
        except Exception as e:
            logging.error(f"✗ {strategy_name} generator crashed: {e}", exc_info=True)
            results[strategy_name] = GenerationResult(
                strategy=strategy_name,
                orchestrator=intermediate_yaml.orchestrator,
                success=False,
                errors=[str(e)]
            )
    
    return results


def generate_report(
    results: Dict[str, GenerationResult],
    intermediate_yaml: IntermediateYAML,
    output_dir: Path
) -> Path:
    """Generate comparison report."""
    report = {
        'generation_timestamp': datetime.now().isoformat(),
        'pipeline_name': intermediate_yaml.pipeline_name,
        'orchestrator': intermediate_yaml.orchestrator,
        'detected_pattern': intermediate_yaml.detect_pattern(),
        'task_count': len(intermediate_yaml.tasks),
        'strategies': {}
    }
    
    for strategy_name, result in results.items():
        report['strategies'][strategy_name] = result.to_dict()
    
    # Summary stats
    successful = sum(1 for r in results.values() if r.success)
    report['summary'] = {
        'total_strategies': len(results),
        'successful': successful,
        'failed': len(results) - successful,
        'fastest_ms': min(
            (r.generation_time_ms for r in results.values() if r.success),
            default=0
        ),
        'total_tokens': sum(
            sum(r.token_usage.values()) for r in results.values()
        )
    }
    
    # Save report
    report_path = output_dir / 'generation_report.json'
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)
    
    logging.info(f"Generation report saved: {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(
        description='Step 3: Generate orchestrator code using multiple strategies',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--input', required=True,
        help='Path to intermediate YAML file from Step 2'
    )
    parser.add_argument(
        '--output-dir', default='outputs_step3',
        help='Output directory for generated code'
    )
    parser.add_argument(
        '--templates-dir', default=None,
        help='Templates directory (default: scripts/templates)'
    )
    parser.add_argument(
        '--strategies', nargs='+', default=['all'],
        choices=['all', 'template', 'llm', 'hybrid'],
        help='Generation strategies to run'
    )
    parser.add_argument(
        '--llm-config', default='config_llm.json',
        help='LLM configuration file'
    )
    parser.add_argument(
        '--provider', default=None,
        help='LLM provider name (deepinfra, openai, claude, etc.)'
    )
    parser.add_argument(
        '--model', default=None,
        help='Model alias (qwen, llama3, etc.)'
    )
    parser.add_argument(
        '--config', default=None,
        help='Optional config file for generation settings'
    )
    parser.add_argument(
        '--log-level', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Resolve paths
    project_root = Path(get_project_root())
    
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = project_root / input_path
    
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = project_root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    templates_dir = Path(args.templates_dir) if args.templates_dir else script_dir / 'templates'
    if not templates_dir.is_absolute():
        templates_dir = project_root / templates_dir
    
    # Setup logging
    setup_logging(args.log_level, output_dir)
    
    logging.info("="*60)
    logging.info("STEP 3: CODE GENERATION")
    logging.info("="*60)
    logging.info(f"Input: {input_path}")
    logging.info(f"Output: {output_dir}")
    logging.info(f"Templates: {templates_dir}")
    logging.info(f"Strategies: {args.strategies}")
    
    # Load config
    config = load_config(args.config, DEFAULT_CONFIG)
    
    # Determine strategies to run
    strategies = args.strategies
    if 'all' in strategies:
        strategies = AVAILABLE_STRATEGIES.copy()
    
    logging.info(f"Running strategies: {strategies}")
    
    # Initialize LLM provider if needed
    llm_provider = None
    if 'llm' in strategies or 'hybrid' in strategies:
        llm_config_path = project_root / args.llm_config
        llm_provider = initialize_llm_provider(
            llm_config_path=llm_config_path,
            provider_name=args.provider,
            model_alias=args.model
        )
        
        if llm_provider is None:
            logging.warning("LLM and Hybrid strategies will be skipped")
    
    try:
        # Load intermediate YAML
        intermediate_yaml = load_intermediate_yaml(input_path)
        
        logging.info(f"Pipeline: {intermediate_yaml.pipeline_name}")
        logging.info(f"Orchestrator: {intermediate_yaml.orchestrator}")
        logging.info(f"Pattern: {intermediate_yaml.detect_pattern()}")
        logging.info(f"Tasks: {len(intermediate_yaml.tasks)}")
        
        # Initialize generators
        generators = initialize_generators(
            strategies=strategies,
            templates_dir=templates_dir,
            output_dir=output_dir,
            llm_provider=llm_provider,
            config=config
        )
        
        if not generators:
            logging.error("No generators initialized!")
            sys.exit(1)
        
        # Run all generators
        results = run_all_generators(generators, intermediate_yaml)
        
        # Generate report
        report_path = generate_report(results, intermediate_yaml, output_dir)
        
        # Print summary
        print("\n" + "="*60)
        print("GENERATION SUMMARY")
        print("="*60)
        
        for strategy_name, result in results.items():
            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print(f"\n{strategy_name.upper()}: {status}")
            if result.success:
                print(f"  Output: {result.output_path}")
                print(f"  Time: {result.generation_time_ms:.2f}ms")
            else:
                for error in result.errors:
                    print(f"  Error: {error}")
            
            if result.warnings:
                for warning in result.warnings:
                    print(f"  Warning: {warning}")
        
        print(f"\nReport: {report_path}")
        
        # Exit with error if all failed
        if not any(r.success for r in results.values()):
            sys.exit(1)
        
    except Exception as e:
        logging.error(f"FATAL ERROR: {e}", exc_info=True)
        print(f"\n✗ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()