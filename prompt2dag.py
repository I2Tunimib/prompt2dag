#!/usr/bin/env python3
"""
Prompt2DAG Orchestrator - Main CLI Entry Point
================================================
Unified interface for all pipeline generation workflows:
1. Direct Prompting (baseline)
2. Prompt2DAG Pipeline (4-step analysis + generation)
3. Direct Prompting with Reasoning Models

Usage Examples:
--------------
# Direct prompting (baseline)
python prompt2dag.py direct \
    --input samples/etl_pipeline.txt \
    --orchestrator airflow \
    --output-dir outputs/direct

# Full prompt2dag pipeline (all orchestrators, all strategies)
python prompt2dag.py pipeline \
    --input samples/etl_pipeline.txt \
    --orchestrators airflow prefect dagster \
    --output-dir outputs/prompt2dag

# Reasoning model (baseline with reasoning)
python prompt2dag.py reasoning \
    --input samples/etl_pipeline.txt \
    --orchestrator airflow \
    --output-dir outputs/reasoning

# Comprehensive comparison
python prompt2dag.py all \
    --input samples/etl_pipeline.txt \
    --orchestrators airflow \
    --provider deepinfra \
    --model qwen \
    --reasoning-model DeepSeek_R1
"""

import os
import sys
import json
import argparse
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add project root to path
script_dir = Path(__file__).parent
project_root = script_dir.parent if script_dir.name == 'scripts' else script_dir
sys.path.insert(0, str(project_root))

from scripts.utils.config_loader import get_project_root, load_config


# ============================================================================
# CONSTANTS
# ============================================================================

ORCHESTRATORS = ['airflow', 'prefect', 'dagster']
GENERATION_STRATEGIES = ['template', 'llm', 'hybrid']

DEFAULT_LLM_CONFIG = 'config_llm.json'
REASONING_LLM_CONFIG = 'config_reasoning_llm.json'

STEP_SCRIPTS = {
    'direct': 'scripts/Step_0_direct_prompting.py',
    'step1': 'scripts/Step_1_pipeline_analyzer.py',
    'step2': 'scripts/Step_2_orchestrator_generator.py',
    'step3': 'scripts/Step_3_code_generator.py',
    'step4': 'scripts/Step_4_evaluate.py',
}

# Step 1 evaluators
STEP1_EVAL_SCRIPTS = {
    'graph_validation': 'scripts/step_evaluators/dag_graph_validation_cli.py',
    'semantic_analysis': 'scripts/step_evaluators/semantic_eval_step1_anl.py',
}


# ============================================================================
# OUTPUT STRUCTURE MANAGEMENT
# ============================================================================

class OutputStructure:
    """Manages structured output directories for all workflows."""
    
    def __init__(self, base_dir: Path, workflow: str, pipeline_name: str):
        self.base_dir = Path(base_dir)
        self.workflow = workflow
        self.pipeline_name = pipeline_name
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create root structure
        self.root = self.base_dir / f"{workflow}_{self.timestamp}"
        self.root.mkdir(parents=True, exist_ok=True)
        
        # ALWAYS create logs directory for ALL workflows
        self.logs = self.root / "logs"
        self.logs.mkdir(exist_ok=True)
        
        # Workflow-specific directories
        if workflow == 'direct':
            self._setup_direct()
        elif workflow == 'pipeline':
            self._setup_pipeline()
        elif workflow == 'reasoning':
            self._setup_reasoning()
        elif workflow == 'all':
            self._setup_all()
    
    def _setup_direct(self):
        """Setup structure for direct prompting and reasoning workflows."""
        self.outputs = self.root / "generated_code"
        self.outputs.mkdir(exist_ok=True)

        # Evaluation directory for Step 4
        self.evaluation = self.root / "evaluation"
        self.evaluation.mkdir(exist_ok=True)
        
        self.metadata = self.root / "metadata"
        self.metadata.mkdir(exist_ok=True)
    
    def _setup_pipeline(self):
        """Setup structure for full pipeline."""
        # Step 1: Analysis
        self.step1 = self.root / "step1_analysis"
        self.step1.mkdir(exist_ok=True)
        
        # Step 2: Intermediate YAML per orchestrator
        self.step2 = self.root / "step2_intermediate"
        self.step2.mkdir(exist_ok=True)
        
        # Step 3: Generated code (orchestrator/strategy/files)
        self.step3 = self.root / "step3_generated"
        self.step3.mkdir(exist_ok=True)
        
        # Step 4: Evaluation results
        self.step4 = self.root / "step4_evaluation"
        self.step4.mkdir(exist_ok=True)
        
        # Final summary
        self.summary = self.root / "summary"
        self.summary.mkdir(exist_ok=True)
    
    def _setup_reasoning(self):
        """Setup structure for reasoning model."""
        # Same layout as direct (generated_code + evaluation + metadata)
        self._setup_direct()
    
    def _setup_all(self):
        """Setup structure for comprehensive workflow."""
        # For 'all' workflow, we'll create subdirectories for each workflow type
        self.direct_dir = self.root / "1_direct"
        self.pipeline_dir = self.root / "2_prompt2dag" 
        self.reasoning_dir = self.root / "3_reasoning"
        
        # Create all subdirectories
        self.direct_dir.mkdir(exist_ok=True)
        self.pipeline_dir.mkdir(exist_ok=True)
        self.reasoning_dir.mkdir(exist_ok=True)
        
        self.metadata = self.root / "metadata"
        self.metadata.mkdir(exist_ok=True)
    
    def get_orchestrator_dir(self, orchestrator: str, step: str = "step3") -> Path:
        """Get orchestrator-specific directory."""
        if step == "step3":
            orch_dir = self.step3 / orchestrator
            orch_dir.mkdir(exist_ok=True)
            return orch_dir
        elif step == "step2":
            orch_dir = self.step2 / orchestrator
            orch_dir.mkdir(exist_ok=True)
            return orch_dir
        return self.root
    
    def get_strategy_dir(self, orchestrator: str, strategy: str) -> Path:
        """Get strategy-specific directory within orchestrator."""
        strat_dir = self.get_orchestrator_dir(orchestrator) / strategy
        strat_dir.mkdir(exist_ok=True)
        return strat_dir
    
    def save_metadata(self, data: Dict):
        """Save workflow metadata."""
        meta_file = self.metadata / "workflow_metadata.json"
        with open(meta_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logging.info(f"Metadata saved: {meta_file}")


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(log_dir: Path, level: str = "INFO"):
    """Setup logging for orchestrator."""
    log_file = log_dir / f"orchestrator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    logging.info(f"Orchestrator logging initialized: {log_file}")


# ============================================================================
# WORKFLOW EXECUTORS
# ============================================================================

def run_script(script_path: str, args: List[str], log_dir: Path) -> Tuple[int, str]:
    """Execute a Python script with arguments and capture output."""
    cmd = [sys.executable, script_path] + args
    
    logging.info(f"Executing: {' '.join(cmd)}")
    
    log_file = log_dir / f"{Path(script_path).stem}_{datetime.now().strftime('%H%M%S')}.log"
    
    try:
        with open(log_file, 'w') as f:
            result = subprocess.run(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=project_root
            )
        
        with open(log_file, 'r') as f:
            output = f.read()
        
        if result.returncode == 0:
            logging.info(f"✓ Script completed successfully")
        else:
            logging.error(f"✗ Script failed with code {result.returncode}")
        
        return result.returncode, output
    
    except Exception as e:
        logging.error(f"Failed to execute script: {e}", exc_info=True)
        return 1, str(e)


def extract_output_path(output: str, marker: str = "main_output_path:") -> Optional[str]:
    """Extract output file path from script output."""
    for line in output.split('\n'):
        if marker in line:
            return line.split(marker, 1)[1].strip()
    return None


# ============================================================================
# DIRECT PROMPTING WORKFLOW (WITH EVALUATION)
# ============================================================================

def run_direct_prompting(args, output_struct: OutputStructure) -> Dict:
    """Execute direct prompting workflow + Step 4 evaluation."""
    logging.info("="*60)
    logging.info("WORKFLOW: DIRECT PROMPTING (BASELINE)")
    logging.info("="*60)
    
    results: Dict[str, Dict] = {}
    
    # ------------------------------------------------------------------------
    # Step 0: Generate DAG via direct prompting
    # ------------------------------------------------------------------------
    script_args = [
        '--input', str(args.input),
        '--output-dir', str(output_struct.outputs),
        '--orchestrator', args.orchestrator,
        '--config', args.llm_config,
    ]
    
    if args.provider:
        script_args.extend(['--provider', args.provider])
    if args.model:
        script_args.extend(['--model', args.model])
    
    returncode, output = run_script(
        STEP_SCRIPTS['direct'],
        script_args,
        output_struct.logs
    )
    
    output_file = extract_output_path(output)
    
    results['direct_prompting'] = {
        'success': returncode == 0 and output_file is not None,
        'orchestrator': args.orchestrator,
        'output_file': output_file,
        'log_snippet': output[-500:] if output else ""
    }
    
    # ------------------------------------------------------------------------
    # Step 4: Evaluate the generated DAG (if any)
    # ------------------------------------------------------------------------
    eval_result = {
        'success': False,
        'summary_file': None,
        'files_evaluated': 0
    }
    
    if results['direct_prompting']['success'] and output_file:
        logging.info("\n" + "="*60)
        logging.info("STEP 4: EVALUATION (DIRECT PROMPTING)")
        logging.info("="*60)
        
        eval_dir = output_struct.evaluation
        eval_dir.mkdir(parents=True, exist_ok=True)
        
        step4_args = [
            '--file', output_file,
            '--output-dir', str(eval_dir),
            '--orchestrator', args.orchestrator,
            '--log-level', args.log_level
        ]
        
        ev_returncode, ev_output = run_script(
            STEP_SCRIPTS['step4'],
            step4_args,
            output_struct.logs
        )
        
        summary_file = None
        for f in eval_dir.glob("evaluation_summary_*.json"):
            summary_file = f
            break
        
        eval_result['success'] = (ev_returncode == 0 and summary_file is not None)
        eval_result['summary_file'] = str(summary_file) if summary_file else None
        eval_result['files_evaluated'] = 1 if eval_result['success'] else 0
        
        if eval_result['success']:
            logging.info(f"✓ Direct prompting DAG evaluation complete: {summary_file}")
        else:
            logging.error("✗ Direct prompting DAG evaluation failed or summary missing")
    else:
        logging.warning("Skipping evaluation for direct prompting (no DAG file generated).")
    
    results['direct_prompting']['evaluation'] = eval_result
    
    return results


# ============================================================================
# REASONING MODEL WORKFLOW (WITH EVALUATION)
# ============================================================================

def run_reasoning_workflow(args, output_struct: OutputStructure) -> Dict:
    """Execute reasoning model workflow + Step 4 evaluation."""
    logging.info("="*60)
    logging.info("WORKFLOW: REASONING MODEL")
    logging.info("="*60)
    
    results: Dict[str, Dict] = {}
    
    # ------------------------------------------------------------------------
    # Step 0: Generate DAG via reasoning model
    # ------------------------------------------------------------------------
    script_args = [
        '--input', str(args.input),
        '--output-dir', str(output_struct.outputs),
        '--orchestrator', args.orchestrator,
        '--config', args.reasoning_config,
    ]
    
    if args.provider:
        script_args.extend(['--provider', args.provider])
    if args.model:
        script_args.extend(['--model', args.model])
    
    returncode, output = run_script(
        STEP_SCRIPTS['direct'],
        script_args,
        output_struct.logs
    )
    
    output_file = extract_output_path(output)
    
    results['reasoning'] = {
        'success': returncode == 0 and output_file is not None,
        'orchestrator': args.orchestrator,
        'output_file': output_file,
        'reasoning_model': True
    }
    
    # ------------------------------------------------------------------------
    # Step 4: Evaluate the reasoning-based DAG (if any)
    # ------------------------------------------------------------------------
    eval_result = {
        'success': False,
        'summary_file': None,
        'files_evaluated': 0
    }
    
    if results['reasoning']['success'] and output_file:
        logging.info("\n" + "="*60)
        logging.info("STEP 4: EVALUATION (REASONING MODEL)")
        logging.info("="*60)
        
        eval_dir = output_struct.evaluation
        eval_dir.mkdir(parents=True, exist_ok=True)
        
        step4_args = [
            '--file', output_file,
            '--output-dir', str(eval_dir),
            '--orchestrator', args.orchestrator,
            '--log-level', args.log_level
        ]
        
        ev_returncode, ev_output = run_script(
            STEP_SCRIPTS['step4'],
            step4_args,
            output_struct.logs
        )
        
        summary_file = None
        for f in eval_dir.glob("evaluation_summary_*.json"):
            summary_file = f
            break
        
        eval_result['success'] = (ev_returncode == 0 and summary_file is not None)
        eval_result['summary_file'] = str(summary_file) if summary_file else None
        eval_result['files_evaluated'] = 1 if eval_result['success'] else 0
        
        if eval_result['success']:
            logging.info(f"✓ Reasoning DAG evaluation complete: {summary_file}")
        else:
            logging.error("✗ Reasoning DAG evaluation failed or summary missing")
    else:
        logging.warning("Skipping evaluation for reasoning model (no DAG file generated).")
    
    results['reasoning']['evaluation'] = eval_result
    
    return results


# ============================================================================
# FULL PIPELINE WORKFLOW (WITH STEP 1 EVALUATORS)
# ============================================================================

def run_full_pipeline(args, output_struct: OutputStructure) -> Dict:
    """Execute full 4-step prompt2dag pipeline."""
    logging.info("="*60)
    logging.info("WORKFLOW: FULL PROMPT2DAG PIPELINE")
    logging.info("="*60)
    
    results = {
        'step1': {},
        'step2': {},
        'step3': {},
        'step4': {}
    }
    
    # ========================================================================
    # STEP 1: ANALYZE PIPELINE (ORCHESTRATOR-AGNOSTIC)
    # ========================================================================
    logging.info("\n" + "="*60)
    logging.info("STEP 1: PIPELINE ANALYSIS (ORCHESTRATOR-AGNOSTIC)")
    logging.info("="*60)
    
    step1_args = [
        '--input', str(args.input),
        '--output-dir', str(output_struct.step1),
        '--llm-config', args.llm_config,
    ]
    
    if args.provider:
        step1_args.extend(['--provider', args.provider])
    if args.model:
        step1_args.extend(['--model', args.model])
    
    returncode, output = run_script(
        STEP_SCRIPTS['step1'],
        step1_args,
        output_struct.logs
    )
    
    if returncode != 0:
        logging.error("Step 1 failed. Aborting pipeline.")
        results['step1']['success'] = False
        return results
    
    # Find Step 1 output JSON
    step1_json = None
    for f in output_struct.step1.glob("pipeline_analysis_s1_*.json"):
        step1_json = f
        break
    
    if not step1_json:
        logging.error("Step 1 JSON output not found")
        results['step1']['success'] = False
        return results
    
    results['step1'] = {
        'success': True,
        'output_json': str(step1_json),
        'evaluations': {}
    }
    
    logging.info(f"✓ Step 1 complete: {step1_json}")

    # ------------------------------------------------------------------------
    # STEP 1 EVALUATIONS:
    #  1. DAG Graph Validation (on Step 1 JSON)
    #  2. Semantic Evaluation (original prompt vs Step 1 .txt)
    # ------------------------------------------------------------------------
    # 1) DAG graph validation
    graph_eval_dir = output_struct.step1 / "graph_validation"
    graph_eval_dir.mkdir(exist_ok=True)

    graph_report_path = graph_eval_dir / f"dag_validation_{step1_json.stem}.json"
    graph_args = [
        '--input', str(step1_json),
        '--output-json', str(graph_report_path),
        '--no-print'
    ]
    gv_returncode, gv_output = run_script(
        STEP1_EVAL_SCRIPTS['graph_validation'],
        graph_args,
        output_struct.logs
    )
    graph_success = (gv_returncode == 0 and graph_report_path.exists())
    results['step1']['evaluations']['graph_validation'] = {
        'success': graph_success,
        'output_json': str(graph_report_path) if graph_report_path.exists() else None,
        'returncode': gv_returncode
    }
    if graph_success:
        logging.info(f"✓ Step 1 DAG graph validation report: {graph_report_path}")
    else:
        logging.error("✗ Step 1 DAG graph validation failed or report missing")

    # 2) Semantic evaluation (original description vs analysis .txt)
    semantic_eval_dir = output_struct.step1 / "semantic_eval"
    semantic_eval_dir.mkdir(exist_ok=True)

    step1_text = step1_json.with_suffix(".txt")
    if not step1_text.exists():
        # Fallback: try to find any *.txt in step1_analysis
        txt_candidates = list(output_struct.step1.glob("*.txt"))
        if txt_candidates:
            step1_text = txt_candidates[0]

    if step1_text.exists():
        semantic_report_path = semantic_eval_dir / f"semantic_eval_{step1_json.stem}.json"
        sem_args = [
            '--original', str(args.input),
            '--generated', str(step1_text),
            '--task-type', 'analysis',
            '--use-bertscore',
            '--output-json', str(semantic_report_path),
            '--no-print'
        ]
        sem_returncode, sem_output = run_script(
            STEP1_EVAL_SCRIPTS['semantic_analysis'],
            sem_args,
            output_struct.logs
        )
        sem_success = (sem_returncode == 0 and semantic_report_path.exists())
        results['step1']['evaluations']['semantic_eval'] = {
            'success': sem_success,
            'output_json': str(semantic_report_path) if semantic_report_path.exists() else None,
            'returncode': sem_returncode
        }
        if sem_success:
            logging.info(f"✓ Step 1 semantic evaluation report: {semantic_report_path}")
        else:
            logging.error("✗ Step 1 semantic evaluation failed or report missing")
    else:
        logging.warning(f"No Step 1 analysis text file found for semantic evaluation in {output_struct.step1}")
        results['step1']['evaluations']['semantic_eval'] = {
            'success': False,
            'output_json': None,
            'returncode': None,
            'reason': 'analysis_text_missing'
        }
    
    # ========================================================================
    # STEP 2: GENERATE INTERMEDIATE YAML (PER ORCHESTRATOR)
    # ========================================================================
    for orchestrator in args.orchestrators:
        logging.info("\n" + "="*60)
        logging.info(f"STEP 2: INTERMEDIATE YAML - {orchestrator.upper()}")
        logging.info("="*60)
        
        orch_dir = output_struct.get_orchestrator_dir(orchestrator, 'step2')
        
        step2_args = [
            '--input', str(step1_json),
            '--target', orchestrator,
            '--output-dir', str(orch_dir),
        ]
        
        returncode, output = run_script(
            STEP_SCRIPTS['step2'],
            step2_args,
            output_struct.logs
        )
        
        # Find intermediate YAML
        yaml_file = None
        for f in orch_dir.glob("*_intermediate_*.yaml"):
            yaml_file = f
            break
        
        results['step2'][orchestrator] = {
            'success': returncode == 0 and yaml_file is not None,
            'intermediate_yaml': str(yaml_file) if yaml_file else None
        }
        
        if returncode == 0 and yaml_file is not None:
            logging.info(f"✓ Step 2 complete ({orchestrator}): {yaml_file}")
        else:
            logging.error(f"✗ Step 2 failed ({orchestrator}) - no intermediate YAML found")
    
    # ========================================================================
    # STEP 3: GENERATE CODE (ALL STRATEGIES PER ORCHESTRATOR)
    # ========================================================================
    for orchestrator in args.orchestrators:
        step2_result = results['step2'].get(orchestrator, {})
        yaml_file = step2_result.get('intermediate_yaml')
        
        if not yaml_file or not step2_result.get('success'):
            logging.warning(f"Skipping Step 3 for {orchestrator} (no intermediate YAML)")
            continue
        
        logging.info("\n" + "="*60)
        logging.info(f"STEP 3: CODE GENERATION - {orchestrator.upper()} (ALL STRATEGIES)")
        logging.info("="*60)
        
        orch_output_dir = output_struct.get_orchestrator_dir(orchestrator, 'step3')
        
        step3_args = [
            '--input', yaml_file,
            '--output-dir', str(orch_output_dir),
            '--strategies', 'all',  # Generate all three strategies
            '--llm-config', args.llm_config,
        ]
        
        if args.provider:
            step3_args.extend(['--provider', args.provider])
        if args.model:
            step3_args.extend(['--model', args.model])
        
        returncode, output = run_script(
            STEP_SCRIPTS['step3'],
            step3_args,
            output_struct.logs
        )
        
        # Collect generated files per strategy (handle nested layout).
        strategy_outputs: Dict[str, List[str]] = {}
        for strategy in GENERATION_STRATEGIES:
            py_files = list(orch_output_dir.glob(f"**/{strategy}/*.py"))
            strategy_outputs[strategy] = [str(f) for f in py_files]
            if py_files:
                logging.info(
                    f"Found {len(py_files)} DAG(s) for {orchestrator} strategy '{strategy}': "
                    f"{[Path(p).name for p in py_files]}"
                )
            else:
                logging.warning(
                    f"No DAGs found for {orchestrator} strategy '{strategy}' under {orch_output_dir}"
                )
        
        results['step3'][orchestrator] = {
            'success': returncode == 0,
            'strategies': strategy_outputs
        }
        
        if returncode == 0:
            logging.info(f"✓ Step 3 complete ({orchestrator})")
        else:
            logging.error(f"✗ Step 3 failed ({orchestrator})")
    
    # ========================================================================
    # STEP 4: EVALUATE ALL GENERATED CODE (STRUCTURED BY ORCH/STRATEGY)
    # ========================================================================
    logging.info("\n" + "="*60)
    logging.info("STEP 4: EVALUATION (ALL ORCHESTRATORS, ALL STRATEGIES)")
    logging.info("="*60)
    
    results['step4'] = {
        'success': False,
        'total_files_evaluated': 0,
        'details': {}
    }
    
    any_files_to_evaluate = False
    any_evaluation_success = False
    
    for orchestrator in args.orchestrators:
        step3_result = results['step3'].get(orchestrator, {})
        strategy_map = step3_result.get('strategies', {})
        orch_details: Dict[str, Dict] = {}
        
        if not strategy_map:
            logging.warning(f"No generated code found for {orchestrator}; skipping evaluation.")
            continue
        
        for strategy, files in strategy_map.items():
            if not files:
                # No DAG for this strategy -> treat as not evaluated (implicit failure)
                logging.warning(
                    f"No DAG files generated for {orchestrator}/{strategy}; "
                    f"strategy will not be evaluated."
                )
                orch_details[strategy] = {
                    'success': False,
                    'summary_file': None,
                    'files_evaluated': 0
                }
                continue
            
            any_files_to_evaluate = True
            
            strategy_output_dir = output_struct.step4 / orchestrator / strategy
            strategy_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Build Step 4 arguments
            step4_args = [
                '--files'
            ] + files + [
                '--output-dir', str(strategy_output_dir),
                '--orchestrator', orchestrator,
                '--log-level', args.log_level
            ]
            
            returncode, output = run_script(
                STEP_SCRIPTS['step4'],
                step4_args,
                output_struct.logs
            )
            
            # Find evaluation summary for this orchestrator/strategy
            summary_file = None
            for f in strategy_output_dir.glob("evaluation_summary_*.json"):
                summary_file = f
                break
            
            # NOTE: Step_4_evaluate returns 1 if any file "fails" evaluation.
            # Here, we treat success strictly as "all passed" (returncode==0).
            success = (returncode == 0 and summary_file is not None)
            if success:
                any_evaluation_success = True
                logging.info(
                    f"✓ Evaluation complete for {orchestrator}/{strategy}: {summary_file}"
                )
            else:
                logging.error(
                    f"✗ Evaluation failed or summary missing for {orchestrator}/{strategy}"
                )
            
            orch_details[strategy] = {
                'success': success,
                'summary_file': str(summary_file) if summary_file else None,
                'files_evaluated': len(files)
            }
            
            results['step4']['total_files_evaluated'] += len(files)
        
        if orch_details:
            results['step4']['details'][orchestrator] = orch_details
    
    if not any_files_to_evaluate:
        logging.warning("No files to evaluate in Step 4.")
        results['step4']['success'] = False
    else:
        results['step4']['success'] = any_evaluation_success
        if any_evaluation_success:
            logging.info("✓ Step 4 evaluation completed for at least one file.")
        else:
            logging.error("✗ Step 4 evaluation ran but no evaluation succeeded.")
    
    return results


# ============================================================================
# COMPREHENSIVE WORKFLOW (ALL THREE)
# ============================================================================

def run_comprehensive_workflow(args, base_output_dir: Path) -> Dict:
    """Execute all three workflows for comprehensive comparison."""
    logging.info("="*80)
    logging.info("COMPREHENSIVE WORKFLOW - RUNNING ALL THREE APPROACHES")
    logging.info("="*80)
    logging.info(f"Input: {args.input}")
    logging.info(f"Orchestrators: {args.orchestrators}")
    logging.info(f"Standard Model: {args.provider}/{args.model}")
    logging.info(f"Reasoning Model: {args.reasoning_provider}/{args.reasoning_model}")
    logging.info("="*80)
    
    comprehensive_results = {
        'workflows': {},
        'comparison': {},
        'summary': {}
    }
    
    start_time = datetime.now()
    
    # ========================================================================
    # WORKFLOW 1: DIRECT PROMPTING (For each orchestrator)
    # ========================================================================
    logging.info("\n" + "="*80)
    logging.info("WORKFLOW 1/3: DIRECT PROMPTING (BASELINE)")
    logging.info("="*80)
    
    direct_results = {}
    
    for orchestrator in args.orchestrators:
        logging.info(f"\nRunning direct prompting for {orchestrator}...")
        
        # Create namespace for direct workflow
        direct_args = argparse.Namespace(
            input=args.input,
            orchestrator=orchestrator,
            output_dir=str(base_output_dir / "1_direct"),
            llm_config=args.llm_config,
            provider=args.provider,
            model=args.model,
            log_level=args.log_level,
            workflow='direct'
        )
        
        output_struct = OutputStructure(
            Path(direct_args.output_dir),
            'direct',
            f"{Path(args.input).stem}_{orchestrator}"
        )
        
        result = run_direct_prompting(direct_args, output_struct)
        direct_results[orchestrator] = {
            'result': result,
            'output_dir': str(output_struct.root)
        }
    
    comprehensive_results['workflows']['1_direct_prompting'] = direct_results
    
    # ========================================================================
    # WORKFLOW 2: FULL PROMPT2DAG PIPELINE
    # ========================================================================
    logging.info("\n" + "="*80)
    logging.info("WORKFLOW 2/3: FULL PROMPT2DAG PIPELINE")
    logging.info("="*80)
    
    pipeline_args = argparse.Namespace(
        input=args.input,
        orchestrators=args.orchestrators,
        output_dir=str(base_output_dir / "2_prompt2dag"),
        llm_config=args.llm_config,
        provider=args.provider,
        model=args.model,
        log_level=args.log_level,
        workflow='pipeline'
    )
    
    pipeline_output_struct = OutputStructure(
        Path(pipeline_args.output_dir),
        'pipeline',
        Path(args.input).stem
    )
    
    pipeline_result = run_full_pipeline(pipeline_args, pipeline_output_struct)
    
    comprehensive_results['workflows']['2_prompt2dag_pipeline'] = {
        'result': pipeline_result,
        'output_dir': str(pipeline_output_struct.root)
    }
    
    # ========================================================================
    # WORKFLOW 3: REASONING MODEL (For each orchestrator)
    # ========================================================================
    logging.info("\n" + "="*80)
    logging.info("WORKFLOW 3/3: REASONING MODEL")
    logging.info("="*80)
    
    reasoning_results = {}
    
    for orchestrator in args.orchestrators:
        logging.info(f"\nRunning reasoning model for {orchestrator}...")
        
        reasoning_args = argparse.Namespace(
            input=args.input,
            orchestrator=orchestrator,
            output_dir=str(base_output_dir / "3_reasoning"),
            reasoning_config=args.reasoning_config,
            provider=args.reasoning_provider,
            model=args.reasoning_model,
            log_level=args.log_level,
            workflow='reasoning'
        )
        
        output_struct = OutputStructure(
            Path(reasoning_args.output_dir),
            'reasoning',
            f"{Path(args.input).stem}_{orchestrator}"
        )
        
        result = run_reasoning_workflow(reasoning_args, output_struct)
        reasoning_results[orchestrator] = {
            'result': result,
            'output_dir': str(output_struct.root)
        }
    
    comprehensive_results['workflows']['3_reasoning_model'] = reasoning_results
    
    # ========================================================================
    # GENERATE COMPARISON SUMMARY
    # ========================================================================
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    
    logging.info("\n" + "="*80)
    logging.info("GENERATING COMPREHENSIVE COMPARISON SUMMARY")
    logging.info("="*80)
    
    # Count successes (based on generation success, not evaluation)
    direct_success = sum(
        1 for r in direct_results.values() 
        if r['result'].get('direct_prompting', {}).get('success', False)
    )
    
    pipeline_success = (
        1 if pipeline_result.get('step4', {}).get('success', False) 
        else 0
    )
    
    reasoning_success = sum(
        1 for r in reasoning_results.values()
        if r['result'].get('reasoning', {}).get('success', False)
    )
    
    # Collect all generated files for final comparison
    all_generated_files = []
    
    # From direct prompting
    for orch, data in direct_results.items():
        output_file = data['result'].get('direct_prompting', {}).get('output_file')
        if output_file:
            all_generated_files.append({
                'workflow': 'direct',
                'orchestrator': orch,
                'strategy': 'direct_prompting',
                'file': output_file
            })
    
    # From prompt2dag pipeline (all strategies)
    for orch in args.orchestrators:
        step3_data = pipeline_result.get('step3', {}).get(orch, {})
        for strategy, files in step3_data.get('strategies', {}).items():
            for file in files:
                all_generated_files.append({
                    'workflow': 'prompt2dag',
                    'orchestrator': orch,
                    'strategy': strategy,
                    'file': file
                })
    
    # From reasoning
    for orch, data in reasoning_results.items():
        output_file = data['result'].get('reasoning', {}).get('output_file')
        if output_file:
            all_generated_files.append({
                'workflow': 'reasoning',
                'orchestrator': orch,
                'strategy': 'reasoning_model',
                'file': output_file
            })
    
    comprehensive_results['summary'] = {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'total_duration_seconds': total_duration,
        'orchestrators': args.orchestrators,
        'workflows_executed': 3,
        'success_count': {
            'direct_prompting': f"{direct_success}/{len(args.orchestrators)}",
            'prompt2dag_pipeline': f"{pipeline_success}/1",
            'reasoning_model': f"{reasoning_success}/{len(args.orchestrators)}"
        },
        'total_files_generated': len(all_generated_files),
        'files_by_workflow': {
            'direct': sum(1 for f in all_generated_files if f['workflow'] == 'direct'),
            'prompt2dag': sum(1 for f in all_generated_files if f['workflow'] == 'prompt2dag'),
            'reasoning': sum(1 for f in all_generated_files if f['workflow'] == 'reasoning')
        },
        'all_generated_files': all_generated_files
    }
    
    comprehensive_results['comparison'] = {
        'by_orchestrator': {},
        'by_strategy': {}
    }
    
    # Group by orchestrator
    for orch in args.orchestrators:
        comprehensive_results['comparison']['by_orchestrator'][orch] = {
            'direct': next((f for f in all_generated_files 
                          if f['orchestrator'] == orch and f['workflow'] == 'direct'), None),
            'prompt2dag': [f for f in all_generated_files 
                          if f['orchestrator'] == orch and f['workflow'] == 'prompt2dag'],
            'reasoning': next((f for f in all_generated_files 
                             if f['orchestrator'] == orch and f['workflow'] == 'reasoning'), None)
        }
    
    # Group by strategy (for prompt2dag)
    for strategy in GENERATION_STRATEGIES:
        comprehensive_results['comparison']['by_strategy'][strategy] = [
            f for f in all_generated_files 
            if f.get('strategy') == strategy
        ]
    
    return comprehensive_results


# ============================================================================
# MAIN CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Prompt2DAG Orchestrator - Unified pipeline generation system',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    subparsers = parser.add_subparsers(dest='workflow', required=True)
    
    # ========================================================================
    # DIRECT PROMPTING SUBCOMMAND
    # ========================================================================
    direct_parser = subparsers.add_parser(
        'direct',
        help='Direct prompting baseline'
    )
    direct_parser.add_argument('--input', required=True, help='Pipeline description file')
    direct_parser.add_argument('--orchestrator', required=True, choices=ORCHESTRATORS)
    direct_parser.add_argument('--output-dir', default='outputs/direct')
    direct_parser.add_argument('--llm-config', default=DEFAULT_LLM_CONFIG)
    direct_parser.add_argument('--provider', choices=['deepinfra', 'openai', 'claude'])
    direct_parser.add_argument('--model', help='Model alias')
    direct_parser.add_argument('--log-level', default='INFO')
    
    # ========================================================================
    # FULL PIPELINE SUBCOMMAND
    # ========================================================================
    pipeline_parser = subparsers.add_parser(
        'pipeline',
        help='Full 4-step prompt2dag pipeline'
    )
    pipeline_parser.add_argument('--input', required=True, help='Pipeline description file')
    pipeline_parser.add_argument('--orchestrators', nargs='+', default=ORCHESTRATORS, choices=ORCHESTRATORS)
    pipeline_parser.add_argument('--output-dir', default='outputs/prompt2dag')
    pipeline_parser.add_argument('--llm-config', default=DEFAULT_LLM_CONFIG)
    pipeline_parser.add_argument('--provider', choices=['deepinfra', 'openai', 'claude'])
    pipeline_parser.add_argument('--model', help='Model alias')
    pipeline_parser.add_argument('--log-level', default='INFO')
    
    # ========================================================================
    # REASONING MODEL SUBCOMMAND
    # ========================================================================
    reasoning_parser = subparsers.add_parser(
        'reasoning',
        help='Direct prompting with reasoning models'
    )
    reasoning_parser.add_argument('--input', required=True, help='Pipeline description file')
    reasoning_parser.add_argument('--orchestrator', required=True, choices=ORCHESTRATORS)
    reasoning_parser.add_argument('--output-dir', default='outputs/reasoning')
    reasoning_parser.add_argument('--reasoning-config', default=REASONING_LLM_CONFIG)
    reasoning_parser.add_argument('--provider', choices=['deepinfra'])
    reasoning_parser.add_argument('--model', help='Reasoning model alias')
    reasoning_parser.add_argument('--log-level', default='INFO')
    
    # ========================================================================
    # ALL WORKFLOWS SUBCOMMAND (COMPREHENSIVE COMPARISON)
    # ========================================================================
    all_parser = subparsers.add_parser(
        'all',
        help='Run all three workflows for comprehensive comparison'
    )
    all_parser.add_argument('--input', required=True, help='Pipeline description file')
    all_parser.add_argument('--orchestrators', nargs='+', default=ORCHESTRATORS, choices=ORCHESTRATORS,
                           help='Target orchestrators (default: all)')
    all_parser.add_argument('--output-dir', default='outputs/comprehensive',
                           help='Base output directory for all workflows')
    all_parser.add_argument('--llm-config', default=DEFAULT_LLM_CONFIG,
                           help='LLM config for direct and pipeline workflows')
    all_parser.add_argument('--reasoning-config', default=REASONING_LLM_CONFIG,
                           help='LLM config for reasoning workflow')
    all_parser.add_argument('--provider', choices=['deepinfra', 'openai', 'claude'],
                           help='LLM provider for direct/pipeline workflows')
    all_parser.add_argument('--model', help='Model alias for direct/pipeline workflows')
    all_parser.add_argument('--reasoning-provider', default='deepinfra',
                           help='Provider for reasoning workflow')
    all_parser.add_argument('--reasoning-model', default='DeepSeek_R1',
                           help='Model alias for reasoning workflow')
    all_parser.add_argument('--log-level', default='INFO')
    
    args = parser.parse_args()
    
    # ========================================================================
    # SETUP
    # ========================================================================
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1
    
    pipeline_name = input_path.stem
    
    # Create output structure
    output_struct = OutputStructure(
        Path(args.output_dir),
        args.workflow,
        pipeline_name
    )
    
    # Setup logging
    setup_logging(output_struct.logs, args.log_level)
    
    logging.info("="*80)
    logging.info("PROMPT2DAG ORCHESTRATOR")
    logging.info("="*80)
    logging.info(f"Workflow: {args.workflow}")
    logging.info(f"Input: {input_path}")
    logging.info(f"Output root: {output_struct.root}")
    
    # ========================================================================
    # EXECUTE WORKFLOW
    # ========================================================================
    start_time = datetime.now()
    
    if args.workflow == 'direct':
        results = run_direct_prompting(args, output_struct)
    elif args.workflow == 'reasoning':
        results = run_reasoning_workflow(args, output_struct)
    elif args.workflow == 'pipeline':
        results = run_full_pipeline(args, output_struct)
    elif args.workflow == 'all':
        # For comprehensive workflow, use base output dir directly
        base_output_dir = Path(args.output_dir)
        if not base_output_dir.is_absolute():
            base_output_dir = project_root / base_output_dir
        
        # Create timestamped comprehensive directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        comprehensive_dir = base_output_dir / f"comprehensive_{timestamp}"
        comprehensive_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging for comprehensive run
        logs_dir = comprehensive_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        setup_logging(logs_dir, args.log_level)
        
        logging.info("="*80)
        logging.info("PROMPT2DAG COMPREHENSIVE COMPARISON")
        logging.info("="*80)
        logging.info(f"Input: {input_path}")
        logging.info(f"Output root: {comprehensive_dir}")
        
        # Run all workflows
        results = run_comprehensive_workflow(args, comprehensive_dir)
        
        # Save comprehensive metadata
        metadata_file = comprehensive_dir / "comprehensive_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logging.info(f"Comprehensive metadata saved: {metadata_file}")
        
        # Print detailed summary
        print("\n" + "="*80)
        print("COMPREHENSIVE COMPARISON SUMMARY")
        print("="*80)
        print(f"Total Duration: {results['summary']['total_duration_seconds']:.2f}s")
        print(f"Output Directory: {comprehensive_dir}")
        print()
        print("Workflows Executed:")
        print(f"  1. Direct Prompting:     {results['summary']['success_count']['direct_prompting']}")
        print(f"  2. Prompt2DAG Pipeline:  {results['summary']['success_count']['prompt2dag_pipeline']}")
        print(f"  3. Reasoning Model:      {results['summary']['success_count']['reasoning_model']}")
        print()
        print("Files Generated:")
        print(f"  Direct:       {results['summary']['files_by_workflow']['direct']} files")
        print(f"  Prompt2DAG:   {results['summary']['files_by_workflow']['prompt2dag']} files")
        print(f"  Reasoning:    {results['summary']['files_by_workflow']['reasoning']} files")
        print(f"  Total:        {results['summary']['total_files_generated']} files")
        print()
        print("Output Structure:")
        print(f"  {comprehensive_dir}/")
        print(f"  ├── 1_direct/               # Direct prompting outputs")
        print(f"  ├── 2_prompt2dag/           # Full pipeline (4 steps)")
        print(f"  ├── 3_reasoning/            # Reasoning model outputs")
        print(f"  ├── logs/                   # All workflow logs")
        print(f"  └── comprehensive_metadata.json")
        print()
        print("Next Steps:")
        print("  1. Review generated code in each workflow directory")
        print("  2. Check evaluation results in 2_prompt2dag/*/step4_evaluation/")
        print("  3. Compare outputs across workflows using metadata JSON")
        print("="*80)
        
        return 0
    else:
        logging.error(f"Unknown workflow: {args.workflow}")
        return 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # ========================================================================
    # SAVE METADATA
    # ========================================================================
    metadata = {
        'workflow': args.workflow,
        'input_file': str(input_path),
        'pipeline_name': pipeline_name,
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': duration,
        'output_root': str(output_struct.root),
        'results': results
    }
    
    if hasattr(output_struct, 'metadata'):
        output_struct.save_metadata(metadata)
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("PROMPT2DAG ORCHESTRATOR - EXECUTION SUMMARY")
    print("="*80)
    print(f"Workflow: {args.workflow}")
    print(f"Duration: {duration:.2f}s")
    print(f"Output: {output_struct.root}")
    print()
    
    if args.workflow == 'pipeline':
        print("Pipeline Steps:")
        print(f"  Step 1 (Analysis): {'✓' if results['step1'].get('success') else '✗'}")
        print("    - Graph validation: "
              f"{'✓' if results['step1']['evaluations'].get('graph_validation', {}).get('success') else '✗'}")
        print("    - Semantic eval:    "
              f"{'✓' if results['step1']['evaluations'].get('semantic_eval', {}).get('success') else '✗'}")
        print(f"  Step 2 (Intermediate YAML): "
              f"{sum(1 for r in results['step2'].values() if r.get('success'))}/"
              f"{len(args.orchestrators)} orchestrators")
        print(f"  Step 3 (Code Generation): "
              f"{sum(1 for r in results['step3'].values() if r.get('success'))}/"
              f"{len(args.orchestrators)} orchestrators")
        print(f"  Step 4 (Evaluation): {'✓' if results['step4'].get('success') else '✗'} "
              f"({results['step4'].get('total_files_evaluated', 0)} files evaluated)")
    else:
        # Direct or reasoning: show both generation and evaluation status
        for key, result in results.items():
            gen_status = '✓' if result.get('success') else '✗'
            eval_info = result.get('evaluation', {})
            eval_status = '✓' if eval_info.get('success') else '✗'
            print(f"{key}: generation={gen_status}, evaluation={eval_status}")
    
    print("="*80)
    print(f"\nFull logs: {output_struct.logs}")
    if hasattr(output_struct, 'metadata'):
        print(f"Metadata: {output_struct.root / 'metadata' / 'workflow_metadata.json'}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())