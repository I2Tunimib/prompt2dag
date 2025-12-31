#!/usr/bin/env python3
"""
comprehensive_results_extractor.py: Extract and consolidate results from Prompt2DAG comprehensive runs.
Handles all three workflows: direct prompting, prompt2dag pipeline, and reasoning model.

Updated to work with the new Step 4 evaluation structure:
- Uses 'final_score' instead of 'score'
- Correctly parses new 'summary' structure
- Handles updated dimension field names
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import re


class ComprehensiveResultsExtractor:
    """Extract and consolidate results from a comprehensive Prompt2DAG run."""
    
    def __init__(self, run_directory: str):
        """Initialize the extractor with a comprehensive run directory."""
        self.run_dir = Path(run_directory).resolve()
        self.results: Dict[str, Any] = {
            'metadata': {},
            'workflows': {
                'direct_prompting': {},
                'prompt2dag_pipeline': {},
                'reasoning_model': {}
            },
            'summary': {}
        }
        self.setup_logging()
        
    # ---------------------------------------------------------------------
    # Utility & I/O helpers
    # ---------------------------------------------------------------------
    
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()],
            force=True,
        )
    
    def find_files_by_pattern(self, pattern: str, directory: Path = None) -> List[Path]:
        """
        Find files matching a glob pattern in the specified directory.
        Returns newest-first (descending mtime).
        """
        search_dir = directory or self.run_dir
        matches = list(search_dir.glob(pattern))
        return sorted(matches, key=lambda x: x.stat().st_mtime, reverse=True)
    
    def load_json_file(self, file_path: Path) -> Optional[Dict]:
        """Load JSON file and return data."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
            return None
    
    # ---------------------------------------------------------------------
    # Metadata extraction
    # ---------------------------------------------------------------------
    
    def extract_metadata(self) -> Dict[str, Any]:
        """Extract run-level metadata, including comprehensive summary if available."""
        metadata: Dict[str, Any] = {
            'run_directory': str(self.run_dir),
            'run_name': self.run_dir.name,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Extract timestamp from directory name (e.g. comprehensive_20251129_154659)
        timestamp_match = re.search(r'(\d{8}_\d{6})', self.run_dir.name)
        if timestamp_match:
            metadata['run_timestamp'] = timestamp_match.group(1)
        
        # Load comprehensive_metadata.json if present
        comp_meta_file = self.run_dir / 'comprehensive_metadata.json'
        if comp_meta_file.exists():
            comp_data = self.load_json_file(comp_meta_file)
            if comp_data:
                # Store full metadata for future use
                metadata['comprehensive_metadata'] = comp_data
                # Flatten some key runtime info
                summary = comp_data.get('summary', {})
                metadata['run_start_time'] = summary.get('start_time')
                metadata['run_end_time'] = summary.get('end_time')
                metadata['total_duration_seconds'] = summary.get('total_duration_seconds')
        
        return metadata
    
    # ---------------------------------------------------------------------
    # Evaluation extraction helpers (UPDATED FOR V2.0)
    # ---------------------------------------------------------------------
    
    def extract_evaluation_metrics(self, eval_file: Path) -> Dict[str, Any]:
        """
        Extract detailed evaluation metrics from an evaluation_*.json file.
        Updated to work with new Step 4 evaluation structure.
        """
        data = self.load_json_file(eval_file)
        if not data:
            return {}
        
        # Extract dimension details
        static_dims = {}
        compliance_dims = {}
        
        # Static analysis dimensions
        static_analysis = data.get('static_analysis', {})
        static_dimensions = static_analysis.get('dimensions', {})
        for dim_name, dim_data in static_dimensions.items():
            static_dims[dim_name] = {
                'raw_score': dim_data.get('raw_score', 0),
                'penalties': dim_data.get('penalties', 0),
                'final_score': dim_data.get('final_score', 0),
                'issue_count': dim_data.get('issue_count', 0)
            }
        
        # Platform compliance dimensions
        platform_compliance = data.get('platform_compliance', {})
        compliance_dimensions = platform_compliance.get('dimensions', {})
        for dim_name, dim_data in compliance_dimensions.items():
            compliance_dims[dim_name] = {
                'raw_score': dim_data.get('raw_score', 0),
                'penalties': dim_data.get('penalties', 0),
                'final_score': dim_data.get('final_score', 0),
                'issue_count': dim_data.get('issue_count', 0)
            }
        
        return {
            'file_path': str(eval_file),
            'static_analysis': {
                'overall_score': static_analysis.get('overall_score', 0),
                'grade': static_analysis.get('grade', 'F'),
                'gates_passed': static_analysis.get('gates_passed', False),
                'total_penalties': static_analysis.get('total_penalties', 0),
                'dimensions': static_dims,
                'total_issues': len(static_analysis.get('issues', []))
            },
            'platform_compliance': {
                'overall_score': platform_compliance.get('overall_score', 0),
                'grade': platform_compliance.get('grade', 'F'),
                'gates_passed': platform_compliance.get('gates_passed', False),
                'total_penalties': platform_compliance.get('total_penalties', 0),
                'dimensions': compliance_dims,
                'total_issues': len(platform_compliance.get('issues', []))
            },
            'summary': data.get('summary', {}),
            'raw_data': data
        }
    
    def extract_evaluation_summary(self, summary_file: Path) -> Dict[str, Any]:
        """
        Extract evaluation summary (evaluation_summary_*.json).
        Updated for new structure.
        """
        data = self.load_json_file(summary_file)
        if not data:
            return {}
        
        # Process results array
        processed_results = []
        for result in data.get('results', []):
            summary = result.get('summary', {})
            processed_results.append({
                'file_path': result.get('file_path'),
                'orchestrator': result.get('orchestrator'),
                'static_score': summary.get('static_score', 0),
                'static_grade': summary.get('static_grade', 'F'),
                'compliance_score': summary.get('compliance_score', 0),
                'compliance_grade': summary.get('compliance_grade', 'F'),
                'combined_score': summary.get('combined_score', 0),
                'combined_grade': summary.get('combined_grade', 'F'),
                'passed': summary.get('passed', False),
                'gates_passed': summary.get('gates_passed', False),
                'total_penalties': summary.get('total_penalties', 0),
                'total_issues': summary.get('total_issues', 0),
                'critical_issues': summary.get('critical_issues', 0),
                'major_issues': summary.get('major_issues', 0),
                'minor_issues': summary.get('minor_issues', 0)
            })
        
        return {
            'file_path': str(summary_file),
            'evaluation_timestamp': data.get('evaluation_timestamp'),
            'evaluator_version': data.get('evaluator_version', '1.0'),
            'total_files': data.get('total_files', 0),
            'passed': data.get('passed', 0),
            'failed': data.get('failed', 0),
            'average_scores': data.get('average_scores', {}),
            'results': processed_results
        }
    
    # ---------------------------------------------------------------------
    # Token usage extraction
    # ---------------------------------------------------------------------
    
    def extract_token_usage(self, token_file: Path) -> Dict[str, Any]:
        """
        Extract token usage information.

        Supports:
        - Step 1 analyzer tokens file (Step_1_pipeline_analyzer_*_tokens_*.json)
        - Generation metadata (generation_metadata.json)
        """
        data = self.load_json_file(token_file)
        if not data:
            return {}
        
        if 'totals' in data:
            # Step 1 analyzer tokens format
            return {
                'file_path': str(token_file),
                'type': 'step1_analyzer',
                'totals': data.get('totals', {}),
                'steps': data.get('steps', {}),
                'raw_data': data
            }
        elif 'token_usage' in data:
            # Generation metadata format
            return {
                'file_path': str(token_file),
                'type': 'generation_metadata',
                'token_usage': data.get('token_usage', {}),
                'orchestrator': data.get('orchestrator'),
                'timestamp': data.get('timestamp'),
                'raw_data': data
            }
        else:
            return {
                'file_path': str(token_file),
                'type': 'unknown',
                'raw_data': data
            }
    
    # ---------------------------------------------------------------------
    # Step 1 evaluation extraction
    # ---------------------------------------------------------------------
    
    def extract_step1_evaluations(self, step1_dir: Path) -> Dict[str, Any]:
        """Extract Step 1 evaluation results (graph validation and semantic analysis)."""
        evaluations: Dict[str, Any] = {}
        
        # Graph validation
        graph_val_dir = step1_dir / 'graph_validation'
        if graph_val_dir.exists():
            graph_files = self.find_files_by_pattern('dag_validation_*.json', graph_val_dir)
            if graph_files:
                data = self.load_json_file(graph_files[0])
                if data:
                    evaluations['graph_validation'] = {
                        'file_path': str(graph_files[0]),
                        'status': data.get('validation_metadata', {}).get('status'),
                        'overall_score': data.get('validation_metadata', {}).get('overall_score', 0),
                        'dimension_scores': data.get('dimension_scores', []),
                        'issues': data.get('issues', []),
                        'graph_statistics': data.get('graph_statistics', {}),
                        'connectivity_analysis': data.get('connectivity_analysis', {}),
                        'recommendations': data.get('recommendations', []),
                        'raw_data': data
                    }
        
        # Semantic evaluation
        semantic_dir = step1_dir / 'semantic_eval'
        if semantic_dir.exists():
            semantic_files = self.find_files_by_pattern('semantic_eval_*.json', semantic_dir)
            if semantic_files:
                data = self.load_json_file(semantic_files[0])
                if data:
                    evaluations['semantic_eval'] = {
                        'file_path': str(semantic_files[0]),
                        'metrics': data.get('metrics', {}),
                        'granular_analysis': data.get('granular_analysis', {}),
                        'summary': data.get('summary', {}),
                        'raw_data': data
                    }
        
        return evaluations
    
    # ---------------------------------------------------------------------
    # Workflow-specific extraction
    # ---------------------------------------------------------------------
    
    def extract_direct_prompting(self) -> Dict[str, Any]:
        """
        Extract direct prompting workflow results.
        There is one 'direct_*' subdirectory per orchestrator.
        """
        base_dir = self.run_dir / '1_direct'
        if not base_dir.exists():
            return {'found': False}
        
        logging.info("Extracting direct prompting results...")
        
        run_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith('direct_')]
        if not run_dirs:
            return {'found': False}
        
        results: Dict[str, Any] = {
            'found': True,
            'runs': [],
            'orchestrators': {}
        }
        
        for run_dir in sorted(run_dirs, key=lambda d: d.name):
            gen_code_dir = run_dir / 'generated_code'
            eval_dir = run_dir / 'evaluation'
            run_info: Dict[str, Any] = {
                'run_directory': str(run_dir),
                'generated_code': {},
                'evaluation': {},
                'evaluation_summary': {}
            }
            
            orchestrator: Optional[str] = None
            
            # Generation metadata and DAG file
            if gen_code_dir.exists():
                dag_files = self.find_files_by_pattern('*.py', gen_code_dir)
                token_files = self.find_files_by_pattern('generation_metadata.json', gen_code_dir)
                
                if dag_files:
                    run_info['generated_code']['dag_file'] = str(dag_files[0])
                if token_files:
                    token_data = self.extract_token_usage(token_files[0])
                    run_info['generated_code']['token_usage'] = token_data
                    orchestrator = token_data.get('orchestrator') or orchestrator
            
            # Evaluation files
            if eval_dir.exists():
                eval_files = [f for f in self.find_files_by_pattern('evaluation_*_*.json', eval_dir)
                              if 'summary' not in f.name]
                summary_files = self.find_files_by_pattern('evaluation_summary_*.json', eval_dir)
                
                if eval_files:
                    run_info['evaluation'] = self.extract_evaluation_metrics(eval_files[0])
                if summary_files:
                    run_info['evaluation_summary'] = self.extract_evaluation_summary(summary_files[0])
            
            results['runs'].append(run_info)
            
            # Map into orchestrator key
            if orchestrator:
                results['orchestrators'][orchestrator] = run_info
            else:
                # Fallback: derive from dag filename if necessary
                dag_file = run_info.get('generated_code', {}).get('dag_file', '')
                if 'airflow' in dag_file:
                    results['orchestrators']['airflow'] = run_info
                elif 'prefect' in dag_file:
                    results['orchestrators']['prefect'] = run_info
                elif 'dagster' in dag_file:
                    results['orchestrators']['dagster'] = run_info
        
        return results
    
    def extract_prompt2dag_pipeline(self) -> Dict[str, Any]:
        """Extract prompt2dag 4-step pipeline workflow results."""
        base_dir = self.run_dir / '2_prompt2dag'
        if not base_dir.exists():
            return {'found': False}
        
        logging.info("Extracting prompt2dag pipeline results...")
        
        run_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith('pipeline_')]
        if not run_dirs:
            return {'found': False}
        
        run_dir = run_dirs[0]
        results: Dict[str, Any] = {
            'found': True,
            'run_directory': str(run_dir),
            'step1': {},
            'step2': {},
            'step3': {},
            'step4': {},
            'generation_report': {}
        }
        
        # STEP 1: Analysis
        step1_dir = run_dir / 'step1_analysis'
        if step1_dir.exists():
            analysis_files = self.find_files_by_pattern('pipeline_analysis_s1_*.json', step1_dir)
            token_files = self.find_files_by_pattern('Step_1_pipeline_analyzer_*_tokens_*.json', step1_dir)
            
            if analysis_files:
                results['step1']['analysis_file'] = str(analysis_files[0])
                results['step1']['analysis_data'] = self.load_json_file(analysis_files[0])
            
            if token_files:
                results['step1']['token_usage'] = self.extract_token_usage(token_files[0])
            
            # Step 1 evaluations
            results['step1']['evaluations'] = self.extract_step1_evaluations(step1_dir)
        
        # STEP 2: Intermediate YAML
        step2_dir = run_dir / 'step2_intermediate'
        if step2_dir.exists():
            results['step2']['orchestrators'] = {}
            for orch_dir in step2_dir.iterdir():
                if orch_dir.is_dir():
                    yaml_files = self.find_files_by_pattern('*.yaml', orch_dir)
                    if yaml_files:
                        results['step2']['orchestrators'][orch_dir.name] = {
                            'intermediate_yaml': str(yaml_files[0])
                        }
        
        # STEP 3: Generated code
        step3_dir = run_dir / 'step3_generated'
        if step3_dir.exists():
            results['step3']['orchestrators'] = {}
            
            # Generation reports per orchestrator
            for orch_dir in step3_dir.iterdir():
                if not orch_dir.is_dir():
                    continue
                orch_name = orch_dir.name
                results['step3']['orchestrators'][orch_name] = {'strategies': {}}
                
                # Generation report for this orchestrator
                gen_report_file = orch_dir / 'generation_report.json'
                if gen_report_file.exists():
                    if 'generation_report' not in results or not results['generation_report']:
                        # Keep also a top-level snapshot (first one)
                        results['generation_report'] = {
                            'file_path': str(gen_report_file),
                            'data': self.load_json_file(gen_report_file)
                        }
                    orch_report = self.load_json_file(gen_report_file)
                    results['step3']['orchestrators'][orch_name]['generation_report'] = orch_report
                
                nested_orch_dir = orch_dir / orch_name
                if nested_orch_dir.exists():
                    for strategy_dir in nested_orch_dir.iterdir():
                        if strategy_dir.is_dir():
                            strategy_name = strategy_dir.name
                            dag_files = self.find_files_by_pattern('*.py', strategy_dir)
                            strat_info: Dict[str, Any] = {}
                            if dag_files:
                                strat_info['dag_file'] = str(dag_files[0])
                            results['step3']['orchestrators'][orch_name]['strategies'][strategy_name] = strat_info
        
        # STEP 4: Evaluation (UPDATED FOR NEW STRUCTURE)
        step4_dir = run_dir / 'step4_evaluation'
        if step4_dir.exists():
            results['step4']['orchestrators'] = {}
            
            for orch_dir in step4_dir.iterdir():
                if not orch_dir.is_dir():
                    continue
                orch_name = orch_dir.name
                results['step4']['orchestrators'][orch_name] = {'strategies': {}}
                
                for strategy_dir in orch_dir.iterdir():
                    if not strategy_dir.is_dir():
                        continue
                    strategy_name = strategy_dir.name
                    
                    eval_files = [f for f in self.find_files_by_pattern('evaluation_*_*.json', strategy_dir)
                                  if 'summary' not in f.name]
                    summary_files = self.find_files_by_pattern('evaluation_summary_*.json', strategy_dir)
                    strat_results: Dict[str, Any] = {}
                    
                    if eval_files:
                        strat_results['evaluation'] = self.extract_evaluation_metrics(eval_files[0])
                    if summary_files:
                        strat_results['evaluation_summary'] = self.extract_evaluation_summary(summary_files[0])
                    
                    results['step4']['orchestrators'][orch_name]['strategies'][strategy_name] = strat_results
        
        return results
    
    def extract_reasoning_model(self) -> Dict[str, Any]:
        """
        Extract reasoning model workflow results.
        There is one 'reasoning_*' subdirectory per orchestrator.
        """
        base_dir = self.run_dir / '3_reasoning'
        if not base_dir.exists():
            return {'found': False}
        
        logging.info("Extracting reasoning model results...")
        
        run_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith('reasoning_')]
        if not run_dirs:
            return {'found': False}
        
        results: Dict[str, Any] = {
            'found': True,
            'runs': [],
            'orchestrators': {}
        }
        
        for run_dir in sorted(run_dirs, key=lambda d: d.name):
            gen_code_dir = run_dir / 'generated_code'
            eval_dir = run_dir / 'evaluation'
            run_info: Dict[str, Any] = {
                'run_directory': str(run_dir),
                'generated_code': {},
                'evaluation': {},
                'evaluation_summary': {}
            }
            
            orchestrator: Optional[str] = None
            
            # Generation metadata and DAG
            if gen_code_dir.exists():
                dag_files = self.find_files_by_pattern('*.py', gen_code_dir)
                token_files = self.find_files_by_pattern('generation_metadata.json', gen_code_dir)
                
                if dag_files:
                    run_info['generated_code']['dag_file'] = str(dag_files[0])
                if token_files:
                    token_data = self.extract_token_usage(token_files[0])
                    run_info['generated_code']['token_usage'] = token_data
                    orchestrator = token_data.get('orchestrator') or orchestrator
            
            # Evaluation
            if eval_dir.exists():
                eval_files = [f for f in self.find_files_by_pattern('evaluation_*_*.json', eval_dir)
                              if 'summary' not in f.name]
                summary_files = self.find_files_by_pattern('evaluation_summary_*.json', eval_dir)
                
                if eval_files:
                    run_info['evaluation'] = self.extract_evaluation_metrics(eval_files[0])
                if summary_files:
                    run_info['evaluation_summary'] = self.extract_evaluation_summary(summary_files[0])
            
            results['runs'].append(run_info)
            
            # Map into orchestrator
            if orchestrator:
                results['orchestrators'][orchestrator] = run_info
            else:
                dag_file = run_info.get('generated_code', {}).get('dag_file', '')
                if 'airflow' in dag_file:
                    results['orchestrators']['airflow'] = run_info
                elif 'prefect' in dag_file:
                    results['orchestrators']['prefect'] = run_info
                elif 'dagster' in dag_file:
                    results['orchestrators']['dagster'] = run_info
        
        return results
    
    # ---------------------------------------------------------------------
    # Summary computation (UPDATED FOR NEW FIELD NAMES)
    # ---------------------------------------------------------------------
    
    def calculate_summary(self) -> Dict[str, Any]:
        """Calculate comprehensive summary statistics across workflows."""
        summary: Dict[str, Any] = {
            'workflows_found': {
                'direct_prompting': self.results['workflows']['direct_prompting'].get('found', False),
                'prompt2dag_pipeline': self.results['workflows']['prompt2dag_pipeline'].get('found', False),
                'reasoning_model': self.results['workflows']['reasoning_model'].get('found', False)
            },
            'runtime': {},
            'token_usage': {
                'direct_prompting': {},
                'prompt2dag_pipeline': {},
                'reasoning_model': {},
                'totals': {
                    'total_input_tokens': 0,
                    'total_output_tokens': 0,
                    'total_tokens': 0
                }
            },
            'evaluation_scores': {
                'direct_prompting': {},
                'prompt2dag_pipeline': {},
                'reasoning_model': {}
            },
            'generation_success': {
                'direct_prompting': [],
                'prompt2dag_pipeline': {},
                'reasoning_model': []
            }
        }
        
        # Runtime from comprehensive metadata (if available)
        comp_meta = self.results.get('metadata', {}).get('comprehensive_metadata', {})
        if comp_meta:
            run_summary = comp_meta.get('summary', {})
            summary['runtime'] = {
                'start_time': run_summary.get('start_time'),
                'end_time': run_summary.get('end_time'),
                'total_duration_seconds': run_summary.get('total_duration_seconds')
            }
        
        # --- Direct Prompting ---
        direct = self.results['workflows']['direct_prompting']
        if direct.get('found'):
            summary['token_usage']['direct_prompting'] = {}
            summary['evaluation_scores']['direct_prompting'] = {}
            
            for orch, orch_data in direct.get('orchestrators', {}).items():
                # Tokens
                token_data = orch_data.get('generated_code', {}).get('token_usage', {})
                usage = token_data.get('token_usage', {})
                summary['token_usage']['direct_prompting'][orch] = usage
                summary['token_usage']['totals']['total_input_tokens'] += usage.get('input_tokens', 0)
                summary['token_usage']['totals']['total_output_tokens'] += usage.get('output_tokens', 0)
                
                # UPDATED: Use new summary structure
                eval_summary = orch_data.get('evaluation', {}).get('summary', {})
                if eval_summary:
                    summary['evaluation_scores']['direct_prompting'][orch] = {
                        'static_score': eval_summary.get('static_score', 0),
                        'static_grade': eval_summary.get('static_grade', 'F'),
                        'compliance_score': eval_summary.get('compliance_score', 0),
                        'compliance_grade': eval_summary.get('compliance_grade', 'F'),
                        'combined_score': eval_summary.get('combined_score', 0),
                        'combined_grade': eval_summary.get('combined_grade', 'F'),
                        'passed': eval_summary.get('passed', False),
                        'gates_passed': eval_summary.get('gates_passed', False),
                        'total_penalties': eval_summary.get('total_penalties', 0),
                        'total_issues': eval_summary.get('total_issues', 0)
                    }
                
                # Generation success
                if orch_data.get('generated_code', {}).get('dag_file'):
                    summary['generation_success']['direct_prompting'].append(orch)
        
        # --- Prompt2DAG Pipeline ---
        pipeline = self.results['workflows']['prompt2dag_pipeline']
        if pipeline.get('found'):
            p2d_tokens: Dict[str, Any] = {}
            
            # Step 1 tokens
            if 'step1' in pipeline and 'token_usage' in pipeline['step1']:
                step1_totals = pipeline['step1']['token_usage'].get('totals', {})
                p2d_tokens['step1'] = step1_totals
                summary['token_usage']['totals']['total_input_tokens'] += step1_totals.get('input_tokens', 0)
                summary['token_usage']['totals']['total_output_tokens'] += step1_totals.get('output_tokens', 0)
            
            # Step 3 tokens from generation_report per orchestrator
            step3_tokens: Dict[str, Any] = {}
            step3 = pipeline.get('step3', {}).get('orchestrators', {})
            for orch_name, orch_data in step3.items():
                orch_report = orch_data.get('generation_report') or {}
                strategies = orch_report.get('strategies', {})
                step3_tokens[orch_name] = {}
                for strategy_name, strat_data in strategies.items():
                    if not strat_data.get('success'):
                        continue
                    token_usage = strat_data.get('token_usage', {})
                    if token_usage:
                        step3_tokens[orch_name][strategy_name] = token_usage
                        # Add to overall totals (only LLM-based: llm, hybrid)
                        if strategy_name in ('llm', 'hybrid'):
                            summary['token_usage']['totals']['total_input_tokens'] += token_usage.get('input_tokens', 0)
                            summary['token_usage']['totals']['total_output_tokens'] += token_usage.get('output_tokens', 0)
            if step3_tokens:
                p2d_tokens['step3_strategies'] = step3_tokens
            
            summary['token_usage']['prompt2dag_pipeline'] = p2d_tokens
            
            # UPDATED: Step 4 evaluation scores per orchestrator & strategy
            step4_scores: Dict[str, Any] = {}
            step4 = pipeline.get('step4', {}).get('orchestrators', {})
            for orch_name, orch_data in step4.items():
                step4_scores[orch_name] = {}
                for strategy_name, strat_data in orch_data.get('strategies', {}).items():
                    eval_summary = strat_data.get('evaluation', {}).get('summary', {})
                    if eval_summary:
                        step4_scores[orch_name][strategy_name] = {
                            'static_score': eval_summary.get('static_score', 0),
                            'static_grade': eval_summary.get('static_grade', 'F'),
                            'compliance_score': eval_summary.get('compliance_score', 0),
                            'compliance_grade': eval_summary.get('compliance_grade', 'F'),
                            'combined_score': eval_summary.get('combined_score', 0),
                            'combined_grade': eval_summary.get('combined_grade', 'F'),
                            'passed': eval_summary.get('passed', False),
                            'gates_passed': eval_summary.get('gates_passed', False),
                            'total_penalties': eval_summary.get('total_penalties', 0),
                            'total_issues': eval_summary.get('total_issues', 0)
                        }
            summary['evaluation_scores']['prompt2dag_pipeline'] = step4_scores
            
            # Generation success per orchestrator/strategy
            gen_success: Dict[str, List[str]] = {}
            for orch_name, orch_data in pipeline.get('step3', {}).get('orchestrators', {}).items():
                available_strategies = list(orch_data.get('strategies', {}).keys())
                gen_success[orch_name] = available_strategies
            summary['generation_success']['prompt2dag_pipeline'] = gen_success
        
        # --- Reasoning Model ---
        reasoning = self.results['workflows']['reasoning_model']
        if reasoning.get('found'):
            summary['token_usage']['reasoning_model'] = {}
            summary['evaluation_scores']['reasoning_model'] = {}
            
            for orch, orch_data in reasoning.get('orchestrators', {}).items():
                token_data = orch_data.get('generated_code', {}).get('token_usage', {})
                usage = token_data.get('token_usage', {})
                summary['token_usage']['reasoning_model'][orch] = usage
                summary['token_usage']['totals']['total_input_tokens'] += usage.get('input_tokens', 0)
                summary['token_usage']['totals']['total_output_tokens'] += usage.get('output_tokens', 0)
                
                # UPDATED: Use new summary structure
                eval_summary = orch_data.get('evaluation', {}).get('summary', {})
                if eval_summary:
                    summary['evaluation_scores']['reasoning_model'][orch] = {
                        'static_score': eval_summary.get('static_score', 0),
                        'static_grade': eval_summary.get('static_grade', 'F'),
                        'compliance_score': eval_summary.get('compliance_score', 0),
                        'compliance_grade': eval_summary.get('compliance_grade', 'F'),
                        'combined_score': eval_summary.get('combined_score', 0),
                        'combined_grade': eval_summary.get('combined_grade', 'F'),
                        'passed': eval_summary.get('passed', False),
                        'gates_passed': eval_summary.get('gates_passed', False),
                        'total_penalties': eval_summary.get('total_penalties', 0),
                        'total_issues': eval_summary.get('total_issues', 0)
                    }
                
                if orch_data.get('generated_code', {}).get('dag_file'):
                    summary['generation_success']['reasoning_model'].append(orch)
        
        # Final total tokens
        summary['token_usage']['totals']['total_tokens'] = (
            summary['token_usage']['totals']['total_input_tokens'] +
            summary['token_usage']['totals']['total_output_tokens']
        )
        
        return summary
    
    # ---------------------------------------------------------------------
    # Main extraction entry point
    # ---------------------------------------------------------------------
    
    def extract_all(self) -> Dict[str, Any]:
        """Extract all results from the comprehensive run directory."""
        logging.info(f"Extracting results from: {self.run_dir}")
        
        # Metadata
        self.results['metadata'] = self.extract_metadata()
        
        # Workflows
        self.results['workflows']['direct_prompting'] = self.extract_direct_prompting()
        self.results['workflows']['prompt2dag_pipeline'] = self.extract_prompt2dag_pipeline()
        self.results['workflows']['reasoning_model'] = self.extract_reasoning_model()
        
        # Summary
        self.results['summary'] = self.calculate_summary()
        
        return self.results
    
    def save_results(self, output_file: str) -> None:
        """Save extracted results to JSON file."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logging.info(f"Results saved to: {output_file}")
    
    # ---------------------------------------------------------------------
    # Human-readable summary (UPDATED FOR NEW FIELDS)
    # ---------------------------------------------------------------------
    
    def print_summary(self) -> None:
        """Print a concise human-readable summary of the extracted results."""
        summary = self.results.get('summary', {})
        metadata = self.results.get('metadata', {})
        
        print("\n" + "="*70)
        print("COMPREHENSIVE PROMPT2DAG RESULTS EXTRACTION SUMMARY")
        print("="*70)
        print(f"Run Directory: {metadata.get('run_name', 'Unknown')}")
        print(f"Run Timestamp: {metadata.get('run_timestamp', 'Unknown')}")
        
        runtime = summary.get('runtime', {})
        if runtime:
            print(f"Start Time:  {runtime.get('start_time', 'N/A')}")
            print(f"End Time:    {runtime.get('end_time', 'N/A')}")
            print(f"Duration:    {runtime.get('total_duration_seconds', 0):.2f}s")
        
        # Workflows found
        workflows_found = summary.get('workflows_found', {})
        print(f"\nWorkflows Found:")
        print(f"  Direct Prompting:    {'✓' if workflows_found.get('direct_prompting') else '✗'}")
        print(f"  Prompt2DAG Pipeline: {'✓' if workflows_found.get('prompt2dag_pipeline') else '✗'}")
        print(f"  Reasoning Model:     {'✓' if workflows_found.get('reasoning_model') else '✗'}")
        
        # Token usage totals
        token_summary = summary.get('token_usage', {})
        totals = token_summary.get('totals', {})
        print(f"\nTotal Token Usage (all workflows):")
        print(f"  Input Tokens:  {totals.get('total_input_tokens', 0):,}")
        print(f"  Output Tokens: {totals.get('total_output_tokens', 0):,}")
        print(f"  Total Tokens:  {totals.get('total_tokens', 0):,}")
        
        # Token breakdown by workflow
        if token_summary.get('direct_prompting'):
            print("\nDirect Prompting Token Usage (by orchestrator):")
            for orch, usage in token_summary['direct_prompting'].items():
                total = usage.get('input_tokens', 0) + usage.get('output_tokens', 0)
                print(f"  {orch}: {total:,} tokens (in={usage.get('input_tokens', 0):,}, "
                      f"out={usage.get('output_tokens', 0):,})")
        
        if token_summary.get('prompt2dag_pipeline'):
            print("\nPrompt2DAG Pipeline Token Usage:")
            p2d = token_summary['prompt2dag_pipeline']
            if 'step1' in p2d:
                step1_totals = p2d['step1']
                print(f"  Step 1 (Analysis): {step1_totals.get('total_tokens', 0):,} tokens "
                      f"(in={step1_totals.get('input_tokens', 0):,}, "
                      f"out={step1_totals.get('output_tokens', 0):,})")
            if 'step3_strategies' in p2d:
                print("  Step 3 (Code Generation LLM usage):")
                for orch, strat_map in p2d['step3_strategies'].items():
                    for strat, usage in strat_map.items():
                        total = usage.get('input_tokens', 0) + usage.get('output_tokens', 0)
                        print(f"    {orch}/{strat}: {total:,} tokens "
                              f"(in={usage.get('input_tokens', 0):,}, "
                              f"out={usage.get('output_tokens', 0):,})")
        
        if token_summary.get('reasoning_model'):
            print("\nReasoning Model Token Usage (by orchestrator):")
            for orch, usage in token_summary['reasoning_model'].items():
                total = usage.get('input_tokens', 0) + usage.get('output_tokens', 0)
                print(f"  {orch}: {total:,} tokens (in={usage.get('input_tokens', 0):,}, "
                      f"out={usage.get('output_tokens', 0):,})")
        
        # Evaluation scores (UPDATED FIELD NAMES)
        eval_scores = summary.get('evaluation_scores', {})
        
        # Direct prompting scores
        if eval_scores.get('direct_prompting'):
            print("\nDirect Prompting Scores (per orchestrator):")
            for orch, scores in eval_scores['direct_prompting'].items():
                print(f"  {orch.upper()}:")
                print(f"    Static:     {scores.get('static_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('static_grade', 'N/A')})")
                print(f"    Compliance: {scores.get('compliance_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('compliance_grade', 'N/A')})")
                print(f"    Combined:   {scores.get('combined_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('combined_grade', 'N/A')})")
                print(f"    Gates:      {'✓' if scores.get('gates_passed') else '✗'}")
                print(f"    Passed:     {'✓' if scores.get('passed') else '✗'}")
                if scores.get('total_penalties', 0) > 0:
                    print(f"    Penalties:  {scores.get('total_penalties', 0):.2f}")
        
        # Prompt2DAG pipeline scores
        if eval_scores.get('prompt2dag_pipeline'):
            print("\nPrompt2DAG Pipeline Scores (Step 4 evaluation):")
            for orch_name, strategies in eval_scores['prompt2dag_pipeline'].items():
                print(f"  {orch_name.upper()}:")
                for strategy_name, scores in strategies.items():
                    print(f"    {strategy_name}:")
                    print(f"      Static:     {scores.get('static_score', 0):.2f}/10 "
                          f"(Grade: {scores.get('static_grade', 'N/A')})")
                    print(f"      Compliance: {scores.get('compliance_score', 0):.2f}/10 "
                          f"(Grade: {scores.get('compliance_grade', 'N/A')})")
                    print(f"      Combined:   {scores.get('combined_score', 0):.2f}/10 "
                          f"(Grade: {scores.get('combined_grade', 'N/A')})")
                    print(f"      Gates:      {'✓' if scores.get('gates_passed') else '✗'}")
                    print(f"      Passed:     {'✓' if scores.get('passed') else '✗'}")
                    if scores.get('total_penalties', 0) > 0:
                        print(f"      Penalties:  {scores.get('total_penalties', 0):.2f}")
        
        # Reasoning scores
        if eval_scores.get('reasoning_model'):
            print("\nReasoning Model Scores (per orchestrator):")
            for orch, scores in eval_scores['reasoning_model'].items():
                print(f"  {orch.upper()}:")
                print(f"    Static:     {scores.get('static_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('static_grade', 'N/A')})")
                print(f"    Compliance: {scores.get('compliance_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('compliance_grade', 'N/A')})")
                print(f"    Combined:   {scores.get('combined_score', 0):.2f}/10 "
                      f"(Grade: {scores.get('combined_grade', 'N/A')})")
                print(f"    Gates:      {'✓' if scores.get('gates_passed') else '✗'}")
                print(f"    Passed:     {'✓' if scores.get('passed') else '✗'}")
                if scores.get('total_penalties', 0) > 0:
                    print(f"    Penalties:  {scores.get('total_penalties', 0):.2f}")
        
        # Step 1 analysis quality (optional nice-to-have)
        pipeline = self.results.get('workflows', {}).get('prompt2dag_pipeline', {})
        if pipeline.get('found') and 'step1' in pipeline:
            step1_evals = pipeline['step1'].get('evaluations', {})
            
            if 'graph_validation' in step1_evals:
                gv = step1_evals['graph_validation']
                print("\nStep 1 Graph Validation:")
                print(f"  Status: {gv.get('status', 'N/A')}")
                print(f"  Score:  {gv.get('overall_score', 0):.2f}/10")
                if gv.get('issues'):
                    print(f"  Issues: {len(gv['issues'])}")
            
            if 'semantic_eval' in step1_evals:
                se = step1_evals['semantic_eval']
                se_summary = se.get('summary', {})
                print("\nStep 1 Semantic Evaluation:")
                print(f"  Grade: {se_summary.get('final_grade', 'N/A')}")
                scores = se_summary.get('scores_by_metric', {})
                for metric, score in scores.items():
                    print(f"  {metric}: {score:.2f}/10")
        
        print("\n" + "="*70)


# -------------------------------------------------------------------------
# CLI Entrypoint
# -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract results from a comprehensive Prompt2DAG run directory",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'run_directory',
        help='Path to the comprehensive run directory (e.g. outputs/comprehensive/comprehensive_YYYYMMDD_HHMMSS)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output JSON file (auto-generated if not specified)'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress printed summary output'
    )
    
    args = parser.parse_args()
    
    run_dir = Path(args.run_directory)
    if not run_dir.exists() or not run_dir.is_dir():
        print(f"ERROR: Run directory does not exist: {run_dir}", file=sys.stderr)
        return 1
    
    try:
        extractor = ComprehensiveResultsExtractor(str(run_dir))
        results = extractor.extract_all()
        
        # Determine output filename
        if args.output:
            output_file = args.output
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"{run_dir.name}_extracted_{timestamp}.json"
        
        extractor.save_results(output_file)
        
        if not args.quiet:
            extractor.print_summary()
            print(f"\nDetailed results saved to: {output_file}")
        
        return 0
    
    except Exception as e:
        logging.error(f"Extraction failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())