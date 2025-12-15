#!/usr/bin/env python3
"""
json_to_csv_converter.py: Convert Prompt2DAG JSON results to CSV format
Enhanced version that properly handles the actual JSON structure from results_extractor.py
"""

import os
import sys
import json
import argparse
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import re

class Prompt2DAGJSONToCSVConverter:
    """Enhanced converter for Prompt2DAG JSON results."""
    
    def __init__(self, output_file: Optional[str] = None):
        self.output_file = output_file
        self.data_rows = []
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    def detect_file_type(self, json_data: Dict) -> str:
        """Detect if this is a consolidated file or individual run file."""
        if 'runs' in json_data and isinstance(json_data['runs'], list):
            return 'consolidated'
        elif 'metadata' in json_data and 'methods' in json_data:
            return 'individual'
        else:
            return 'unknown'
    
    def extract_experiment_metadata(self, run_data: Dict, file_path: str) -> Dict[str, Any]:
        """Extract experiment metadata and identifiers."""
        metadata = run_data.get('metadata', {})
        
        # Extract case study from run name
        run_name = metadata.get('run_name', '')
        case_study = self.extract_case_study(run_name, file_path)
        
        # Extract LLM information
        llm_provider, llm_model = self.extract_llm_info(run_data, run_name)
        
        return {
            'experiment_id': run_data.get('run_identifier', metadata.get('run_name', Path(file_path).stem)),
            'case_study': case_study,
            'llm_provider': llm_provider,
            'llm_model': llm_model,
            'run_timestamp': metadata.get('run_timestamp', ''),
            'extraction_timestamp': metadata.get('extraction_timestamp', ''),
            'run_directory': metadata.get('run_directory', ''),
            'file_path': str(file_path)
        }
    
    def extract_case_study(self, run_name: str, file_path: str) -> str:
        """Extract case study name from run name or file path."""
        case_patterns = [
            ('sequential_pipeline', 'Sequential Pipeline'),
            ('procurement_supplier', 'Procurement Supplier'),
            ('parallel_pipeline', 'Parallel Pipeline'),
            ('conditional_pipeline', 'Conditional Pipeline'),
            ('mixed_pipeline', 'Mixed Pipeline')
        ]
        
        search_text = (run_name + ' ' + str(file_path)).lower()
        
        for pattern, display_name in case_patterns:
            if pattern in search_text.replace('_', '').replace('-', ''):
                return display_name
        
        return 'Unknown'
    
    def extract_llm_info(self, run_data: Dict, run_name: str) -> tuple:
        """Extract LLM provider and model information."""
        # Try to get from step1 tokens first
        step1_data = run_data.get('step1_tokens', {}).get('data', {})
        if step1_data:
            provider = step1_data.get('provider', 'unknown')
            model = step1_data.get('model_key', 'unknown')
            if provider != 'unknown' and model != 'unknown':
                return provider, model
        
        # Extract from run name
        run_name_lower = run_name.lower()
        
        if 'deepinfra' in run_name_lower:
            provider = 'deepinfra'
            # Extract model name
            parts = run_name.split('_')
            for i, part in enumerate(parts):
                if part.lower() == 'deepinfra' and i + 1 < len(parts):
                    model = parts[i + 1]
                    return provider, model
            return provider, 'unknown'
        elif 'claude' in run_name_lower:
            return 'claude', 'claude-3-5-sonnet-20241022'
        elif 'azureopenai' in run_name_lower:
            return 'azureopenai', 'gpt-4o-mini'
        
        return 'unknown', 'unknown'
    
    def extract_analysis_quality_metrics(self, run_data: Dict) -> Dict[str, Any]:
        """Extract analysis quality metrics."""
        analysis_quality = run_data.get('analysis_quality', {})
        if not analysis_quality.get('found', False):
            return {
                'analysis_quality_found': False,
                'analysis_quality_total_issues': None,
                'analysis_quality_missing_count': None,
                'analysis_quality_hallucination_count': None,
                'analysis_quality_inconsistency_count': None,
                'analysis_quality_correct_count': None
            }
        
        data = analysis_quality.get('data', {})
        summary_metrics = data.get('summary_metrics', {})
        
        return {
            'analysis_quality_found': True,
            'analysis_quality_total_issues': summary_metrics.get('total_issues', 0),
            'analysis_quality_missing_count': summary_metrics.get('missing_count', 0),
            'analysis_quality_hallucination_count': summary_metrics.get('hallucination_count', 0),
            'analysis_quality_inconsistency_count': summary_metrics.get('inconsistency_count', 0),
            'analysis_quality_correct_count': summary_metrics.get('correct_count', 0)
        }
    
    def extract_step1_tokens(self, run_data: Dict) -> Dict[str, Any]:
        """Extract Step 1 token usage."""
        step1_tokens = run_data.get('step1_tokens', {})
        if not step1_tokens.get('found', False):
            return {
                'step1_tokens_found': False,
                'step1_input_tokens': None,
                'step1_output_tokens': None,
                'step1_total_tokens': None
            }
        
        totals = step1_tokens.get('data', {}).get('totals', {})
        
        return {
            'step1_tokens_found': True,
            'step1_input_tokens': totals.get('input_tokens', 0),
            'step1_output_tokens': totals.get('output_tokens', 0),
            'step1_total_tokens': totals.get('total_tokens', 0)
        }
    
    def extract_quality_report_metrics(self, method_data: Dict) -> Dict[str, Any]:
        """Extract quality report metrics for a method."""
        quality_report = method_data.get('quality_report', {})
        if not quality_report.get('found', False):
            return {
                'quality_report_found': False,
                'quality_total_issues': None,
                'quality_high_severity': None,
                'quality_medium_severity': None,
                'quality_low_severity': None,
                'quality_security_issues': None,
                'quality_best_practice_violations': None
            }
        
        data = quality_report.get('data', {})
        summary_metrics = data.get('summary_metrics', {})
        
        return {
            'quality_report_found': True,
            'quality_total_issues': summary_metrics.get('total_issues', 0),
            'quality_high_severity': summary_metrics.get('high_severity', 0),
            'quality_medium_severity': summary_metrics.get('medium_severity', 0),
            'quality_low_severity': summary_metrics.get('low_severity', 0),
            'quality_security_issues': summary_metrics.get('security_issues', 0),
            'quality_best_practice_violations': summary_metrics.get('best_practice_violations', 0)
        }
    
    def extract_static_analysis_metrics(self, method_data: Dict) -> Dict[str, Any]:
        """Extract static analysis metrics for a method."""
        static_analysis = method_data.get('static_analysis', {})
        if not static_analysis.get('found', False):
            return {
                'static_analysis_found': False,
                'static_overall_score': None,
                'static_grade': None,
                'static_passed': None,
                'static_security_score': None,
                'static_code_quality_score': None,
                'static_complexity_score': None,
                'static_flake8_score': None,
                'static_dag_design_score': None,
                'static_advanced_ast_score': None,
                'static_total_violations': None,
                'static_critical_violations': None,
                'static_major_violations': None,
                'static_minor_violations': None
            }
        
        data = static_analysis.get('data', {})
        file_type = static_analysis.get('file_type', 'unknown')
        
        if file_type == 'detailed':
            # Extract from detailed analysis file structure
            results = data.get('results', {})
            metadata = results.get('metadata', {})
            violation_counts = metadata.get('violation_counts', {})
            
            return {
                'static_analysis_found': True,
                'static_overall_score': results.get('overall_score', 0.0),
                'static_grade': results.get('grade', ''),
                'static_passed': results.get('passed', False),
                'static_security_score': results.get('security', {}).get('score', 0.0),
                'static_code_quality_score': results.get('code_quality', {}).get('score', 0.0),
                'static_complexity_score': results.get('complexity', {}).get('score', 0.0),
                'static_flake8_score': results.get('flake8', {}).get('score', 0.0),
                'static_dag_design_score': results.get('dag_design', {}).get('score', 0.0),
                'static_advanced_ast_score': results.get('advanced_ast', {}).get('score', 0.0),
                'static_total_violations': violation_counts.get('total', 0),
                'static_critical_violations': violation_counts.get('critical', 0),
                'static_major_violations': violation_counts.get('major', 0),
                'static_minor_violations': violation_counts.get('minor', 0)
            }
        
        elif file_type == 'summary':
            # Extract from summary file structure
            dag_results = data.get('dag_results', [])
            if dag_results:
                # Use the first (usually only) DAG result
                dag_result = dag_results[0]
                return {
                    'static_analysis_found': True,
                    'static_overall_score': dag_result.get('score', 0.0),
                    'static_grade': dag_result.get('grade', ''),
                    'static_passed': dag_result.get('error') is None,
                    'static_security_score': None,  # Not available in summary
                    'static_code_quality_score': None,
                    'static_complexity_score': None,
                    'static_flake8_score': None,
                    'static_dag_design_score': None,
                    'static_advanced_ast_score': None,
                    'static_total_violations': None,
                    'static_critical_violations': None,
                    'static_major_violations': None,
                    'static_minor_violations': None
                }
        
        # Fallback for unknown structure - try the old approach
        results = data.get('results', {})
        metadata = results.get('metadata', {})
        violation_counts = metadata.get('violation_counts', {})
        
        return {
            'static_analysis_found': True,
            'static_overall_score': results.get('overall_score', 0.0),
            'static_grade': results.get('grade', ''),
            'static_passed': results.get('passed', False),
            'static_security_score': results.get('security', {}).get('score', 0.0),
            'static_code_quality_score': results.get('code_quality', {}).get('score', 0.0),
            'static_complexity_score': results.get('complexity', {}).get('score', 0.0),
            'static_flake8_score': results.get('flake8', {}).get('score', 0.0),
            'static_dag_design_score': results.get('dag_design', {}).get('score', 0.0),
            'static_advanced_ast_score': results.get('advanced_ast', {}).get('score', 0.0),
            'static_total_violations': violation_counts.get('total', 0),
            'static_critical_violations': violation_counts.get('critical', 0),
            'static_major_violations': violation_counts.get('major', 0),
            'static_minor_violations': violation_counts.get('minor', 0)
        }

    def extract_dag_structure_metrics(self, method_data: Dict) -> Dict[str, Any]:
        """Extract DAG structure metrics for a method."""
        dag_structure = method_data.get('dag_structure', {})
        if not dag_structure.get('found', False):
            return {
                'structure_found': False,
                'structure_overall_score': None,
                'structure_passed': None,
                'structure_n_tasks': None,
                'structure_n_dependencies': None,
                'structure_valid_connectivity': None,
                'structure_is_acyclic': None,
                'structure_max_depth': None,
                'structure_isolated_tasks_count': None,
                'structure_large_components': None,
                'structure_tiny_components': None,
                'structure_docker_operators_found': None,
                'structure_docker_score': None
            }
        
        results = dag_structure.get('data', {}).get('results', {})
        graph_properties = results.get('graph_properties', {})
        docker_analysis = results.get('docker_analysis', {})
        
        return {
            'structure_found': True,
            'structure_overall_score': results.get('overall_score', 0.0),
            'structure_passed': results.get('passed', False),
            'structure_n_tasks': graph_properties.get('n_tasks', 0),
            'structure_n_dependencies': graph_properties.get('n_dependencies', 0),
            'structure_valid_connectivity': graph_properties.get('valid_connectivity', False),
            'structure_is_acyclic': graph_properties.get('is_acyclic', False),
            'structure_max_depth': graph_properties.get('max_depth', 0),
            'structure_isolated_tasks_count': len(graph_properties.get('isolated_tasks', [])),
            'structure_large_components': graph_properties.get('large_components', 0),
            'structure_tiny_components': graph_properties.get('tiny_components', 0),
            'structure_docker_operators_found': docker_analysis.get('docker_operators_found', 0),
            'structure_docker_score': docker_analysis.get('score', 0.0)
        }
    
    def extract_dry_run_metrics(self, method_data: Dict) -> Dict[str, Any]:
        """Extract dry run metrics for a method."""
        dry_run = method_data.get('dry_run', {})
        if not dry_run.get('found', False):
            return {
                'dry_run_found': False,
                'dry_run_executable': None,
                'dry_run_loadable': None,
                'dry_run_runnable': None,
                'dry_run_overall_score': None,
                'dry_run_grade': None,
                'dry_run_task_count': None,
                'dry_run_error_count': None,
                'dry_run_warning_count': None
            }
        
        data = dry_run.get('data', {})
        summary = data.get('summary', {})
        
        return {
            'dry_run_found': True,
            'dry_run_executable': data.get('executable', False),
            'dry_run_loadable': data.get('loadable', False),
            'dry_run_runnable': data.get('runnable', False),
            'dry_run_overall_score': data.get('overall_score', 0.0),
            'dry_run_grade': summary.get('grade', ''),
            'dry_run_task_count': len(data.get('tasks', {})),
            'dry_run_error_count': len(data.get('errors', [])),
            'dry_run_warning_count': len(data.get('warnings', []))
        }
    
    def extract_token_usage_metrics(self, method_data: Dict, method_name: str) -> Dict[str, Any]:
        """Extract token usage metrics for a method."""
        token_usage = method_data.get('token_usage', {})
        if not token_usage.get('found', False) or not token_usage.get('tokens_used', True):
            return {
                'method_tokens_found': False,
                'method_tokens_used': False,
                'method_input_tokens': None,
                'method_output_tokens': None,
                'method_total_tokens': None
            }
        
        data = token_usage.get('data', {})
        
        # Handle different token data structures
        if method_name == 'llm':
            totals = data.get('totals', {})
            return {
                'method_tokens_found': True,
                'method_tokens_used': True,
                'method_input_tokens': totals.get('input_tokens', 0),
                'method_output_tokens': totals.get('output_tokens', 0),
                'method_total_tokens': totals.get('total_tokens', 0)
            }
        elif method_name == 'hybrid':
            token_usage_data = data.get('token_usage', {})
            return {
                'method_tokens_found': True,
                'method_tokens_used': True,
                'method_input_tokens': token_usage_data.get('input_tokens', 0),
                'method_output_tokens': token_usage_data.get('output_tokens', 0),
                'method_total_tokens': data.get('total_tokens', 0)
            }
        elif method_name == 'direct':
            token_usage_data = data.get('token_usage', {})
            input_tokens = token_usage_data.get('input_tokens', 0)
            output_tokens = token_usage_data.get('output_tokens', 0)
            return {
                'method_tokens_found': True,
                'method_tokens_used': True,
                'method_input_tokens': input_tokens,
                'method_output_tokens': output_tokens,
                'method_total_tokens': input_tokens + output_tokens
            }
        else:  # template
            return {
                'method_tokens_found': True,
                'method_tokens_used': False,
                'method_input_tokens': 0,
                'method_output_tokens': 0,
                'method_total_tokens': 0
            }
    
    def extract_dag_file_metrics(self, method_data: Dict) -> Dict[str, Any]:
        """Extract DAG file information."""
        dag_file = method_data.get('dag_file', {})
        if not dag_file.get('found', False):
            return {
                'dag_file_found': False,
                'dag_file_size': None,
                'dag_file_path': None
            }
        
        return {
            'dag_file_found': True,
            'dag_file_size': dag_file.get('file_size', 0),
            'dag_file_path': dag_file.get('file_path', '')
        }
    
    def process_single_run(self, run_data: Dict, file_path: str) -> None:
        """Process a single run's data and extract all relevant metrics."""
        logging.info(f"Processing run: {run_data.get('run_identifier', run_data.get('metadata', {}).get('run_name', 'unknown'))}")
        
        # Extract experiment metadata
        experiment_metadata = self.extract_experiment_metadata(run_data, file_path)
        
        # Extract global metrics
        analysis_quality_metrics = self.extract_analysis_quality_metrics(run_data)
        step1_token_metrics = self.extract_step1_tokens(run_data)
        
        # Extract summary token information
        summary = run_data.get('summary', {})
        token_summary = summary.get('token_summary', {})
        totals = token_summary.get('totals', {})
        
        global_metrics = {
            **experiment_metadata,
            **analysis_quality_metrics,
            **step1_token_metrics,
            'experiment_total_input_tokens': totals.get('total_input_tokens', 0),
            'experiment_total_output_tokens': totals.get('total_output_tokens', 0),
            'experiment_total_tokens': totals.get('total_tokens', 0),
            'experiment_successful_methods': summary.get('successful_methods', 0),
            'experiment_total_methods': summary.get('total_methods', 0)
        }
        
        # Extract method-specific metrics
        methods = run_data.get('methods', {})
        
        for method_name, method_data in methods.items():
            if not method_data.get('found', False):
                # Create row with method info even if not found
                row = {
                    **global_metrics,
                    'dag_generation_method': method_name,
                    'method_found': False
                }
            else:
                # Create a row for each method
                row = {
                    **global_metrics,
                    'dag_generation_method': method_name,
                    'method_found': True,
                    
                    # Method-specific metrics
                    **self.extract_dag_file_metrics(method_data),
                    **self.extract_quality_report_metrics(method_data),
                    **self.extract_static_analysis_metrics(method_data),
                    **self.extract_dag_structure_metrics(method_data),
                    **self.extract_dry_run_metrics(method_data),
                    **self.extract_token_usage_metrics(method_data, method_name)
                }
            
            self.data_rows.append(row)
    
    def process_json_file(self, file_path: str) -> None:
        """Process a single JSON file and extract all relevant metrics."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            logging.info(f"Processing: {file_path}")
            
            # Detect file type
            file_type = self.detect_file_type(json_data)
            
            if file_type == 'consolidated':
                # Process consolidated file with multiple runs
                logging.info(f"Processing consolidated file with {len(json_data['runs'])} runs")
                for run_data in json_data['runs']:
                    self.process_single_run(run_data, file_path)
                    
            elif file_type == 'individual':
                # Process individual run file
                logging.info("Processing individual run file")
                self.process_single_run(json_data, file_path)
                
            else:
                logging.warning(f"Unknown file format: {file_path}")
                
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
    
    def convert_files(self, file_paths: List[str]) -> pd.DataFrame:
        """Convert multiple JSON files to a single DataFrame."""
        logging.info(f"Converting {len(file_paths)} JSON files to CSV format")
        
        for file_path in file_paths:
            self.process_json_file(file_path)
        
        if not self.data_rows:
            logging.warning("No data rows generated")
            return pd.DataFrame()
        
        df = pd.DataFrame(self.data_rows)
        
        # Define preferred column order for better readability
        column_order = [
            # Identifiers
            'experiment_id', 'case_study', 'llm_provider', 'llm_model', 'dag_generation_method',
            'method_found',
            
            # Timestamps
            'run_timestamp', 'extraction_timestamp',
            
            # Experiment-level metrics
            'experiment_total_methods', 'experiment_successful_methods',
            
            # Analysis Quality
            'analysis_quality_found', 'analysis_quality_total_issues', 'analysis_quality_missing_count', 
            'analysis_quality_hallucination_count', 'analysis_quality_inconsistency_count',
            'analysis_quality_correct_count',
            
            # Step 1 Tokens
            'step1_tokens_found', 'step1_input_tokens', 'step1_output_tokens', 'step1_total_tokens',
            
            # DAG File
            'dag_file_found', 'dag_file_size',
            
            # Quality Report
            'quality_report_found', 'quality_total_issues', 'quality_high_severity', 'quality_medium_severity',
            'quality_low_severity', 'quality_security_issues', 'quality_best_practice_violations',
            
            # Static Analysis
            'static_analysis_found', 'static_overall_score', 'static_grade', 'static_passed',
            'static_security_score', 'static_code_quality_score', 'static_complexity_score',
            'static_flake8_score', 'static_dag_design_score', 'static_advanced_ast_score',
            'static_total_violations', 'static_critical_violations', 'static_major_violations',
            'static_minor_violations',
            
            # DAG Structure
            'structure_found', 'structure_overall_score', 'structure_passed', 'structure_n_tasks',
            'structure_n_dependencies', 'structure_valid_connectivity', 'structure_is_acyclic',
            'structure_max_depth', 'structure_isolated_tasks_count', 'structure_large_components',
            'structure_tiny_components', 'structure_docker_operators_found', 'structure_docker_score',
            
            # Dry Run
            'dry_run_found', 'dry_run_executable', 'dry_run_loadable', 'dry_run_runnable',
            'dry_run_overall_score', 'dry_run_grade', 'dry_run_task_count',
            'dry_run_error_count', 'dry_run_warning_count',
            
            # Method Token Usage
            'method_tokens_found', 'method_tokens_used', 'method_input_tokens', 'method_output_tokens', 'method_total_tokens',
            
            # Experiment Totals
            'experiment_total_input_tokens', 'experiment_total_output_tokens', 'experiment_total_tokens',
            
            # File paths
            'dag_file_path', 'run_directory', 'file_path'
        ]
        
        # Reorder columns (keep any additional columns at the end)
        existing_columns = [col for col in column_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in column_order]
        final_column_order = existing_columns + remaining_columns
        
        df = df[final_column_order]
        
        logging.info(f"Generated DataFrame with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    def save_csv(self, df: pd.DataFrame, output_file: Optional[str] = None) -> str:
        """Save DataFrame to CSV file."""
        if output_file:
            csv_path = output_file
        elif self.output_file:
            csv_path = self.output_file
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = f"prompt2dag_results_{timestamp}.csv"
        
        df.to_csv(csv_path, index=False)
        logging.info(f"CSV saved to: {csv_path}")
        
        return csv_path
    
    def print_summary(self, df: pd.DataFrame) -> None:
        """Print a summary of the converted data."""
        if df.empty:
            print("No data to summarize")
            return
        
        print("\n" + "="*70)
        print("PROMPT2DAG CSV CONVERSION SUMMARY")
        print("="*70)
        
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        
        # Summary by case study
        if 'case_study' in df.columns:
            print(f"\nCase Studies:")
            case_study_counts = df['case_study'].value_counts()
            for case_study, count in case_study_counts.items():
                print(f"  {case_study}: {count} rows")
        
        # Summary by LLM
        if 'llm_provider' in df.columns and 'llm_model' in df.columns:
            print(f"\nLLM Providers:")
            llm_counts = df.groupby(['llm_provider', 'llm_model']).size()
            for (provider, model), count in llm_counts.items():
                print(f"  {provider} ({model}): {count} rows")
        
        # Summary by DAG method
        if 'dag_generation_method' in df.columns:
            print(f"\nDAG Generation Methods:")
            method_counts = df['dag_generation_method'].value_counts()
            for method, count in method_counts.items():
                print(f"  {method}: {count} rows")
        
        # Performance summary
        if 'dry_run_executable' in df.columns:
            executable_count = df['dry_run_executable'].sum()
            total_count = len(df[df['dry_run_found'] == True])
            if total_count > 0:
                print(f"\nExecutability Summary:")
                print(f"  Executable DAGs: {executable_count}/{total_count} ({executable_count/total_count*100:.1f}%)")
        
        if 'static_overall_score' in df.columns:
            static_scores = df[df['static_analysis_found'] == True]['static_overall_score']
            if len(static_scores) > 0:
                avg_static_score = static_scores.mean()
                print(f"  Average Static Analysis Score: {avg_static_score:.2f}/10")
        
        if 'structure_overall_score' in df.columns:
            structure_scores = df[df['structure_found'] == True]['structure_overall_score']
            if len(structure_scores) > 0:
                avg_structure_score = structure_scores.mean()
                print(f"  Average Structure Score: {avg_structure_score:.2f}/10")
        
        print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Convert Prompt2DAG JSON results to CSV format",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'input_paths',
        nargs='+',
        help='JSON files or directories to process'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output CSV file path (auto-generated if not specified)'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress summary output'
    )
    
    args = parser.parse_args()
    
    # Find JSON files
    json_files = []
    for input_path in args.input_paths:
        path = Path(input_path)
        if path.is_file() and path.suffix == '.json':
            json_files.append(str(path))
        elif path.is_dir():
            # Look for extracted results files
            patterns = ['*_extracted_results.json', 'consolidated_results_*.json']
            for pattern in patterns:
                json_files.extend([str(f) for f in path.glob(pattern)])
    
    if not json_files:
        print("ERROR: No JSON files found in the specified paths", file=sys.stderr)
        return 1
    
    json_files = sorted(list(set(json_files)))  # Remove duplicates
    logging.info(f"Found {len(json_files)} JSON files to process")
    
    try:
        # Convert to CSV
        converter = Prompt2DAGJSONToCSVConverter(args.output)
        df = converter.convert_files(json_files)
        
        if df.empty:
            print("ERROR: No data extracted from JSON files", file=sys.stderr)
            return 1
        
        # Save CSV
        csv_file = converter.save_csv(df)
        
        # Print summary
        if not args.quiet:
            converter.print_summary(df)
            print(f"\nCSV file saved: {csv_file}")
        
        return 0
        
    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())