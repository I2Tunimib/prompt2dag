#!/usr/bin/env python3
"""
prompt2dag_json_to_csv.py

Flatten extracted Prompt2DAG comprehensive results JSON files into a CSV.
Updated to work with Step 4 evaluation v2.0 output structure.

For each JSON (one comprehensive run), produces rows for:
  - Direct prompting:   one row per orchestrator
  - Prompt2DAG:         one row per orchestrator × strategy (llm/template/hybrid)
  - Reasoning model:    one row per orchestrator

Each row includes:
  - Run/pipeline/model metadata
  - Static/compliance scores and grades (from summary)
  - Per-dimension static/compliance scores (from dimensions with final_score)
  - Token usage (from top-level summary.token_usage)
  - Penalties and issues breakdown
  - For Prompt2DAG rows: detailed Step 1 analyzer, graph validation, and semantic eval metrics

Usage:
  python prompt2dag_json_to_csv.py results_*.json
  # or with no args, it will process all *.json in the current directory
"""

import json
import csv
import glob
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional


# ---------------------------------------------------------------------------
# Helpers for metadata parsing
# ---------------------------------------------------------------------------

def parse_pipeline_and_models(run_dir: str):
    """
    Extract model identifiers from JSON metadata, not directory structure.
    """
    pipeline_id = 'unknown'
    model_identifier = 'unknown'
    std_llm = 'unknown'
    reasoning_llm = 'unknown'

    try:
        # For this run, extract from comprehensive_metadata
        # Since we're loading from consolidated JSON, use the metadata section
        if run_dir:
            p = Path(run_dir)
            pipeline_id = p.parent.parent.name  # comprehensive_20251130_220755
            # These would normally come from model config
            model_identifier = 'deepinfra-qwen__reasoning'  # from your metadata
            std_llm = 'deepinfra-qwen'
            reasoning_llm = 'deepinfra-kimi-k2'
    except Exception:
        pass

    return pipeline_id, model_identifier, std_llm, reasoning_llm

# ---------------------------------------------------------------------------
# Helpers for evaluation flattening (UPDATED FOR V2.0)
# ---------------------------------------------------------------------------

def apply_eval_summary_fields(row: Dict[str, Any], eval_summary: Dict[str, Any]) -> None:
    """Fill row with scores/grades from an evaluation summary object."""
    if not isinstance(eval_summary, dict):
        return

    # Summary-level scores
    row['Static_Score'] = eval_summary.get('static_score', 0.0) or 0.0
    row['Compliance_Score'] = eval_summary.get('compliance_score', 0.0) or 0.0
    row['Combined_Score'] = eval_summary.get('combined_score', 0.0) or 0.0
    row['Grade'] = eval_summary.get('combined_grade', row.get('Grade', 'N/A')) or row.get('Grade', 'N/A')
    row['Passed'] = bool(eval_summary.get('passed', row.get('Passed', False)))
    row['Gates_Passed'] = bool(eval_summary.get('gates_passed', row.get('Gates_Passed', False)))

    # ✅ EXTRACT PER-DIMENSION SCORES
    static_dimensions = eval_summary.get('static_dimensions', {}) or {}
    if isinstance(static_dimensions, dict):
        for dim, score in static_dimensions.items():
            norm_name = normalize_dim_name(dim)
            row[f'StaticDim_{norm_name}'] = score or 0.0

    compliance_dimensions = eval_summary.get('compliance_dimensions', {}) or {}
    if isinstance(compliance_dimensions, dict):
        for dim, score in compliance_dimensions.items():
            norm_name = normalize_dim_name(dim)
            row[f'ComplianceDim_{norm_name}'] = score or 0.0

    # Issues and penalties
    row['Total_Penalties'] = eval_summary.get('total_penalties', row.get('Total_Penalties', 0.0)) or 0.0
    row['Total_Issues'] = eval_summary.get('total_issues', row.get('Total_Issues', 0)) or 0
    row['Critical_Issues'] = eval_summary.get('critical_issues', row.get('Critical_Issues', 0)) or 0
    row['Major_Issues'] = eval_summary.get('major_issues', row.get('Major_Issues', 0)) or 0
    row['Minor_Issues'] = eval_summary.get('minor_issues', row.get('Minor_Issues', 0)) or 0
    
def normalize_dim_name(name: str) -> str:
    """
    Normalize a dimension name (e.g. "Structural Integrity") for use in column names.
    """
    return name.strip().replace(' ', '_').replace('-', '_')


# ---------------------------------------------------------------------------
# Prompt2DAG Step 1 extraction (NO CHANGES NEEDED)
# ---------------------------------------------------------------------------

def collect_step1_info(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collect flattened Prompt2DAG Step 1 information into a dict of column_name -> value.
    This is per-run and will be applied to every Prompt2DAG row for that run.
    """
    result: Dict[str, Any] = {}
    workflows = json_data.get('workflows', {}) or {}
    p2d = workflows.get('prompt2dag_pipeline', {}) or {}
    step1 = p2d.get('step1', {}) or {}

    if not step1:
        return result

    # 1) Analyzer token usage
    token_usage = step1.get('token_usage', {}) or {}
    totals = token_usage.get('totals', {}) or {}
    steps = token_usage.get('steps', {}) or {}

    result['S1_Tokens_total_input'] = totals.get('input_tokens', 0) or 0
    result['S1_Tokens_total_output'] = totals.get('output_tokens', 0) or 0
    result['S1_Tokens_total'] = totals.get('total_tokens', 0) or 0

    if isinstance(steps, dict):
        for step_name, vals in steps.items():
            if not isinstance(vals, dict):
                continue
            col_base = f'S1_Tokens_{step_name}'
            result[f'{col_base}_input'] = vals.get('input_tokens', 0) or 0
            result[f'{col_base}_output'] = vals.get('output_tokens', 0) or 0

    # 2) Graph validation
    evals = step1.get('evaluations', {}) or {}
    graph_eval = evals.get('graph_validation', {}) or {}

    if isinstance(graph_eval, dict) and graph_eval:
        result['S1_Graph_status'] = graph_eval.get('status', '')
        result['S1_Graph_overall_score'] = graph_eval.get('overall_score', 0.0) or 0.0
        issues = graph_eval.get('issues') or []
        result['S1_Graph_total_issues'] = len(issues)

        # Per-dimension scores
        dim_scores = graph_eval.get('dimension_scores') or []
        if isinstance(dim_scores, list):
            for dim in dim_scores:
                dim_name = dim.get('dimension')
                if not dim_name:
                    continue
                norm_name = normalize_dim_name(dim_name)
                base = f'S1_Graph_{norm_name}'
                result[f'{base}_score'] = dim.get('score', 0.0) or 0.0
                result[f'{base}_criteria_passed'] = dim.get('criteria_passed', 0) or 0
                result[f'{base}_criteria_total'] = dim.get('criteria_total', 0) or 0

        # A few useful graph statistics
        stats = graph_eval.get('graph_statistics') or {}
        if isinstance(stats, dict):
            result['S1_Graph_total_components'] = stats.get('total_components', 0) or 0
            result['S1_Graph_total_nodes_in_flow'] = stats.get('total_nodes_in_flow', 0) or 0
            result['S1_Graph_total_edges'] = stats.get('total_edges', 0) or 0
            result['S1_Graph_entry_point_count'] = stats.get('entry_point_count', 0) or 0
            result['S1_Graph_terminal_node_count'] = stats.get('terminal_node_count', 0) or 0
            result['S1_Graph_max_pipeline_depth'] = stats.get('max_pipeline_depth', 0) or 0

    # 3) Semantic evaluation
    sem_eval = evals.get('semantic_eval', {}) or {}
    if isinstance(sem_eval, dict) and sem_eval:
        metrics = sem_eval.get('metrics', {}) or {}
        bert = metrics.get('bertscore', {}) or {}
        rouge1 = metrics.get('rouge1', {}) or {}
        granular = sem_eval.get('granular_analysis', {}) or {}
        key_terms = granular.get('key_term_preservation', {}) or {}
        tok_stats = granular.get('token_statistics', {}) or {}
        sem_summary = sem_eval.get('summary', {}) or {}

        # BERTScore
        result['S1_Sem_BERT_f1'] = bert.get('f1', 0.0) or 0.0
        result['S1_Sem_BERT_norm'] = bert.get('normalized_score', 0.0) or 0.0

        # ROUGE-1
        result['S1_Sem_ROUGE1_f1'] = rouge1.get('f1', 0.0) or 0.0
        result['S1_Sem_ROUGE1_norm'] = rouge1.get('normalized_score', 0.0) or 0.0

        # Key term preservation
        result['S1_Sem_KeyTerm_total'] = key_terms.get('total_key_terms', 0) or 0
        result['S1_Sem_KeyTerm_preserved'] = key_terms.get('preserved_terms_count', 0) or 0
        result['S1_Sem_KeyTerm_missing'] = key_terms.get('missing_terms_count', 0) or 0
        result['S1_Sem_KeyTerm_rate'] = key_terms.get('preservation_rate', 0.0) or 0.0

        # Token statistics
        result['S1_Sem_tok_original'] = tok_stats.get('original_token_count', 0) or 0
        result['S1_Sem_tok_generated'] = tok_stats.get('generated_token_count', 0) or 0
        result['S1_Sem_tok_overlap_ratio'] = tok_stats.get('token_overlap_ratio', 0.0) or 0.0

        # Final semantic grade
        result['S1_Sem_final_grade'] = sem_summary.get('final_grade', '') or ''

    return result


# ---------------------------------------------------------------------------
# Main extractor for one JSON file (UPDATED FOR V2.0)
# ---------------------------------------------------------------------------

def extract_run_data(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    metadata = json_data.get('metadata', {}) or {}
    run_name = metadata.get('run_name', 'unknown')
    run_dir = metadata.get('run_directory', '')

    pipeline_id, model_identifier, std_llm, reasoning_llm = parse_pipeline_and_models(run_dir)

    # Top-level summary (for token_usage)
    summary = json_data.get('summary', {}) or {}
    tokens = summary.get('token_usage') or {}
    if not isinstance(tokens, dict):
        tokens = {}

    workflows = json_data.get('workflows', {}) or {}

    # Pre-collect Prompt2DAG Step 1 info (applied to all Prompt2DAG rows)
    step1_info = collect_step1_info(json_data)

    # Helper to initialize a base row
    def create_row(workflow: str, orchestrator: str, strategy: str) -> Dict[str, Any]:
        return {
            'Run_Name': run_name,
            'Pipeline_ID': pipeline_id,
            'Model_ID': model_identifier,
            'Std_LLM': std_llm,
            'Reasoning_LLM': reasoning_llm,
            'Workflow': workflow,
            'Orchestrator': orchestrator,
            'Strategy': strategy,
            'Static_Score': 0.0,
            'Static_Grade': '',
            'Compliance_Score': 0.0,
            'Compliance_Grade': '',
            'Combined_Score': 0.0,
            'Grade': 'N/A',
            'Passed': False,
            'Gates_Passed': False,
            'Total_Penalties': 0.0,
            'Total_Issues': 0,
            'Critical_Issues': 0,
            'Major_Issues': 0,
            'Minor_Issues': 0,
            'Input_Tokens': 0,
            'Output_Tokens': 0,
            'Total_Tokens': 0,
        }

    # ----------------------------------------------------------------------
    # 1) Direct prompting rows (workflows.direct_prompting.orchestrators)
    # ----------------------------------------------------------------------
    direct_wf = workflows.get('direct_prompting', {}) or {}
    direct_orchs = direct_wf.get('orchestrators', {}) or {}
    direct_tokens = tokens.get('direct_prompting', {}) or {}

    if isinstance(direct_orchs, dict):
        for orch_name, orch_obj in direct_orchs.items():
            row = create_row('Direct', orch_name, 'Direct Prompting')

            # UPDATED: Use evaluation.summary (from extract step, already has correct fields)
            eval_data = orch_obj.get('evaluation', {}) or {}
            eval_summary = eval_data.get('summary', {}) or {}
            apply_eval_summary_fields(row, eval_summary)

            # Tokens
            t = direct_tokens.get(orch_name, {}) or {}
            row['Input_Tokens'] = t.get('input_tokens', 0) or 0
            row['Output_Tokens'] = t.get('output_tokens', 0) or 0
            row['Total_Tokens'] = row['Input_Tokens'] + row['Output_Tokens']

            rows.append(row)

    # ----------------------------------------------------------------------
    # 2) Reasoning model rows (workflows.reasoning_model.orchestrators)
    # ----------------------------------------------------------------------
    reason_wf = workflows.get('reasoning_model', {}) or {}
    reason_orchs = reason_wf.get('orchestrators', {}) or {}
    reason_tokens = tokens.get('reasoning_model', {}) or {}

    if isinstance(reason_orchs, dict):
        for orch_name, orch_obj in reason_orchs.items():
            row = create_row('Reasoning', orch_name, 'Reasoning Model')

            eval_data = orch_obj.get('evaluation', {}) or {}
            eval_summary = eval_data.get('summary', {}) or {}
            apply_eval_summary_fields(row, eval_summary)

            t = reason_tokens.get(orch_name, {}) or {}
            row['Input_Tokens'] = t.get('input_tokens', 0) or 0
            row['Output_Tokens'] = t.get('output_tokens', 0) or 0
            row['Total_Tokens'] = row['Input_Tokens'] + row['Output_Tokens']

            rows.append(row)

    # ----------------------------------------------------------------------
    # 3) Prompt2DAG rows (workflows.prompt2dag_pipeline.step4.orchestrators)
    # ----------------------------------------------------------------------
    p2d_wf = workflows.get('prompt2dag_pipeline', {}) or {}
    step4 = p2d_wf.get('step4', {}) or {}
    p2d_orchs = step4.get('orchestrators', {}) or {}
    p2d_tokens = (
        tokens.get('prompt2dag_pipeline', {}) or {}
    ).get('step3_strategies', {}) or {}

    if isinstance(p2d_orchs, dict):
        for orch_name, orch_obj in p2d_orchs.items():
            strategies = orch_obj.get('strategies', {}) or {}
            orch_token_map = p2d_tokens.get(orch_name, {}) or {}

            if not isinstance(strategies, dict):
                continue

            for strat_name, strat_obj in strategies.items():
                row = create_row('Prompt2DAG', orch_name, strat_name)

                # UPDATED: Step 4 evaluation summary (for this strategy)
                eval_data = strat_obj.get('evaluation', {}) or {}
                eval_summary = eval_data.get('summary', {}) or {}
                apply_eval_summary_fields(row, eval_summary)

                # Step 3 LLM/template/hybrid token usage
                strat_toks = orch_token_map.get(strat_name, {}) or {}
                row['Input_Tokens'] = strat_toks.get('input_tokens', 0) or 0
                row['Output_Tokens'] = strat_toks.get('output_tokens', 0) or 0
                row['Total_Tokens'] = row['Input_Tokens'] + row['Output_Tokens']

                # Apply Step 1 metrics (same for all Prompt2DAG rows)
                for k, v in step1_info.items():
                    row[k] = v

                rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def main():
    # 1) Determine input files
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = glob.glob("*.json")

    if not files:
        print("No JSON files found. Please provide filenames or ensure files are in the directory.")
        return

    all_data: List[Dict[str, Any]] = []
    print(f"Found {len(files)} files. Processing...")

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            extracted_rows = extract_run_data(data)
            all_data.extend(extracted_rows)
            print(f"  -> Processed {file_path}: {len(extracted_rows)} rows extracted.")
        except Exception as e:
            print(f"  !! Error processing {file_path}: {e}")

    if not all_data:
        print("\nNo data extracted.")
        return

    # 2) Build full fieldnames set across all rows
    base_cols = [
        'Run_Name', 'Pipeline_ID', 'Model_ID', 'Std_LLM', 'Reasoning_LLM',
        'Workflow', 'Orchestrator', 'Strategy',
        'Static_Score', 'Static_Grade',
        'Compliance_Score', 'Compliance_Grade',
        'Combined_Score', 'Grade', 
        'Passed', 'Gates_Passed',
        'Total_Penalties',
        'Total_Issues', 'Critical_Issues', 'Major_Issues', 'Minor_Issues',
        'Input_Tokens', 'Output_Tokens', 'Total_Tokens',
    ]

    all_keys = set()
    for row in all_data:
        all_keys.update(row.keys())

    other_cols = sorted(k for k in all_keys if k not in base_cols)
    fieldnames = base_cols + other_cols

    # 3) Write CSV
    output_filename = "consolidated_results.csv"
    with open(output_filename, 'w', newline='', encoding='utf-8') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)

    print(f"\nSuccess! Data saved to '{output_filename}'")
    print(f"Total rows: {len(all_data)}")
    print(f"Total columns: {len(fieldnames)}")
    
    # Print summary statistics
    workflows = {}
    for row in all_data:
        wf = row.get('Workflow', 'Unknown')
        workflows[wf] = workflows.get(wf, 0) + 1
    
    print(f"\nRows by workflow:")
    for wf, count in sorted(workflows.items()):
        print(f"  {wf}: {count}")


if __name__ == "__main__":
    main()