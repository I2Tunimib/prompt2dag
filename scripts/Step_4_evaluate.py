#!/usr/bin/env python3
"""
Step 4: Unified Evaluation
==========================
Evaluates generated orchestrator code using:
1. Enhanced Static Analysis (50%) - 5 dimensions
2. Platform Compliance Testing (50%) - 5 dimensions

Each dimension scored 0-10 with weighted penalties applied.

Usage:
    # Evaluate single file
    python Step_4_evaluate.py --file outputs_step3/airflow/template/pipeline.py
    
    # Evaluate multiple files
    python Step_4_evaluate.py --files pipeline1.py pipeline2.py
    
    # Evaluate folder
    python Step_4_evaluate.py --folder outputs_step3/airflow/
    
    # Specify orchestrator
    python Step_4_evaluate.py --file pipeline.py --orchestrator airflow
    
    # With reference YAML (for enhanced validation)
    python Step_4_evaluate.py \
        --file outputs_step3/airflow/template/pipeline.py \
        --reference outputs_step2/airflow/pipeline_intermediate.yaml
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add parent to path
script_dir = Path(__file__).parent
project_root = script_dir.parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from evaluators.unified_evaluator import UnifiedEvaluator
from evaluators.base_evaluator import Orchestrator


def setup_logging(level: str = "INFO", output_dir: Optional[Path] = None):
    """Setup logging configuration."""
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if output_dir:
        log_file = output_dir / f"step4_evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )


def get_files_to_evaluate(args) -> List[Path]:
    """Get list of files to evaluate from arguments."""
    files = []
    
    if args.file:
        files.append(Path(args.file))
    
    if args.files:
        files.extend([Path(f) for f in args.files])
    
    if args.folder:
        folder = Path(args.folder)
        if folder.is_dir():
            # Find Python files, excluding __pycache__ and common patterns
            for pattern in ["**/*.py", "*.py"]:
                found = folder.glob(pattern)
                for f in found:
                    if "__pycache__" not in str(f) and "__init__" not in f.name:
                        files.append(f)
    
    # Deduplicate and filter to existing files
    files = list(set(f for f in files if f.is_file() and f.suffix == ".py"))
    
    return sorted(files)


def print_detailed_summary(results: List[dict]):
    """Print comprehensive evaluation summary to console."""
    print("\n" + "=" * 120)
    print(" " * 45 + "EVALUATION SUMMARY")
    print("=" * 120)
    
    print(f"\nTotal files evaluated: {len(results)}")
    
    passed = sum(1 for r in results if r.get("summary", {}).get("passed", False))
    failed = len(results) - passed
    
    print(f"✓ Passed: {passed}")
    print(f"✗ Failed: {failed}")
    
    if not results:
        return
    
    # ═══════════════════════════════════════════════════════════════════════
    # STATIC ANALYSIS DIMENSIONS
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "-" * 120)
    print("STATIC ANALYSIS SCORES (0-10) - 50% of total")
    print("-" * 120)
    print(f"{'File':<40} {'Correctness':>12} {'Quality':>10} {'BestPrac':>10} {'Maintain':>10} {'Robust':>10} {'Avg':>8}")
    print("-" * 120)
    
    for result in results:
        file_name = Path(result["file_path"]).name[:38]
        static = result.get("static_analysis", {})
        dimensions = static.get("dimensions", {})
        
        correctness = dimensions.get("correctness", {}).get("final_score", 0)
        quality = dimensions.get("code_quality", {}).get("final_score", 0)
        best_prac = dimensions.get("best_practices", {}).get("final_score", 0)
        maintain = dimensions.get("maintainability", {}).get("final_score", 0)
        robust = dimensions.get("robustness", {}).get("final_score", 0)
        avg = static.get("overall_score", 0.0)
        
        print(f"{file_name:<40} "
              f"{correctness:>12.2f} "
              f"{quality:>10.2f} "
              f"{best_prac:>10.2f} "
              f"{maintain:>10.2f} "
              f"{robust:>10.2f} "
              f"{avg:>8.2f}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PLATFORM COMPLIANCE DIMENSIONS
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "-" * 120)
    print("PLATFORM COMPLIANCE SCORES (0-10) - 50% of total")
    print("-" * 120)
    print(f"{'File':<40} {'Loadable':>10} {'Structure':>10} {'Config':>10} {'Tasks':>10} {'Executable':>12} {'Avg':>8}")
    print("-" * 120)
    
    for result in results:
        file_name = Path(result["file_path"]).name[:38]
        compliance = result.get("platform_compliance", {})
        dimensions = compliance.get("dimensions", {})
        
        if dimensions:
            loadable = dimensions.get("loadability", {}).get("final_score", 0)
            structure = dimensions.get("structure_validity", {}).get("final_score", 0)
            config = dimensions.get("configuration_validity", {}).get("final_score", 0)
            tasks = dimensions.get("task_validity", {}).get("final_score", 0)
            executable = dimensions.get("executability", {}).get("final_score", 0)
            avg = compliance.get("overall_score", 0.0)
            
            print(f"{file_name:<40} "
                  f"{loadable:>10.2f} "
                  f"{structure:>10.2f} "
                  f"{config:>10.2f} "
                  f"{tasks:>10.2f} "
                  f"{executable:>12.2f} "
                  f"{avg:>8.2f}")
        else:
            print(f"{file_name:<40} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>12} {'N/A':>8}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PENALTIES & ISSUES
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "-" * 120)
    print("PENALTIES & ISSUES")
    print("-" * 120)
    print(f"{'File':<40} {'Total Penalties':>16} {'Critical':>10} {'Major':>10} {'Minor':>10} {'Info':>10}")
    print("-" * 120)
    
    for result in results:
        file_name = Path(result["file_path"]).name[:38]
        summary = result.get("summary", {})
        
        penalties = summary.get("total_penalties", 0.0)
        critical = summary.get("critical_issues", 0)
        major = summary.get("major_issues", 0)
        minor = summary.get("minor_issues", 0)
        info = summary.get("total_issues", 0) - critical - major - minor
        
        print(f"{file_name:<40} "
              f"{penalties:>16.2f} "
              f"{critical:>10} "
              f"{major:>10} "
              f"{minor:>10} "
              f"{info:>10}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # COMBINED SCORES & GRADES
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "-" * 120)
    print("COMBINED SCORES & GRADES")
    print("-" * 120)
    print(f"{'File':<40} {'Static':>10} {'Compliance':>12} {'Combined':>10} {'Grade':>8} {'Gates':>8} {'Status':>10}")
    print("-" * 120)
    
    for result in results:
        file_name = Path(result["file_path"]).name[:38]
        summary = result.get("summary", {})
        
        static_score = summary.get("static_score", 0.0)
        compliance_score = summary.get("compliance_score")
        combined = summary.get("combined_score", 0.0)
        grade = summary.get("combined_grade", "F")
        gates_passed = "✓" if summary.get("gates_passed", False) else "✗"
        passed = "✓ PASS" if summary.get("passed", False) else "✗ FAIL"
        
        compliance_str = f"{compliance_score:.2f}" if compliance_score is not None else "N/A"
        
        # Color coding for status (if terminal supports it)
        status_color = "\033[92m" if summary.get("passed") else "\033[91m"
        reset_color = "\033[0m"
        
        print(f"{file_name:<40} "
              f"{static_score:>10.2f} "
              f"{compliance_str:>12} "
              f"{combined:>10.2f} "
              f"{grade:>8} "
              f"{gates_passed:>8} "
              f"{status_color}{passed:>10}{reset_color}")
    
    print("=" * 120)
    
    # ═══════════════════════════════════════════════════════════════════════
    # STATISTICS
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "-" * 120)
    print("STATISTICS")
    print("-" * 120)
    
    # Calculate averages
    static_scores = [r.get("summary", {}).get("static_score", 0) for r in results]
    compliance_scores = [
        r.get("summary", {}).get("compliance_score", 0) 
        for r in results 
        if r.get("summary", {}).get("compliance_score") is not None
    ]
    combined_scores = [r.get("summary", {}).get("combined_score", 0) for r in results]
    
    if static_scores:
        print(f"Average Static Score:     {sum(static_scores) / len(static_scores):>6.2f}")
    if compliance_scores:
        print(f"Average Compliance Score: {sum(compliance_scores) / len(compliance_scores):>6.2f}")
    if combined_scores:
        print(f"Average Combined Score:   {sum(combined_scores) / len(combined_scores):>6.2f}")
    
    # Grade distribution
    grades = [r.get("summary", {}).get("combined_grade", "F") for r in results]
    print("\nGrade Distribution:")
    for grade in ["A", "B", "C", "D", "F"]:
        count = grades.count(grade)
        if count > 0:
            print(f"  {grade}: {count} ({count/len(results)*100:.1f}%)")
    
    # Issue summary
    total_issues = sum(r.get("summary", {}).get("total_issues", 0) for r in results)
    total_critical = sum(r.get("summary", {}).get("critical_issues", 0) for r in results)
    total_major = sum(r.get("summary", {}).get("major_issues", 0) for r in results)
    total_minor = sum(r.get("summary", {}).get("minor_issues", 0) for r in results)
    
    print(f"\nTotal Issues Across All Files: {total_issues}")
    print(f"  Critical: {total_critical}")
    print(f"  Major:    {total_major}")
    print(f"  Minor:    {total_minor}")
    
    print("-" * 120)


def main():
    parser = argparse.ArgumentParser(
        description="Step 4: Unified evaluation for generated orchestrator code",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Evaluate single file
    python Step_4_evaluate.py --file outputs_step3/airflow/template/pipeline.py
    
    # Evaluate all files in folder
    python Step_4_evaluate.py --folder outputs_step3/airflow/template/
    
    # With reference YAML for validation
    python Step_4_evaluate.py \\
        --file outputs_step3/airflow/template/pipeline.py \\
        --reference outputs_step2/airflow/pipeline_intermediate.yaml
    
    # Compare multiple generation strategies
    python Step_4_evaluate.py \\
        --files outputs_step3/airflow/template/pipeline.py \\
                outputs_step3/airflow/llm/pipeline.py \\
                outputs_step3/airflow/hybrid/pipeline.py
        """
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--file", help="Single file to evaluate")
    input_group.add_argument("--files", nargs="+", help="Multiple files to evaluate")
    input_group.add_argument("--folder", help="Folder containing Python files")
    
    # Reference YAML
    parser.add_argument(
        "--reference",
        help="Intermediate YAML file for reference (optional)",
    )
    
    # Output options
    parser.add_argument(
        "--output-dir",
        default="outputs_step4",
        help="Output directory for evaluation results (default: outputs_step4)",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="Suffix for output filenames",
    )
    
    # Orchestrator option
    parser.add_argument(
        "--orchestrator",
        choices=["airflow", "prefect", "dagster", "auto"],
        default="auto",
        help="Target orchestrator (auto-detect if not specified)",
    )
    
    # Evaluation options
    parser.add_argument(
        "--config",
        help="Optional config file for evaluation settings",
    )
    
    # Output format
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output results as JSON to stdout (instead of formatted table)",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip printing summary table",
    )
    
    # Other options
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    
    args = parser.parse_args()
    
    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup logging
    setup_logging(args.log_level, output_dir)
    
    logging.info("=" * 80)
    logging.info("STEP 4: UNIFIED EVALUATION")
    logging.info("=" * 80)
    
    # Get files
    files = get_files_to_evaluate(args)
    if not files:
        logging.error("No Python files found to evaluate")
        return 1
    
    logging.info(f"Found {len(files)} file(s) to evaluate")
    for f in files:
        logging.info(f"  - {f}")
    
    # Determine orchestrator
    orchestrator = None
    if args.orchestrator != "auto":
        orchestrator = Orchestrator(args.orchestrator)
        logging.info(f"Using orchestrator: {orchestrator.value}")
    else:
        logging.info("Auto-detecting orchestrator from code")
    
    # Load config if provided
    config = {}
    if args.config:
        config_path = Path(args.config)
        if config_path.exists():
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logging.info(f"Loaded config from: {config_path}")
    
    # Initialize evaluator
    evaluator = UnifiedEvaluator(config=config)
    
    # Load reference YAML if provided
    if args.reference:
        reference_path = Path(args.reference)
        if reference_path.exists():
            evaluator.load_reference_from_file(reference_path)
            logging.info(f"Loaded reference YAML: {reference_path}")
        else:
            logging.warning(f"Reference YAML not found: {reference_path}")
    
    # Evaluate all files
    all_results = []
    
    for idx, file_path in enumerate(files, 1):
        logging.info(f"\n{'=' * 60}")
        logging.info(f"[{idx}/{len(files)}] Evaluating: {file_path}")
        logging.info(f"{'=' * 60}")
        
        try:
            result = evaluator.evaluate(file_path, orchestrator)
            
            # Save individual result
            output_path = evaluator.save_results(result, output_dir, args.suffix)
            result["output_file"] = str(output_path)
            
            all_results.append(result)
            
            # Log key metrics
            summary = result.get("summary", {})
            logging.info(
                f"Results: "
                f"Static={summary.get('static_score', 0):.2f}/{summary.get('static_grade', 'F')}, "
                f"Compliance={summary.get('compliance_score', 'N/A')}/{summary.get('compliance_grade', 'N/A')}, "
                f"Combined={summary.get('combined_score', 0):.2f}/{summary.get('combined_grade', 'F')}, "
                f"Status={'PASS' if summary.get('passed') else 'FAIL'}"
            )
            
            # Log penalties if significant
            penalties = summary.get("total_penalties", 0)
            if penalties > 0:
                logging.info(f"  Penalties Applied: {penalties:.2f}")
            
            # Log critical issues
            critical = summary.get("critical_issues", 0)
            if critical > 0:
                logging.warning(f"  ⚠ {critical} CRITICAL issue(s) found!")
        
        except Exception as e:
            logging.error(f"Failed to evaluate {file_path}: {e}", exc_info=True)
            all_results.append({
                "file_path": str(file_path),
                "error": str(e),
                "summary": {
                    "static_score": 0.0,
                    "compliance_score": None,
                    "combined_score": 0.0,
                    "combined_grade": "F",
                    "passed": False,
                    "gates_passed": False,
                    "total_penalties": 0.0,
                    "total_issues": 0,
                    "critical_issues": 0,
                    "major_issues": 0,
                    "minor_issues": 0,
                }
            })
    
    # Save comprehensive summary
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_path = output_dir / f"evaluation_summary_{timestamp}.json"
    
    summary_data = {
        "evaluation_timestamp": datetime.now().isoformat(),
        "evaluator_version": "2.0",  # Updated version
        "total_files": len(all_results),
        "passed": sum(1 for r in all_results if r.get("summary", {}).get("passed", False)),
        "failed": sum(1 for r in all_results if not r.get("summary", {}).get("passed", False)),
        "average_scores": {
            "static": sum(r.get("summary", {}).get("static_score", 0) for r in all_results) / len(all_results) if all_results else 0,
            "compliance": sum(
                r.get("summary", {}).get("compliance_score", 0) 
                for r in all_results 
                if r.get("summary", {}).get("compliance_score") is not None
            ) / len([r for r in all_results if r.get("summary", {}).get("compliance_score") is not None]) if all_results else 0,
            "combined": sum(r.get("summary", {}).get("combined_score", 0) for r in all_results) / len(all_results) if all_results else 0,
        },
        "results": all_results,
    }
    
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, default=str)
    
    logging.info(f"\nComprehensive summary saved to: {summary_path}")
    
    # Output
    if args.json_output:
        print(json.dumps(summary_data, indent=2, default=str))
    elif not args.no_summary:
        print_detailed_summary(all_results)
    
    # Print final status
    print("\n" + "=" * 80)
    if summary_data["failed"] == 0:
        print("✓ All evaluations PASSED")
        print("=" * 80)
        return 0
    else:
        print(f"✗ {summary_data['failed']} evaluation(s) FAILED")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())