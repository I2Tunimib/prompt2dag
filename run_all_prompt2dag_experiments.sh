#!/usr/bin/env bash
set -euo pipefail

################################################################################
# run_all_prompt2dag_experiments.sh
#
# PURPOSE:
#   Execute comprehensive Prompt2DAG experiments across:
#   - 38 pipelines (from Pipeline_Description_Dataset/)
#   - 2 std LLM × reasoning LLM pairs
#   - 3 orchestrators (Airflow, Prefect, Dagster)
#   - 3 full repetitions (for robustness)
#
# TOTAL RUNS: 38 × 2 × 3 = 228 pipelines per repetition × 3 repetitions = 684 total
#
# USAGE:
#   chmod +x run_all_prompt2dag_experiments.sh
#   ./run_all_prompt2dag_experiments.sh [--dry-run] [--continue-from RUN_N]
#
# OPTIONS:
#   --dry-run           : Show what would be run, don't execute
#   --continue-from N   : Resume from run N (1-3) if interrupted
#
################################################################################

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Number of full repetitions of all experiments
REPEATS=3

# Standard/Direct LLMs (provider:model)
# MUST be paired 1:1 with REASONING_LLMS
STD_LLMS=(
  "deepinfra:qwen"
  "deepinfra:gpt-oss-120b"
)

# Reasoning LLMs (provider:model)
# Paired: qwen ↔ DeepSeek_R1, gpt-oss-120b ↔ Kimi-K2-Thinking
REASONING_LLMS=(
  "deepinfra:DeepSeek_R1"
  "deepinfra:Kimi-K2-Thinking"
)

# Orchestrators
ORCHESTRATORS=("airflow" "prefect" "dagster")

# Directories
PIPELINE_DIR="Pipeline_Description_Dataset"
DIRECT_PIPELINE_DIR="Pipeline_Description_Dataset_direct"

# Output structure
OUTPUT_ROOT="outputs/comprehensive_batch_all"
RESULTS_ROOT="${OUTPUT_ROOT}/extracted_results_all"
LOGS_ROOT="${OUTPUT_ROOT}/logs_all"

# Logging and control
LOG_LEVEL="INFO"
DRY_RUN=false
CONTINUE_FROM=0

# ==============================================================================
# FUNCTIONS
# ==============================================================================

# Print colored messages
print_header() {
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  $1"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

print_status() {
  echo "▶ $1"
}

print_success() {
  echo "✓ $1"
}

print_error() {
  echo "✗ $1" >&2
}

print_warning() {
  echo "⚠ $1" >&2
}

# Get current timestamp (cross-platform compatible)
get_timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

# Estimate time remaining (cross-platform)
estimate_time() {
  local total_runs=$1
  local completed=$2
  local elapsed_seconds=$3
  
  if [[ $completed -eq 0 ]]; then
    echo "N/A"
    return
  fi
  
  local avg_time=$((elapsed_seconds / completed))
  local remaining_runs=$((total_runs - completed))
  local remaining_seconds=$((avg_time * remaining_runs))
  
  local hours=$((remaining_seconds / 3600))
  local minutes=$(((remaining_seconds % 3600) / 60))
  
  printf "%02d:%02d\n" "$hours" "$minutes"
}

# ==============================================================================
# PARSE ARGUMENTS
# ==============================================================================

while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --continue-from)
      CONTINUE_FROM="$2"
      shift 2
      ;;
    *)
      print_error "Unknown option: $1"
      exit 1
      ;;
  esac
done

# ==============================================================================
# SANITY CHECKS
# ==============================================================================

print_header "SANITY CHECKS"

# Check array lengths
if [[ ${#STD_LLMS[@]} -ne ${#REASONING_LLMS[@]} ]]; then
  print_error "STD_LLMS (${#STD_LLMS[@]}) and REASONING_LLMS (${#REASONING_LLMS[@]}) must match!"
  exit 1
fi
print_success "LLM pairing valid: ${#STD_LLMS[@]} pairs"

# Check directories exist
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR_ABS="${PROJECT_ROOT}/${PIPELINE_DIR}"
DIRECT_PIPELINE_DIR_ABS="${PROJECT_ROOT}/${DIRECT_PIPELINE_DIR}"

if [[ ! -d "${PIPELINE_DIR_ABS}" ]]; then
  print_error "Pipeline dir not found: ${PIPELINE_DIR_ABS}"
  exit 1
fi
print_success "Pipeline dir found: ${PIPELINE_DIR_ABS}"

if [[ ! -d "${DIRECT_PIPELINE_DIR_ABS}" ]]; then
  print_warning "Direct pipeline dir not found: ${DIRECT_PIPELINE_DIR_ABS}"
  print_warning "Will use main descriptions as fallback"
fi

# Check Python scripts exist
for script in batch_run_prompt2dag.py prompt2dag_json_to_csv.py; do
  if [[ ! -f "${PROJECT_ROOT}/${script}" ]]; then
    print_error "Script not found: ${script}"
    exit 1
  fi
done
print_success "Python scripts found"

# Create output directories
OUTPUT_ROOT_ABS="${PROJECT_ROOT}/${OUTPUT_ROOT}"
RESULTS_ROOT_ABS="${PROJECT_ROOT}/${RESULTS_ROOT}"
LOGS_ROOT_ABS="${PROJECT_ROOT}/${LOGS_ROOT}"

mkdir -p "${OUTPUT_ROOT_ABS}"
mkdir -p "${RESULTS_ROOT_ABS}"
mkdir -p "${LOGS_ROOT_ABS}"

print_success "Output directories created"

# ==============================================================================
# BUILD PIPELINE LIST
# ==============================================================================

print_header "BUILDING PIPELINE LIST"

PIPELINE_LIST_FILE="${LOGS_ROOT_ABS}/pipeline_list_all.txt"

# Count pipelines
PIPELINE_COUNT=$(find "${PIPELINE_DIR_ABS}" -name "*.txt" | wc -l)
print_status "Found ${PIPELINE_COUNT} pipelines"

# Create list
find "${PIPELINE_DIR_ABS}" -name "*.txt" -exec basename {} \; | sort > "${PIPELINE_LIST_FILE}"
print_success "Pipeline list written to ${PIPELINE_LIST_FILE}"

# Validate
if [[ $PIPELINE_COUNT -ne $(wc -l < "${PIPELINE_LIST_FILE}") ]]; then
  print_error "Pipeline count mismatch!"
  exit 1
fi

# Show first and last 5
echo
print_status "First 5 pipelines:"
head -5 "${PIPELINE_LIST_FILE}" | sed 's/^/  - /'
echo
print_status "Last 5 pipelines:"
tail -5 "${PIPELINE_LIST_FILE}" | sed 's/^/  - /'
echo

# ==============================================================================
# EXPERIMENT SUMMARY
# ==============================================================================

print_header "EXPERIMENT SUMMARY"

echo "LLM Pairs:"
for i in "${!STD_LLMS[@]}"; do
  echo "  [$(($i+1))] ${STD_LLMS[$i]} ↔ ${REASONING_LLMS[$i]}"
done
echo

echo "Orchestrators: ${ORCHESTRATORS[@]}"
echo "Pipelines: ${PIPELINE_COUNT}"
echo "Repeats: ${REPEATS}"
echo

RUNS_PER_REPEAT=$((PIPELINE_COUNT * ${#STD_LLMS[@]} * ${#ORCHESTRATORS[@]}))
TOTAL_RUNS=$((RUNS_PER_REPEAT * REPEATS))

echo "Calculations:"
echo "  Runs per pipeline: ${#STD_LLMS[@]} (LLM pairs) × ${#ORCHESTRATORS[@]} (orchestrators) = $((${#STD_LLMS[@]} * ${#ORCHESTRATORS[@]}))"
echo "  Runs per repeat: ${PIPELINE_COUNT} × $((${#STD_LLMS[@]} * ${#ORCHESTRATORS[@]})) = ${RUNS_PER_REPEAT}"
echo "  Total runs: ${RUNS_PER_REPEAT} × ${REPEATS} = ${TOTAL_RUNS}"
echo

# ==============================================================================
# EXECUTION LOG
# ==============================================================================

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MAIN_LOG="${LOGS_ROOT_ABS}/experiment_${TIMESTAMP}.log"

{
  echo "Prompt2DAG Batch Experiment"
  echo "Started: $(get_timestamp)"
  echo "Project Root: ${PROJECT_ROOT}"
  echo "Total Expected Runs: ${TOTAL_RUNS}"
  echo "Dry Run: ${DRY_RUN}"
  echo ""
} | tee "${MAIN_LOG}"

# ==============================================================================
# RUN EXPERIMENTS
# ==============================================================================

START_TIME=$(date +%s)
GLOBAL_RUN_COUNT=0
GLOBAL_SUCCESS=0
GLOBAL_FAILURE=0

for run_idx in $(seq 1 "${REPEATS}"); do
  
  # Skip if continuing from later run
  if [[ $run_idx -lt $CONTINUE_FROM ]]; then
    print_status "Skipping run ${run_idx} (continuing from run ${CONTINUE_FROM})"
    continue
  fi

  print_header "REPETITION ${run_idx}/${REPEATS}"
  
  RUN_START=$(date +%s)
  RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  RUN_LOG="${LOGS_ROOT_ABS}/run_${run_idx}_${RUN_TIMESTAMP}.log"
  
  # Build batch command (WITHOUT --max-workers)
  BATCH_CMD=(
    python "${PROJECT_ROOT}/batch_run_prompt2dag.py"
    --pipeline-list-file "${PIPELINE_LIST_FILE}"
    --std-llms "${STD_LLMS[@]}"
    --reasoning-llms "${REASONING_LLMS[@]}"
    --orchestrators "${ORCHESTRATORS[@]}"
    --pipeline-dir "${PIPELINE_DIR}"
    --direct-pipeline-dir "${DIRECT_PIPELINE_DIR}"
    --output-root "${OUTPUT_ROOT}"
    --results-root "${RESULTS_ROOT}"
    --log-level "${LOG_LEVEL}"
  )

  if [[ "${DRY_RUN}" == true ]]; then
    print_status "[DRY RUN] Would execute:"
    echo "  ${BATCH_CMD[@]}"
    echo
    continue
  fi

  # Execute batch
  print_status "Starting batch run ${run_idx}..."
  print_status "Log: ${RUN_LOG}"
  
  if "${BATCH_CMD[@]}" 2>&1 | tee "${RUN_LOG}"; then
    RUN_SUCCESS=true
    GLOBAL_SUCCESS=$((GLOBAL_SUCCESS + RUNS_PER_REPEAT))
  else
    RUN_SUCCESS=false
    GLOBAL_FAILURE=$((GLOBAL_FAILURE + RUNS_PER_REPEAT))
    print_error "Run ${run_idx} had failures - check ${RUN_LOG}"
  fi

  GLOBAL_RUN_COUNT=$((GLOBAL_RUN_COUNT + RUNS_PER_REPEAT))
  
  RUN_END=$(date +%s)
  RUN_DURATION=$((RUN_END - RUN_START))
  
  RUN_HOURS=$((RUN_DURATION / 3600))
  RUN_MINUTES=$(((RUN_DURATION % 3600) / 60))
  RUN_SECONDS=$((RUN_DURATION % 60))
  
  print_success "Run ${run_idx} completed in ${RUN_HOURS}h ${RUN_MINUTES}m ${RUN_SECONDS}s"
  
  # Progress estimate
  if [[ $run_idx -lt $REPEATS ]]; then
    ELAPSED=$(($(date +%s) - START_TIME))
    TIME_REMAINING=$(estimate_time "${TOTAL_RUNS}" "${GLOBAL_RUN_COUNT}" "${ELAPSED}")
    print_status "Progress: ${GLOBAL_RUN_COUNT}/${TOTAL_RUNS} runs completed"
    print_status "Estimated time remaining: ${TIME_REMAINING}"
  fi
  
  echo
done

# ==============================================================================
# AGGREGATE RESULTS
# ==============================================================================

if [[ "${DRY_RUN}" == false ]]; then
  
  print_header "AGGREGATING RESULTS"
  
  cd "${RESULTS_ROOT_ABS}"
  
  RESULT_COUNT=$(find . -name "results_*.json" -type f 2>/dev/null | wc -l)
  print_status "Found ${RESULT_COUNT} extracted JSON files"
  
  if [[ ${RESULT_COUNT} -eq 0 ]]; then
    print_error "No JSON results found in ${RESULTS_ROOT_ABS}"
  else
    print_status "Aggregating into consolidated CSV..."
    
    CSV_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    CSV_LOG="${LOGS_ROOT_ABS}/csv_aggregation_${CSV_TIMESTAMP}.log"
    
    if python "${PROJECT_ROOT}/prompt2dag_json_to_csv.py" results_*.json 2>&1 | tee "${CSV_LOG}"; then
      print_success "CSV aggregation completed"
      
      if [[ -f "${RESULTS_ROOT_ABS}/consolidated_results.csv" ]]; then
        CSV_ROWS=$(tail -n +2 "${RESULTS_ROOT_ABS}/consolidated_results.csv" 2>/dev/null | wc -l)
        CSV_COLS=$(head -1 "${RESULTS_ROOT_ABS}/consolidated_results.csv" 2>/dev/null | awk -F',' '{print NF}')
        print_success "CSV created: ${CSV_ROWS} rows × ${CSV_COLS} columns"
      fi
    else
      print_error "CSV aggregation failed - check ${CSV_LOG}"
    fi
  fi
fi

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

TOTAL_HOURS=$((TOTAL_DURATION / 3600))
TOTAL_MINUTES=$(((TOTAL_DURATION % 3600) / 60))
TOTAL_SECONDS=$((TOTAL_DURATION % 60))

print_header "EXPERIMENT COMPLETE"

echo "Execution Summary:"
echo "  Start Time: $(get_timestamp)"
echo "  End Time: $(get_timestamp)"
echo "  Total Duration: ${TOTAL_HOURS}h ${TOTAL_MINUTES}m ${TOTAL_SECONDS}s"
echo "  Runs Completed: ${GLOBAL_RUN_COUNT}/${TOTAL_RUNS}"
echo "  Successful: ${GLOBAL_SUCCESS}"
echo "  Failed: ${GLOBAL_FAILURE}"
if [[ ${GLOBAL_RUN_COUNT} -gt 0 ]]; then
  SUCCESS_RATE=$(( (GLOBAL_SUCCESS * 100) / GLOBAL_RUN_COUNT ))
  echo "  Success Rate: ${SUCCESS_RATE}%"
fi
echo

echo "Output Locations:"
echo "  Results: ${RESULTS_ROOT_ABS}"
echo "  Logs: ${LOGS_ROOT_ABS}"
echo "  Main Log: ${MAIN_LOG}"
echo

if [[ -f "${RESULTS_ROOT_ABS}/consolidated_results.csv" ]]; then
  echo "✓ Consolidated CSV: ${RESULTS_ROOT_ABS}/consolidated_results.csv"
else
  echo "✗ Consolidated CSV not found"
fi

echo
print_success "All experiments finished at $(get_timestamp)"