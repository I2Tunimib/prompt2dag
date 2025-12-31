#!/usr/bin/env bash
set -euo pipefail

################################################################################
# run_prompt2dag_experiment_v2.sh
#
# PRODUCTION-READY experiment runner with:
#   - Rate limiting protection
#   - Checkpoint/resume capability
#   - Pilot mode for testing
#   - Session-based data management
#   - Comprehensive validation
#   - Non-interactive mode support (nohup/background)
#
# EXPERIMENT SCOPE:
#   - 38 pipelines
#   - 7 standard LLMs × 2 reasoning LLMs = 14 combinations
#   - 3 orchestrators
#   - 1 repeat per session (run 3 sessions manually)
#
# USAGE:
#   # Interactive
#   ./run_prompt2dag_experiment_v2.sh --session 1 [--pilot] [--dry-run]
#   
#   # Background (nohup)
#   nohup ./run_prompt2dag_experiment_v2.sh --session 1 --no-confirm > session1.log 2>&1 &
#   
#   # Resume
#   ./run_prompt2dag_experiment_v2.sh --session 1 --resume --no-confirm
#
################################################################################

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Session number (1, 2, or 3) - SET VIA COMMAND LINE
SESSION_NUM=""

# ===== LLM CONFIGURATION =====
STANDARD_LLMS=(
  "deepinfra:deepseek_ai"       # deepseek-ai/DeepSeek-V3
  "deepinfra:meta_llama"        # meta-llama/Llama-3.3-70B-Instruct
  "deepinfra:qwen"              # Qwen/Qwen2.5-72B-Instruct
  "deepinfra:qwen3"             # Qwen/Qwen3-14B
  "deepinfra:microsoft_phi"     # microsoft/phi-4
  "deepinfra:claude-4-sonnet"   # anthropic/claude-4-sonnet
  "deepinfra:mistralaiSmall"    # mistralai/Mistral-Small-3.1-24B-Instruct-2503
)

REASONING_LLMS=(
  "deepinfra:Qwen3-235B-A22B-Thinking-2507"  # Qwen/Qwen3-235B-A22B-Thinking-2507
  "deepinfra:gemini-2.5-pro"                  # google/gemini-2.5-pro
)

ORCHESTRATORS=("airflow" "prefect" "dagster")

# ===== RATE LIMITING PROTECTION =====
DELAY_BETWEEN_PIPELINES=2      # Seconds between pipeline runs
DELAY_BETWEEN_COMBINATIONS=60  # Seconds between LLM combination switches
DELAY_ON_ERROR=120             # Seconds to wait after an error before retry
MAX_RETRIES=3                  # Max retries per combination

# ===== DIRECTORIES =====
PIPELINE_DIR="Pipeline_Description_Dataset"
DIRECT_PIPELINE_DIR="Pipeline_Description_Dataset_direct"
BASE_OUTPUT_ROOT="outputs/experiment_v2"

# ===== CONTROL FLAGS =====
DRY_RUN=false
PILOT_MODE=false
RESUME_MODE=false
NO_CONFIRM=false
LOG_LEVEL="INFO"

# Pilot mode settings
PILOT_PIPELINE_COUNT=3  # Number of pipelines to test in pilot mode

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

print_header() {
  echo
  echo "╔════════════════════════════════════════════════════════════════════╗"
  printf "║  %-64s  ║\n" "$1"
  echo "╚════════════════════════════════════════════════════════════════════╝"
}

print_subheader() {
  echo
  echo "┌────────────────────────────────────────────────────────────────────┐"
  printf "│  %-64s  │\n" "$1"
  echo "└────────────────────────────────────────────────────────────────────┘"
}

print_status() { echo "▶ $1"; }
print_success() { echo "✓ $1"; }
print_error() { echo "✗ $1" >&2; }
print_warning() { echo "⚠ $1"; }
print_info() { echo "ℹ $1"; }

get_timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
get_file_timestamp() { date '+%Y%m%d_%H%M%S'; }

format_duration() {
  local seconds=$1
  local hours=$((seconds / 3600))
  local minutes=$(((seconds % 3600) / 60))
  local secs=$((seconds % 60))
  printf "%02d:%02d:%02d" "$hours" "$minutes" "$secs"
}

# Countdown timer with message
countdown() {
  local seconds=$1
  local message=$2
  
  while [[ $seconds -gt 0 ]]; do
    printf "\r${message}: %02d seconds remaining..." "$seconds"
    sleep 1
    seconds=$((seconds - 1))
  done
  printf "\r${message}: Done!                        \n"
}

# ==============================================================================
# CHECKPOINT MANAGEMENT
# ==============================================================================

CHECKPOINT_FILE=""

init_checkpoint() {
  CHECKPOINT_FILE="${SESSION_LOGS_DIR}/checkpoint.txt"
  
  if [[ "${RESUME_MODE}" == true ]] && [[ -f "${CHECKPOINT_FILE}" ]]; then
    print_info "Resume mode: Loading checkpoint from ${CHECKPOINT_FILE}"
  else
    # Create new checkpoint file
    echo "0" > "${CHECKPOINT_FILE}"
  fi
}

get_last_completed() {
  if [[ -f "${CHECKPOINT_FILE}" ]]; then
    cat "${CHECKPOINT_FILE}"
  else
    echo "0"
  fi
}

save_checkpoint() {
  local combo_index=$1
  echo "${combo_index}" > "${CHECKPOINT_FILE}"
  print_info "Checkpoint saved: combination ${combo_index} completed"
}

# ==============================================================================
# LLM COMBINATION GENERATOR
# ==============================================================================

generate_llm_pairs() {
  PAIRED_STD_LLMS=()
  PAIRED_REASONING_LLMS=()
  COMBO_NAMES=()
  
  for std_llm in "${STANDARD_LLMS[@]}"; do
    for reasoning_llm in "${REASONING_LLMS[@]}"; do
      PAIRED_STD_LLMS+=("$std_llm")
      PAIRED_REASONING_LLMS+=("$reasoning_llm")
      
      std_short=$(echo "$std_llm" | cut -d':' -f2)
      reasoning_short=$(echo "$reasoning_llm" | cut -d':' -f2)
      COMBO_NAMES+=("${std_short}__${reasoning_short}")
    done
  done
}

# ==============================================================================
# VALIDATION FUNCTIONS
# ==============================================================================

validate_json_files() {
  local expected=$1
  local actual=$(find "${SESSION_RESULTS_DIR}" -name "results_*.json" -type f 2>/dev/null | wc -l | tr -d ' ')
  
  echo "  Expected: ${expected}"
  echo "  Found:    ${actual}"
  
  if [[ ${actual} -ge ${expected} ]]; then
    print_success "Validation passed"
    return 0
  else
    print_warning "Missing $((expected - actual)) files"
    return 1
  fi
}

validate_combination_results() {
  local combo_name=$1
  local expected_files=$2
  
  # Count files for this combination
  local pattern="results_*__std-${combo_name%%__*}*__reason-*${combo_name##*__}*"
  local actual=$(find "${SESSION_RESULTS_DIR}" -name "results_*.json" -type f | grep -c "${combo_name%%__*}" 2>/dev/null || echo "0")
  
  if [[ ${actual} -ge ${expected_files} ]]; then
    return 0
  else
    return 1
  fi
}

# ==============================================================================
# USAGE
# ==============================================================================

show_usage() {
  cat << EOF
Usage: $0 --session N [OPTIONS]

Required:
  --session N       Session number (1, 2, or 3)

Options:
  --pilot           Run pilot mode (${PILOT_PIPELINE_COUNT} pipelines only)
  --resume          Resume from last checkpoint
  --no-confirm      Skip confirmation prompt (for nohup/background)
  --dry-run         Show what would run without executing
  --help            Show this help message

Examples:
  # Interactive run with confirmation
  $0 --session 1

  # Background run (no prompt needed)
  nohup $0 --session 1 --no-confirm > session1.log 2>&1 &

  # Pilot test (3 pipelines)
  $0 --session 1 --pilot

  # Resume interrupted session
  $0 --session 1 --resume --no-confirm

  # Dry run to preview
  $0 --session 1 --dry-run

Monitoring:
  # Watch log
  tail -f session1.log

  # Count results
  watch -n 60 'find outputs/experiment_v2/session_1/results -name "*.json" | wc -l'

  # Check checkpoint
  cat outputs/experiment_v2/session_1/logs/checkpoint.txt
EOF
}

# ==============================================================================
# PARSE ARGUMENTS
# ==============================================================================

while [[ $# -gt 0 ]]; do
  case $1 in
    --session)
      SESSION_NUM="$2"
      shift 2
      ;;
    --pilot)
      PILOT_MODE=true
      shift
      ;;
    --resume)
      RESUME_MODE=true
      shift
      ;;
    --no-confirm)
      NO_CONFIRM=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help)
      show_usage
      exit 0
      ;;
    *)
      print_error "Unknown option: $1"
      show_usage
      exit 1
      ;;
  esac
done

# Validate session number
if [[ -z "${SESSION_NUM}" ]]; then
  print_error "Session number is required"
  show_usage
  exit 1
fi

if [[ ! "${SESSION_NUM}" =~ ^[1-3]$ ]]; then
  print_error "Session number must be 1, 2, or 3"
  exit 1
fi

# ==============================================================================
# SETUP DIRECTORIES
# ==============================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Session-specific directories
SESSION_OUTPUT_ROOT="${BASE_OUTPUT_ROOT}/session_${SESSION_NUM}"
SESSION_RESULTS_DIR="${SESSION_OUTPUT_ROOT}/results"
SESSION_LOGS_DIR="${SESSION_OUTPUT_ROOT}/logs"
SESSION_BACKUP_DIR="${SESSION_OUTPUT_ROOT}/backups"

# Create directories
mkdir -p "${SESSION_RESULTS_DIR}"
mkdir -p "${SESSION_LOGS_DIR}"
mkdir -p "${SESSION_BACKUP_DIR}"

# Paths
PIPELINE_DIR_ABS="${PROJECT_ROOT}/${PIPELINE_DIR}"
DIRECT_PIPELINE_DIR_ABS="${PROJECT_ROOT}/${DIRECT_PIPELINE_DIR}"

# ==============================================================================
# SANITY CHECKS
# ==============================================================================

print_header "SESSION ${SESSION_NUM} - INITIALIZATION"

echo "Mode:       $([ "${PILOT_MODE}" == true ] && echo "PILOT" || echo "FULL")"
echo "Resume:     ${RESUME_MODE}"
echo "No Confirm: ${NO_CONFIRM}"
echo "Dry Run:    ${DRY_RUN}"
echo

# Check directories
if [[ ! -d "${PIPELINE_DIR_ABS}" ]]; then
  print_error "Pipeline directory not found: ${PIPELINE_DIR_ABS}"
  exit 1
fi
print_success "Pipeline directory found"

# Check config files
for cfg in config_llm.json config_reasoning_llm.json; do
  if [[ ! -f "${PROJECT_ROOT}/${cfg}" ]]; then
    print_error "Config file not found: ${cfg}"
    exit 1
  fi
done
print_success "Config files found"

# Check Python scripts
for script in batch_run_prompt2dag.py prompt2dag_json_to_csv.py; do
  if [[ ! -f "${PROJECT_ROOT}/${script}" ]]; then
    print_error "Script not found: ${script}"
    exit 1
  fi
done
print_success "Python scripts found"

# ==============================================================================
# BUILD PIPELINE LIST
# ==============================================================================

print_subheader "PIPELINE LIST"

PIPELINE_LIST_FILE="${SESSION_LOGS_DIR}/pipeline_list.txt"

# Create pipeline list
find "${PIPELINE_DIR_ABS}" -name "*.txt" -exec basename {} \; | sort > "${PIPELINE_LIST_FILE}"
TOTAL_PIPELINES=$(wc -l < "${PIPELINE_LIST_FILE}" | tr -d ' ')

print_status "Total pipelines available: ${TOTAL_PIPELINES}"

# Apply pilot mode limit
if [[ "${PILOT_MODE}" == true ]]; then
  PILOT_LIST_FILE="${SESSION_LOGS_DIR}/pipeline_list_pilot.txt"
  head -n "${PILOT_PIPELINE_COUNT}" "${PIPELINE_LIST_FILE}" > "${PILOT_LIST_FILE}"
  PIPELINE_LIST_FILE="${PILOT_LIST_FILE}"
  ACTIVE_PIPELINE_COUNT=${PILOT_PIPELINE_COUNT}
  print_warning "PILOT MODE: Using only ${PILOT_PIPELINE_COUNT} pipelines"
else
  ACTIVE_PIPELINE_COUNT=${TOTAL_PIPELINES}
fi

echo
echo "Pipelines to process:"
cat "${PIPELINE_LIST_FILE}" | head -5 | sed 's/^/  - /'
if [[ ${ACTIVE_PIPELINE_COUNT} -gt 5 ]]; then
  echo "  ... and $((ACTIVE_PIPELINE_COUNT - 5)) more"
fi

# ==============================================================================
# EXPERIMENT CONFIGURATION
# ==============================================================================

print_subheader "EXPERIMENT CONFIGURATION"

generate_llm_pairs

NUM_STD=${#STANDARD_LLMS[@]}
NUM_REASONING=${#REASONING_LLMS[@]}
NUM_COMBINATIONS=${#PAIRED_STD_LLMS[@]}
NUM_ORCHESTRATORS=${#ORCHESTRATORS[@]}

echo "LLM Configuration:"
echo "  Standard LLMs:    ${NUM_STD}"
echo "  Reasoning LLMs:   ${NUM_REASONING}"
echo "  Combinations:     ${NUM_COMBINATIONS}"
echo

echo "Standard LLMs:"
for i in "${!STANDARD_LLMS[@]}"; do
  echo "  [$((i+1))] ${STANDARD_LLMS[$i]}"
done
echo

echo "Reasoning LLMs:"
for i in "${!REASONING_LLMS[@]}"; do
  echo "  [$((i+1))] ${REASONING_LLMS[$i]}"
done
echo

echo "All ${NUM_COMBINATIONS} Combinations:"
for i in "${!COMBO_NAMES[@]}"; do
  echo "  [$((i+1))] ${COMBO_NAMES[$i]}"
done
echo

# Calculate totals
RUNS_PER_COMBINATION=$((ACTIVE_PIPELINE_COUNT * NUM_ORCHESTRATORS))
TOTAL_RUNS=$((NUM_COMBINATIONS * RUNS_PER_COMBINATION))
EXPECTED_JSON_FILES=$((ACTIVE_PIPELINE_COUNT * NUM_COMBINATIONS))

echo "Run Calculations:"
echo "  Pipelines:              ${ACTIVE_PIPELINE_COUNT}"
echo "  Orchestrators:          ${NUM_ORCHESTRATORS}"
echo "  Combinations:           ${NUM_COMBINATIONS}"
echo "  Runs per combination:   ${RUNS_PER_COMBINATION}"
echo "  Total DAG generations:  ${TOTAL_RUNS}"
echo "  Expected JSON files:    ${EXPECTED_JSON_FILES}"
echo

# Time estimates (conservative: 3 min per pipeline)
EST_MINUTES=$((TOTAL_RUNS * 3 / 60))
EST_HOURS=$((EST_MINUTES / 60))
EST_MINS_REMAINDER=$((EST_MINUTES % 60))
echo "Time Estimate (conservative):"
echo "  ~${EST_HOURS} hours ${EST_MINS_REMAINDER} minutes"
echo

# ==============================================================================
# RATE LIMITING SETTINGS
# ==============================================================================

print_subheader "RATE LIMITING CONFIGURATION"

echo "Delays:"
echo "  Between pipelines:     ${DELAY_BETWEEN_PIPELINES}s"
echo "  Between combinations:  ${DELAY_BETWEEN_COMBINATIONS}s"
echo "  On error:              ${DELAY_ON_ERROR}s"
echo "  Max retries:           ${MAX_RETRIES}"
echo

# ==============================================================================
# SAVE EXPERIMENT METADATA
# ==============================================================================

TIMESTAMP=$(get_file_timestamp)
METADATA_FILE="${SESSION_LOGS_DIR}/experiment_metadata_${TIMESTAMP}.json"
MAIN_LOG="${SESSION_LOGS_DIR}/experiment_${TIMESTAMP}.log"

# Create metadata JSON
cat > "${METADATA_FILE}" << EOF
{
  "session_number": ${SESSION_NUM},
  "experiment_start": "$(get_timestamp)",
  "pilot_mode": ${PILOT_MODE},
  "resume_mode": ${RESUME_MODE},
  "dry_run": ${DRY_RUN},
  "project_root": "${PROJECT_ROOT}",
  "pipeline_count": ${ACTIVE_PIPELINE_COUNT},
  "standard_llms": [
$(printf '    "%s"' "${STANDARD_LLMS[0]}")$(printf ',\n    "%s"' "${STANDARD_LLMS[@]:1}")
  ],
  "reasoning_llms": [
$(printf '    "%s"' "${REASONING_LLMS[0]}")$(printf ',\n    "%s"' "${REASONING_LLMS[@]:1}")
  ],
  "num_combinations": ${NUM_COMBINATIONS},
  "orchestrators": ["${ORCHESTRATORS[0]}", "${ORCHESTRATORS[1]}", "${ORCHESTRATORS[2]}"],
  "expected_json_files": ${EXPECTED_JSON_FILES},
  "total_runs": ${TOTAL_RUNS},
  "rate_limiting": {
    "delay_between_pipelines": ${DELAY_BETWEEN_PIPELINES},
    "delay_between_combinations": ${DELAY_BETWEEN_COMBINATIONS},
    "delay_on_error": ${DELAY_ON_ERROR},
    "max_retries": ${MAX_RETRIES}
  }
}
EOF

print_success "Metadata saved: ${METADATA_FILE}"

# ==============================================================================
# INITIALIZE CHECKPOINT
# ==============================================================================

init_checkpoint
LAST_COMPLETED=$(get_last_completed)

if [[ ${LAST_COMPLETED} -gt 0 ]]; then
  print_info "Resuming from combination $((LAST_COMPLETED + 1))"
  print_info "Combinations 1-${LAST_COMPLETED} already completed"
fi

# ==============================================================================
# CONFIRMATION (SMART INTERACTIVE/NON-INTERACTIVE DETECTION)
# ==============================================================================

print_header "READY TO START"

echo "Session:        ${SESSION_NUM}"
echo "Mode:           $([ "${PILOT_MODE}" == true ] && echo "PILOT (${PILOT_PIPELINE_COUNT} pipelines)" || echo "FULL (${ACTIVE_PIPELINE_COUNT} pipelines)")"
echo "Combinations:   ${NUM_COMBINATIONS}"
echo "Total Runs:     ${TOTAL_RUNS}"
echo "Expected Files: ${EXPECTED_JSON_FILES}"
echo "Resume From:    $([[ ${LAST_COMPLETED} -gt 0 ]] && echo "Combination $((LAST_COMPLETED + 1))" || echo "Beginning")"
echo
echo "Output:         ${SESSION_OUTPUT_ROOT}"
echo

if [[ "${DRY_RUN}" == true ]]; then
  print_warning "DRY RUN MODE - No actual execution"
  echo
fi

# Smart confirmation handling
SHOULD_CONFIRM=true

# Skip confirmation if:
# 1. --no-confirm flag is set
# 2. Running in non-interactive mode (nohup, background, pipe)
# 3. Dry run mode

if [[ "${NO_CONFIRM}" == true ]]; then
  SHOULD_CONFIRM=false
  print_info "Skipping confirmation (--no-confirm flag)"
elif [[ ! -t 0 ]] || [[ ! -t 1 ]]; then
  SHOULD_CONFIRM=false
  print_info "Non-interactive mode detected - proceeding automatically"
elif [[ "${DRY_RUN}" == true ]]; then
  SHOULD_CONFIRM=false
fi

if [[ "${SHOULD_CONFIRM}" == true ]]; then
  echo
  read -p "Proceed with experiment? (y/N) " confirm
  echo
  
  if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
    print_warning "Aborted by user"
    exit 0
  fi
  
  print_success "Confirmed - starting experiment"
else
  print_success "Auto-confirmed - starting experiment"
  sleep 2
fi

echo

# ==============================================================================
# MAIN EXPERIMENT LOOP
# ==============================================================================

print_header "STARTING EXPERIMENT - SESSION ${SESSION_NUM}"

START_TIME=$(date +%s)
COMPLETED_COMBOS=0
FAILED_COMBOS=0
TOTAL_SUCCESS=0
TOTAL_FAILURE=0

# Log start
{
  echo "========================================"
  echo "Session ${SESSION_NUM} Started: $(get_timestamp)"
  echo "Pilot Mode: ${PILOT_MODE}"
  echo "Pipelines: ${ACTIVE_PIPELINE_COUNT}"
  echo "Combinations: ${NUM_COMBINATIONS}"
  echo "========================================"
} | tee -a "${MAIN_LOG}"

for combo_idx in "${!PAIRED_STD_LLMS[@]}"; do
  
  # Skip already completed combinations (resume mode)
  if [[ ${combo_idx} -lt ${LAST_COMPLETED} ]]; then
    print_info "Skipping combination $((combo_idx + 1)) (already completed)"
    COMPLETED_COMBOS=$((COMPLETED_COMBOS + 1))
    continue
  fi
  
  std_llm="${PAIRED_STD_LLMS[$combo_idx]}"
  reasoning_llm="${PAIRED_REASONING_LLMS[$combo_idx]}"
  combo_name="${COMBO_NAMES[$combo_idx]}"
  combo_num=$((combo_idx + 1))
  
  print_subheader "COMBINATION ${combo_num}/${NUM_COMBINATIONS}: ${combo_name}"
  
  echo "Standard LLM:  ${std_llm}"
  echo "Reasoning LLM: ${reasoning_llm}"
  echo
  
  # Combination-specific log
  COMBO_LOG="${SESSION_LOGS_DIR}/combo_${combo_num}_${combo_name}_${TIMESTAMP}.log"
  
  # Build command
  BATCH_CMD=(
    python "${PROJECT_ROOT}/batch_run_prompt2dag.py"
    --pipeline-list-file "${PIPELINE_LIST_FILE}"
    --std-llms "${std_llm}"
    --reasoning-llms "${reasoning_llm}"
    --orchestrators "${ORCHESTRATORS[@]}"
    --pipeline-dir "${PIPELINE_DIR}"
    --direct-pipeline-dir "${DIRECT_PIPELINE_DIR}"
    --output-root "${SESSION_OUTPUT_ROOT}"
    --results-root "results"
    --log-level "${LOG_LEVEL}"
  )
  
  if [[ "${DRY_RUN}" == true ]]; then
    print_status "[DRY RUN] Would execute:"
    echo "  ${BATCH_CMD[*]}"
    echo
    COMPLETED_COMBOS=$((COMPLETED_COMBOS + 1))
    continue
  fi
  
  # Execute with retry logic
  RETRY_COUNT=0
  COMBO_SUCCESS=false
  
  while [[ ${RETRY_COUNT} -lt ${MAX_RETRIES} ]] && [[ "${COMBO_SUCCESS}" == false ]]; do
    
    if [[ ${RETRY_COUNT} -gt 0 ]]; then
      print_warning "Retry ${RETRY_COUNT}/${MAX_RETRIES} for combination ${combo_num}"
      countdown ${DELAY_ON_ERROR} "Waiting before retry"
    fi
    
    COMBO_START=$(date +%s)
    
    print_status "Executing..."
    
    if "${BATCH_CMD[@]}" 2>&1 | tee -a "${COMBO_LOG}"; then
      COMBO_SUCCESS=true
      TOTAL_SUCCESS=$((TOTAL_SUCCESS + RUNS_PER_COMBINATION))
    else
      RETRY_COUNT=$((RETRY_COUNT + 1))
      print_error "Combination ${combo_num} failed (attempt ${RETRY_COUNT})"
      
      # Log failure
      echo "[$(get_timestamp)] Combination ${combo_num} FAILED (attempt ${RETRY_COUNT})" | tee -a "${MAIN_LOG}"
    fi
    
  done
  
  COMBO_END=$(date +%s)
  COMBO_DURATION=$((COMBO_END - COMBO_START))
  
  if [[ "${COMBO_SUCCESS}" == true ]]; then
    COMPLETED_COMBOS=$((COMPLETED_COMBOS + 1))
    print_success "Combination ${combo_num} completed in $(format_duration ${COMBO_DURATION})"
    
    # Save checkpoint
    save_checkpoint ${combo_num}
    
    # Log success
    echo "[$(get_timestamp)] Combination ${combo_num} SUCCESS (${COMBO_DURATION}s)" | tee -a "${MAIN_LOG}"
  else
    FAILED_COMBOS=$((FAILED_COMBOS + 1))
    TOTAL_FAILURE=$((TOTAL_FAILURE + RUNS_PER_COMBINATION))
    print_error "Combination ${combo_num} FAILED after ${MAX_RETRIES} retries"
    
    # Log permanent failure
    echo "[$(get_timestamp)] Combination ${combo_num} PERMANENT FAILURE" | tee -a "${MAIN_LOG}"
  fi
  
  # Validate results for this combination
  print_status "Validating results..."
  FOUND_FOR_COMBO=$(find "${SESSION_RESULTS_DIR}" -name "results_*.json" -type f 2>/dev/null | wc -l | tr -d ' ')
  EXPECTED_TOTAL_SO_FAR=$((combo_num * ACTIVE_PIPELINE_COUNT))
  echo "  JSON files: ${FOUND_FOR_COMBO}/${EXPECTED_TOTAL_SO_FAR} expected so far"
  
  # Progress report
  ELAPSED=$(($(date +%s) - START_TIME))
  PROGRESS_PCT=$(( (combo_num * 100) / NUM_COMBINATIONS ))
  
  if [[ ${combo_num} -lt ${NUM_COMBINATIONS} ]]; then
    REMAINING_COMBOS=$((NUM_COMBINATIONS - combo_num))
    AVG_TIME=$((ELAPSED / combo_num))
    ETA_SECONDS=$((AVG_TIME * REMAINING_COMBOS))
    
    echo
    print_status "Progress: ${combo_num}/${NUM_COMBINATIONS} (${PROGRESS_PCT}%)"
    print_status "Elapsed: $(format_duration ${ELAPSED})"
    print_status "ETA: $(format_duration ${ETA_SECONDS})"
    
    # Rate limiting delay between combinations
    if [[ ${combo_num} -lt ${NUM_COMBINATIONS} ]]; then
      echo
      countdown ${DELAY_BETWEEN_COMBINATIONS} "Rate limiting pause"
    fi
  fi
  
  echo
done

# ==============================================================================
# FINAL VALIDATION
# ==============================================================================

if [[ "${DRY_RUN}" == false ]]; then
  
  print_header "FINAL VALIDATION"
  
  print_status "Checking result files..."
  echo
  
  FINAL_JSON_COUNT=$(find "${SESSION_RESULTS_DIR}" -name "results_*.json" -type f 2>/dev/null | wc -l | tr -d ' ')
  
  echo "Results Summary:"
  echo "  Expected JSON files: ${EXPECTED_JSON_FILES}"
  echo "  Found JSON files:    ${FINAL_JSON_COUNT}"
  echo
  
  if [[ ${FINAL_JSON_COUNT} -eq ${EXPECTED_JSON_FILES} ]]; then
    print_success "ALL ${EXPECTED_JSON_FILES} RESULT FILES PRESENT!"
  elif [[ ${FINAL_JSON_COUNT} -gt ${EXPECTED_JSON_FILES} ]]; then
    print_warning "Found $((FINAL_JSON_COUNT - EXPECTED_JSON_FILES)) extra files (possible duplicates)"
  else
    print_error "Missing $((EXPECTED_JSON_FILES - FINAL_JSON_COUNT)) files"
  fi
  
  # Count by LLM
  echo
  echo "Results by Standard LLM:"
  for std_llm in "${STANDARD_LLMS[@]}"; do
    std_short=$(echo "$std_llm" | cut -d':' -f2)
    count=$(find "${SESSION_RESULTS_DIR}" -name "results_*__std-deepinfra-${std_short}__*.json" -type f 2>/dev/null | wc -l | tr -d ' ')
    expected=$((ACTIVE_PIPELINE_COUNT * NUM_REASONING))
    status=$([[ ${count} -eq ${expected} ]] && echo "✓" || echo "⚠")
    echo "  ${status} ${std_short}: ${count}/${expected}"
  done
  
  echo
  echo "Results by Reasoning LLM:"
  for reasoning_llm in "${REASONING_LLMS[@]}"; do
    reasoning_short=$(echo "$reasoning_llm" | cut -d':' -f2)
    count=$(find "${SESSION_RESULTS_DIR}" -name "results_*__reason-deepinfra-${reasoning_short}__*.json" -type f 2>/dev/null | wc -l | tr -d ' ')
    expected=$((ACTIVE_PIPELINE_COUNT * NUM_STD))
    status=$([[ ${count} -eq ${expected} ]] && echo "✓" || echo "⚠")
    echo "  ${status} ${reasoning_short}: ${count}/${expected}"
  done
  
fi

# ==============================================================================
# GENERATE CSV
# ==============================================================================

if [[ "${DRY_RUN}" == false ]] && [[ ${FINAL_JSON_COUNT:-0} -gt 0 ]]; then
  
  print_header "GENERATING CSV"
  
  cd "${SESSION_RESULTS_DIR}"
  
  CSV_LOG="${SESSION_LOGS_DIR}/csv_generation_${TIMESTAMP}.log"
  
  print_status "Converting ${FINAL_JSON_COUNT} JSON files to CSV..."
  
  if python "${PROJECT_ROOT}/prompt2dag_json_to_csv.py" results_*.json 2>&1 | tee "${CSV_LOG}"; then
    
    if [[ -f "consolidated_results.csv" ]]; then
      CSV_ROWS=$(tail -n +2 "consolidated_results.csv" 2>/dev/null | wc -l | tr -d ' ')
      CSV_COLS=$(head -1 "consolidated_results.csv" 2>/dev/null | awk -F',' '{print NF}')
      CSV_SIZE=$(du -h "consolidated_results.csv" 2>/dev/null | cut -f1)
      
      print_success "CSV generated successfully!"
      echo "  File:    ${SESSION_RESULTS_DIR}/consolidated_results.csv"
      echo "  Size:    ${CSV_SIZE}"
      echo "  Rows:    ${CSV_ROWS}"
      echo "  Columns: ${CSV_COLS}"
      
      # Create backup
      BACKUP_NAME="consolidated_results_session${SESSION_NUM}_${TIMESTAMP}.csv"
      cp "consolidated_results.csv" "${SESSION_BACKUP_DIR}/${BACKUP_NAME}"
      print_success "Backup created: ${SESSION_BACKUP_DIR}/${BACKUP_NAME}"
    fi
    
  else
    print_error "CSV generation failed - check ${CSV_LOG}"
  fi
  
fi

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))

print_header "SESSION ${SESSION_NUM} COMPLETE"

echo "Execution Summary:"
echo "  Session:           ${SESSION_NUM}"
echo "  Mode:              $([ "${PILOT_MODE}" == true ] && echo "PILOT" || echo "FULL")"
echo "  Duration:          $(format_duration ${TOTAL_DURATION})"
echo "  Combinations:      ${COMPLETED_COMBOS}/${NUM_COMBINATIONS} completed"
echo "  Failed:            ${FAILED_COMBOS}"
echo

if [[ "${DRY_RUN}" == false ]]; then
  echo "Results:"
  echo "  JSON Files:        ${FINAL_JSON_COUNT:-0}/${EXPECTED_JSON_FILES}"
  echo "  CSV File:          $([ -f "${SESSION_RESULTS_DIR}/consolidated_results.csv" ] && echo "✓ Created" || echo "✗ Not created")"
  echo
fi

echo "Output Locations:"
echo "  Results:   ${SESSION_RESULTS_DIR}"
echo "  Logs:      ${SESSION_LOGS_DIR}"
echo "  Backups:   ${SESSION_BACKUP_DIR}"
echo

# Log completion
{
  echo "========================================"
  echo "Session ${SESSION_NUM} Completed: $(get_timestamp)"
  echo "Duration: $(format_duration ${TOTAL_DURATION})"
  echo "Combinations: ${COMPLETED_COMBOS}/${NUM_COMBINATIONS}"
  if [[ "${DRY_RUN}" == false ]]; then
    echo "JSON Files: ${FINAL_JSON_COUNT:-0}/${EXPECTED_JSON_FILES}"
  fi
  echo "========================================"
} | tee -a "${MAIN_LOG}"

# Next steps
if [[ ${SESSION_NUM} -lt 3 ]] && [[ "${DRY_RUN}" == false ]]; then
  echo "Next Session:"
  echo "  Wait some time, then run:"
  echo "  nohup ./run_prompt2dag_experiment_v2.sh --session $((SESSION_NUM + 1)) --no-confirm > session$((SESSION_NUM + 1))_output.log 2>&1 &"
  echo
fi

print_success "Session ${SESSION_NUM} finished at $(get_timestamp)"

# Exit with appropriate code
if [[ ${FAILED_COMBOS} -gt 0 ]]; then
  exit 1
else
  exit 0
fi