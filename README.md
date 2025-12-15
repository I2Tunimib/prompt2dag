# Prompt2DAG: Reproducibility Guide

## Table of Contents
1. [Overview](#overview)
2. [Repository Structure](#repository-structure)
3. [Environment Setup](#environment-setup)
4. [Configuration](#configuration)
5. [Running Experiments](#running-experiments)
6. [Evaluation and Results](#evaluation-and-results)
7. [Reproducing Paper Results](#reproducing-paper-results)
8. [Troubleshooting](#troubleshooting)

---

## Overview

This repository contains the complete implementation and evaluation framework for **Prompt2DAG**, a systematic approach for generating data orchestration DAGs across multiple platforms (Apache Airflow, Dagster, and Prefect) from natural language pipeline descriptions.

### Key Features

- **Multi-stage pipeline**: 4-step generation process (Analysis → Intermediate → Code → Evaluation)
- **Multiple methods**: Direct prompting, Prompt2DAG pipeline, and reasoning-based generation
- **Three orchestrators**: Airflow, Dagster, and Prefect
- **Three generation strategies**: Template-based, LLM-driven, and Hybrid
- **Comprehensive evaluation**: Structural analysis, semantic analysis, platform compliance, and unified scoring
- **Two execution scenarios**: API-based and local Step-1 inference

### Paper Research Questions

- **RQ1**: How robust and high-quality is Prompt2DAG across different orchestrators and generation strategies?
- **RQ2**: What is the token and cost efficiency of Prompt2DAG compared to direct generation baselines?

---

## Repository Structure

```
.
├── Pipeline_Description_Dataset/          # Input dataset (38 pipelines)
│   ├── [pipeline descriptions]
│   └── [gold-standard references]
│
├── scripts/                               # Core implementation
│   ├── Step_0_direct_prompting.py         # Direct generation baseline
│   ├── Step_0_direct_prompting_reasoning.py  # Direct + reasoning models
│   ├── Step_1_pipeline_analyzer.py        # Orchestrator-agnostic analysis
│   ├── Step_2_orchestrator_generator.py   # Intermediate YAML generation
│   ├── Step_3_code_generator.py           # Orchestrator-specific code gen
│   ├── Step_4_evaluate.py                 # Evaluation and scoring
│   │
│   ├── code_generators/                   # Code generation strategies
│   │   ├── base_generator.py
│   │   ├── template_generator.py
│   │   ├── llm_generator.py
│   │   └── hybrid_generator.py
│   │
│   ├── orchestrator_mappers/              # Intermediate → code mapping
│   │   ├── base_mapper.py
│   │   ├── airflow_mapper.py
│   │   ├── dagster_mapper.py
│   │   └── prefect_mapper.py
│   │
│   ├── evaluators/                        # Evaluation components
│   │   ├── base_evaluator.py
│   │   ├── semantic_analyzer.py           # Semantic similarity (RQ1)
│   │   ├── enhanced_static_analyzer.py    # Structural analysis (SAT, PCT)
│   │   ├── unified_evaluator.py           # Unified scoring
│   │   └── platform_compliance/           # Orchestrator-specific checks
│   │       ├── airflow_compliance.py
│   │       ├── dagster_compliance.py
│   │       └── prefect_compliance.py
│   │
│   ├── validators/                        # Orchestrator validators
│   │   ├── airflow_validator.py
│   │   ├── dagster_validator.py
│   │   ├── prefect_validator.py
│   │   └── validation_rules.yaml
│   │
│   ├── step_evaluators/                   # Step-1 specific evaluation
│   │   ├── dag_graph_validation_cli.py    # DAG graph structure validation
│   │   └── semantic_eval_step1_anl.py     # Semantic similarity analysis
│   │
│   ├── templates/                         # Jinja2 code templates
│   │   ├── airflow/
│   │   ├── dagster/
│   │   └── prefect/
│   │
│   ├── utils/                             # Utility modules
│   │   ├── config_loader.py
│   │   ├── llm_provider.py                # OpenAI-compatible API client
│   │   ├── token_tracker.py               # Token usage accounting (RQ2)
│   │   ├── workflow_schema.py
│   │   └── yaml_helpers.py
│   │
│   └── [other evaluation and helper scripts]
│
├── prompt2dag.py                          # Main CLI orchestrator
├── config_llm.json                        # Standard model configuration
├── config_reasoning_llm.json              # Reasoning model configuration
│
├── analysis/                              # Post-run analysis scripts
│   ├── aggregate_results.py               # CSV aggregation
│   ├── compute_rq1.py                     # RQ1 analysis
│   └── compute_rq2.py                     # RQ2 analysis
│
└── README.md                              # This file
```

---

## Environment Setup

### Prerequisites

- **Python**: 3.10 or higher
- **pip** or **conda** for package management
- **GPU** (optional): Recommended for local Step-1 inference (Scenario B)

### Step 1: Clone and Create Environment

```bash
# Clone repository
git clone <repository-url>
cd prompt2dag

# Create virtual environment
python3.10 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

### Step 2: Install Core Dependencies

```bash
# Install base packages
pip install pandas numpy scipy jinja2 pyyaml

# Install LLM client (OpenAI-compatible)
pip install openai>=1.0.0

# Install semantic evaluation dependencies (optional, for evaluation)
pip install rouge-score bert-score

# Install prompt and token tracking
pip install tiktoken
```

### Step 3: Install Orchestrator Support (Platform-Specific)

Install the orchestrator versions used in the experiments. **This is critical for reproducibility.**

```bash
# Apache Airflow 2.8+
pip install apache-airflow==2.8.0

# Dagster 1.6+
pip install dagster==1.6.0

# Prefect 2.14+
pip install prefect==2.14.0
```

> **Note**: Different versions of orchestrators may have different syntax requirements and validation logic. Ensure you match the versions documented in the paper's appendix.

### Step 4: Verify Installation

```bash
# Test imports
python -c "import airflow; import dagster; import prefect; print('All orchestrators installed')"

# Test LLM provider connectivity (requires API key setup first)
python scripts/test_llm_provider.py --provider deepinfra
```

---

## Configuration

### LLM Provider Setup

The system uses **DeepInfra** as the LLM service provider via OpenAI-compatible REST API.

#### Configuration Files

Two configuration files control model access:

1. **`config_llm.json`** – Standard models (Direct and Prompt2DAG pipeline)
2. **`config_reasoning_llm.json`** – Reasoning models (Direct + Reasoning baseline)

#### Example: `config_llm.json`

```json
{
  "model_settings": {
    "active_provider": "deepinfra",
    "deepinfra": {
      "active_model": "claude_sonnet",
      "models": {
        "claude_sonnet": {
          "model_name": "anthropic/claude-4-sonnet",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        },
        "qwen_72b": {
          "model_name": "Qwen/Qwen2.5-72B-Instruct",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        },
        "deepseek_v3": {
          "model_name": "deepseek-ai/DeepSeek-V3",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        }
      }
    }
  }
}
```

#### Example: `config_reasoning_llm.json`

```json
{
  "model_settings": {
    "active_provider": "deepinfra",
    "deepinfra": {
      "active_model": "deepseek_r1",
      "models": {
        "deepseek_r1": {
          "model_name": "deepseek-ai/DeepSeek-R1",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        },
        "qwen3_235b": {
          "model_name": "Qwen/Qwen3-235B-Thinking",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        },
        "gemini_2_5_pro": {
          "model_name": "google/gemini-2.5-pro",
          "api_key": "${DEEPINFRA_API_KEY}",
          "max_tokens": 8000,
          "temperature": 0,
          "base_url": "https://api.deepinfra.com/v1/openai"
        }
      }
    }
  }
}
```

### Setting API Credentials

**Option 1: Environment Variable (Recommended)**

```bash
export DEEPINFRA_API_KEY="your_api_key_here"
```

**Option 2: Direct in Configuration**

Edit the config files and replace `"${DEEPINFRA_API_KEY}"` with your actual API key.

> **⚠️ Security Warning**: Never commit API keys to version control. Add config files to `.gitignore`.

### Decoding Parameters

All API models use consistent decoding settings for reproducibility:

```
Temperature: 0.0       # Deterministic generation
Max Tokens:  8000      # Sufficient for most DAGs
Top-p:       Not set   # Defaults to 1.0
```

---

## Running Experiments

The main entry point is `prompt2dag.py`, a unified CLI orchestrator that coordinates all workflows.

### Available Workflows

| Workflow | Command | Purpose | Steps |
|----------|---------|---------|-------|
| **Direct** | `python prompt2dag.py direct` | Direct generation baseline | Step 0 + Step 4 |
| **Pipeline** | `python prompt2dag.py pipeline` | Full Prompt2DAG pipeline | Step 1–4 |
| **Reasoning** | `python prompt2dag.py reasoning` | Direct + reasoning models | Step 0 + Step 4 |
| **All** | `python prompt2dag.py all` | Comprehensive comparison | All three workflows |

### Example Commands

#### 1. Single Direct Prompting Run

Generate a DAG for one pipeline using direct prompting:

```bash
python prompt2dag.py direct \
  --input Pipeline_Description_Dataset/sample_etl.txt \
  --orchestrator airflow \
  --output-dir outputs/direct \
  --provider deepinfra \
  --model claude_sonnet
```

**Output Structure:**
```
outputs/direct/
├── direct_20240115_143022/
│   ├── generated_code/
│   │   └── generated_dag.py
│   ├── evaluation/
│   │   └── evaluation_summary_*.json
│   ├── metadata/
│   │   └── workflow_metadata.json
│   └── logs/
│       └── *.log
```

#### 2. Full Prompt2DAG Pipeline

Run the complete 4-step pipeline for multiple orchestrators:

```bash
python prompt2dag.py pipeline \
  --input Pipeline_Description_Dataset/sample_etl.txt \
  --orchestrators airflow dagster prefect \
  --output-dir outputs/prompt2dag \
  --provider deepinfra \
  --model claude_sonnet
```

**Output Structure:**
```
outputs/prompt2dag/
├── pipeline_20240115_143022/
│   ├── step1_analysis/
│   │   ├── pipeline_analysis_s1_*.json
│   │   ├── graph_validation/
│   │   │   └── dag_validation_*.json
│   │   └── semantic_eval/
│   │       └── semantic_eval_*.json
│   ├── step2_intermediate/
│   │   ├── airflow/
│   │   │   └── *_intermediate_*.yaml
│   │   ├── dagster/
│   │   └── prefect/
│   ├── step3_generated/
│   │   ├── airflow/
│   │   │   ├── template/
│   │   │   ├── llm/
│   │   │   └── hybrid/
│   │   ├── dagster/
│   │   └── prefect/
│   ├── step4_evaluation/
│   │   ├── airflow/
│   │   │   ├── template/
│   │   │   ├── llm/
│   │   │   └── hybrid/
│   │   └── [evaluation summaries per strategy]
│   ├── summary/
│   │   └── [consolidated results]
│   ├── metadata/
│   │   └── workflow_metadata.json
│   └── logs/
```

#### 3. Reasoning Model Baseline

Generate using reasoning models:

```bash
python prompt2dag.py reasoning \
  --input Pipeline_Description_Dataset/sample_etl.txt \
  --orchestrator airflow \
  --output-dir outputs/reasoning \
  --reasoning-config config_reasoning_llm.json \
  --provider deepinfra \
  --model deepseek_r1
```

#### 4. Comprehensive Comparison

Run all three methods in one orchestration (produces direct, pipeline, and reasoning outputs):

```bash
python prompt2dag.py all \
  --input Pipeline_Description_Dataset/sample_etl.txt \
  --orchestrators airflow dagster prefect \
  --output-dir outputs/comprehensive \
  --llm-config config_llm.json \
  --reasoning-config config_reasoning_llm.json \
  --provider deepinfra \
  --model claude_sonnet \
  --reasoning-provider deepinfra \
  --reasoning-model deepseek_r1
```

### Batch Processing (Full Benchmark)

To reproduce the paper's full evaluation, process all pipelines in the dataset:

```bash
#!/bin/bash
# batch_run.sh

DATASET_DIR="Pipeline_Description_Dataset"
OUTPUT_DIR="outputs/full_benchmark"
ORCHESTRATORS="airflow dagster prefect"

for pipeline_file in $DATASET_DIR/*.txt; do
  pipeline_name=$(basename "$pipeline_file" .txt)
  
  echo "Processing: $pipeline_name"
  
  # Run comprehensive workflow for each pipeline
  python prompt2dag.py all \
    --input "$pipeline_file" \
    --orchestrators $ORCHESTRATORS \
    --output-dir "$OUTPUT_DIR/$pipeline_name" \
    --llm-config config_llm.json \
    --reasoning-config config_reasoning_llm.json \
    --provider deepinfra \
    --model claude_sonnet \
    --reasoning-provider deepinfra \
    --reasoning-model deepseek_r1 \
    --log-level INFO
done

echo "Batch processing complete!"
```

Run the batch:

```bash
chmod +x batch_run.sh
./batch_run.sh
```

---

## Two Execution Scenarios for Step-1 (RQ2)

The paper evaluates Prompt2DAG under two scenarios to analyze token efficiency (RQ2):

### Scenario A: API Step-1 (Default)

Both Step-1 and Step-3 use API-served models.

- **When to use**: To reproduce the main RQ1 results and the "API Step-1" cost analysis in RQ2.
- **Configuration**: Use standard `config_llm.json` with DeepInfra endpoint.
- **Cost implication**: Both steps are billable.

```bash
python prompt2dag.py pipeline \
  --input Pipeline_Description_Dataset/sample.txt \
  --orchestrators airflow dagster prefect \
  --output-dir outputs/scenario_a \
  --llm-config config_llm.json \
  --provider deepinfra \
  --model claude_sonnet
```

### Scenario B: Local Step-1 (Optional)

Step-1 is executed using a local LLM inference backend; Step-3 remains API-based.

- **When to use**: To analyze cost reduction when Step-1 inference is self-hosted.
- **Setup**:
  1. Install a local inference stack (e.g., Ollama, LM Studio, or vLLM)
  2. Create a modified config file pointing to `localhost`
  3. Run the pipeline with the local config

#### Setting Up Local Inference with Ollama

```bash
# Install and run Ollama (https://ollama.ai)
brew install ollama  # macOS
# or download from https://ollama.ai for other OS

# Start Ollama service
ollama serve

# In another terminal, pull a model (e.g., Llama 2)
ollama pull llama2:70b

# Verify local endpoint
curl http://localhost:11434/api/generate -d '{"model":"llama2:70b","prompt":"test"}'
```

#### Local Step-1 Configuration (`config_llm_local.json`)

```json
{
  "model_settings": {
    "active_provider": "local",
    "local": {
      "active_model": "llama2_70b",
      "models": {
        "llama2_70b": {
          "model_name": "llama2:70b",
          "base_url": "http://localhost:11434/api/generate",
          "max_tokens": 8000,
          "temperature": 0
        }
      }
    }
  }
}
```

#### Running Scenario B

```bash
# Ensure Ollama is running (see above)

python prompt2dag.py pipeline \
  --input Pipeline_Description_Dataset/sample.txt \
  --orchestrators airflow dagster prefect \
  --output-dir outputs/scenario_b \
  --llm-config config_llm_local.json \
  --provider local \
  --model llama2_70b
```

**Key Difference for RQ2 Analysis**:
- Scenario A: Report billed cost for Step-1 + Step-3.
- Scenario B: Report billed cost for Step-3 only; local Step-1 compute cost excluded (see [RQ2 Analysis](#rq2-cost-and-token-efficiency)).

---

## Evaluation and Results

### Step-4 Evaluation Metrics

Each generated DAG is evaluated on four metrics:

#### 1. **Structural Acceptance Test (SAT)** – Binary Pass/Fail
- Does the generated code parse syntactically and load without runtime errors?
- Used to gate downstream analysis.

#### 2. **Platform Compliance Test (PCT)** – Binary Pass/Fail
- Does the code conform to orchestrator-specific conventions (e.g., task naming, dependency graph structure)?
- Checked via orcheestrator SDK validators.

#### 3. **Semantic Similarity** (\(S_{\text{code}}\)) – Continuous [0, 1]
- How similar is the generated code to a gold-standard reference (if available)?
- Computed as ROUGE-1 or BERTScore between generated and reference text.

#### 4. **Orchestration Readiness Test (ORT)** – Continuous [0, 1]
- Composite measure of code quality: test coverage, issue density, and structural complexity.
- Aggregates multiple static analysis findings.

### Evaluation Output Structure

For each generation (orchestrator/strategy), a JSON summary is produced:

**Example: `evaluation_summary_20240115_143022.json`**

```json
{
  "timestamp": "2024-01-15T14:30:22",
  "orchestrator": "airflow",
  "strategy": "llm",
  "sat_pass": true,
  "pct_pass": true,
  "semantic_similarity": 0.82,
  "ort_score": 0.75,
  "gating_status": "PASS",
  "issues": [
    {
      "severity": "WARNING",
      "type": "undefined_variable",
      "message": "Variable 'dag_id' may not be defined",
      "line": 15
    }
  ],
  "metrics": {
    "lines_of_code": 142,
    "cyclomatic_complexity": 3,
    "task_count": 5,
    "dependency_count": 6
  },
  "tokens": {
    "input_tokens": 512,
    "output_tokens": 384,
    "total_tokens": 896
  }
}
```

### Running Manual Evaluation

Evaluate a single generated DAG:

```bash
python scripts/Step_4_evaluate.py \
  --file outputs/direct/direct_20240115_143022/generated_code/generated_dag.py \
  --output-dir evaluation_results \
  --orchestrator airflow \
  --log-level INFO
```

Evaluate multiple DAGs (entire orchestrator):

```bash
python scripts/Step_4_evaluate.py \
  --files outputs/prompt2dag/pipeline_20240115_143022/step3_generated/airflow/*/*.py \
  --output-dir evaluation_results \
  --orchestrator airflow
```

---

## Reproducing Paper Results

This section explains how to regenerate the tables and figures reported in the paper.

### Overview of Analysis Pipeline

```
Raw Outputs (Step-4 JSON)
         ↓
aggregate_results.py  → all_sessions_*.csv
         ↓
compute_rq1.py        → RQ1 tables (quality & robustness)
compute_rq2.py        → RQ2 tables (cost & token efficiency)
```

### Step 1: Aggregate Run Results

After running the full benchmark (all pipelines, all methods), aggregate outputs into a CSV file:

```bash
python analysis/aggregate_results.py \
  --input-dir outputs/comprehensive \
  --output-csv results/all_sessions_cleaned.csv \
  --scenario api
```

**CSV Schema** (columns):

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | str | Unique run identifier |
| `pipeline_name` | str | Source pipeline description name |
| `method` | str | Generation method (direct, pipeline, reasoning) |
| `orchestrator` | str | Target orchestrator (airflow, dagster, prefect) |
| `strategy` | str | Code generation strategy (template, llm, hybrid) |
| `sat_pass` | bool | Structural Acceptance Test result |
| `pct_pass` | bool | Platform Compliance Test result |
| `semantic_similarity` | float | \(S_{\text{code}}\) ∈ [0, 1] |
| `ort_score` | float | Orchestration Readiness Test score |
| `gating_status` | str | PASS/FAIL (requires SAT and PCT) |
| `issue_count_critical` | int | Number of critical issues |
| `issue_count_warning` | int | Number of warnings |
| `step1_input_tokens` | int | Tokens for Step-1 input |
| `step1_output_tokens` | int | Tokens for Step-1 output |
| `step3_input_tokens` | int | Tokens for Step-3 input |
| `step3_output_tokens` | int | Tokens for Step-3 output |
| `model_name` | str | LLM used |
| `execution_time_seconds` | float | End-to-end execution time |

### Step 2: Compute RQ1 Results (Quality & Robustness)

Generate RQ1 analysis tables from aggregated CSV:

```bash
python analysis/compute_rq1.py \
  --input results/all_sessions_cleaned.csv \
  --output-dir results/rq1_analysis
```

**Output Tables Generated:**

1. **Method-Level Performance** (`rq1_method_summary.csv`)
   - Pass rates per method (Direct, Prompt2DAG, Reasoning)
   - Mean SAT, PCT, \(S_{\text{code}}\), ORT per method

2. **Orchestrator-Stratified Results** (`rq1_orchestrator_stratified.csv`)
   - Pass rates per (method, orchestrator) pair
   - Metrics broken down by platform

3. **Strategy-Specific Analysis** (`rq1_strategy_comparison.csv`)
   - Performance of template, LLM, and hybrid strategies within Prompt2DAG

4. **Pipeline-Level Correlations** (`rq1_pipeline_correlations.csv`)
   - Pairwise method comparisons per pipeline (for statistical testing)

5. **Step-1 Metric Analysis** (`rq1_step1_metrics.csv`)
   - Correlations between Step-1 analysis quality and final code quality

### Step 3: Compute RQ2 Results (Cost & Token Efficiency)

Generate RQ2 analysis for both scenarios:

```bash
# Scenario A: API Step-1
python analysis/compute_rq2.py \
  --input results/all_sessions_cleaned.csv \
  --scenario api \
  --pricing-model 2024_deepinfra \
  --output-dir results/rq2_analysis_api

# Scenario B: Local Step-1
python analysis/compute_rq2.py \
  --input results/all_sessions_local_llm_synthetic.csv \
  --scenario local \
  --pricing-model 2024_deepinfra \
  --output-dir results/rq2_analysis_local
```

**Output Tables Generated:**

1. **Scenario A: API Step-1** (`rq2_scenario_a_cost.csv`)
   - Billed cost per method (Step-1 + Step-3)
   - Cost per successful output (\$/pass)
   - Token breakdown (Step-1 vs Step-3 share)

2. **Scenario B: Local Step-1** (`rq2_scenario_b_cost.csv`)
   - Billed cost per method (Step-3 only)
   - Cost savings vs Scenario A
   - Local inference cost excluded

3. **Cross-Scenario Comparison** (`rq2_scenario_delta.csv`)
   - Cost differences (Scenario B – Scenario A)
   - Token efficiency gains

### Step 4: Generate Figures

Plot RQ1 and RQ2 results:

```bash
python analysis/plot_results.py \
  --rq1-dir results/rq1_analysis \
  --rq2-api-dir results/rq2_analysis_api \
  --rq2-local-dir results/rq2_analysis_local \
  --output-dir results/figures
```

**Expected Figures:**
- `fig_rq1_method_comparison.pdf` – RQ1 method-level pass rates
- `fig_rq1_orchestrator_stratified.pdf` – RQ1 orchestrator breakdown
- `fig_rq2_cost_per_method.pdf` – RQ2 cost analysis
- `fig_rq2_scenario_comparison.pdf` – Scenario A vs B

### Validating Reproducibility

To verify your results match the paper:

1. Check aggregate CSV row counts against reported pipeline counts (38 pipelines × methods × orchestrators).
2. Verify gating status distribution (% PASS/FAIL) against paper Table X.
3. Compare mean metrics (SAT, PCT, \(S_{\text{code}}\), ORT) within ±2% of reported values.
4. Cross-check token totals and cost calculations with the pricing snapshot used in the paper.

---

## Detailed Workflow Descriptions

### Workflow 1: Direct Prompting

**Steps:**
1. User provides a pipeline description and target orchestrator.
2. LLM generates DAG code in one call (Step-0).
3. Generated code is evaluated (Step-4).

**Configuration:**
- Single LLM call with full DAG context.
- No intermediate analysis or refinement.

**Output:**
- Single generated DAG per orchestrator.
- One evaluation summary per output.

### Workflow 2: Prompt2DAG Pipeline

**Steps:**

1. **Step-1: Pipeline Analysis** (Orchestrator-Agnostic)
   - Input: Natural language pipeline description.
   - LLM extracts task structure, dependencies, and execution semantics.
   - Output: Structured JSON analysis.
   - Evaluation:
     - DAG graph validation (structural correctness).
     - Semantic similarity to original description.

2. **Step-2: Intermediate YAML Generation** (Deterministic)
   - Input: Step-1 JSON analysis.
   - Mapper translates abstract analysis to orchestrator-specific intermediate representation.
   - Output: YAML file with orchestrator-specific task and dependency syntax.

3. **Step-3: Code Generation** (Three Strategies)
   - **Template Strategy**: Jinja2 templates fill in YAML-specified tasks and dependencies.
   - **LLM Strategy**: LLM generates full code from intermediate YAML.
   - **Hybrid Strategy**: Templates handle common patterns; LLM refines edge cases.
   - Output: Three Python DAG files per orchestrator.

4. **Step-4: Evaluation**
   - All generated DAGs (3 strategies × 3 orchestrators = 9 variants per pipeline) are evaluated.
   - Metrics: SAT, PCT, \(S_{\text{code}}\), ORT.

### Workflow 3: Direct with Reasoning Models

**Steps:**
1. User provides pipeline description, target orchestrator, and reasoning model.
2. Reasoning model generates DAG code with extended thinking/chain-of-thought (Step-0).
3. Generated code is evaluated (Step-4).

**Rationale:**
- Assess whether reasoning models reduce the need for multi-step refinement.
- Compare direct reasoning with direct standard generation.

**Output:**
- Single generated DAG per (orchestrator, reasoning model).
- One evaluation summary per output.

---

## Understanding Token Accounting (RQ2)

Token costs are tracked to support RQ2 analysis. Here's how tokens are recorded:

### API Call Tracking

Each LLM API call logs:
- `input_tokens`: Tokens in the prompt.
- `output_tokens`: Tokens generated.
- `total_tokens`: Sum of input and output.

### Scenario A: API Step-1

| Step | Component | Tokens Tracked | Billable |
|------|-----------|-----------------|----------|
| Step-1 | Pipeline analysis | prompt + completion | ✓ Yes |
| Step-2 | Intermediate YAML | N/A (deterministic) | ✗ No |
| Step-3 (Template) | None | N/A | ✗ No |
| Step-3 (LLM) | Code generation | prompt + completion | ✓ Yes |
| Step-3 (Hybrid) | LLM refinement | prompt + completion | ✓ Yes |

**Total Cost (Scenario A):**
```
Cost = (Step-1 tokens) × price_per_token 
      + (Step-3 LLM tokens) × price_per_token
      + (Step-3 Hybrid tokens) × price_per_token
```

(Template strategy has zero Step-3 cost.)

### Scenario B: Local Step-1

| Step | Component | Tokens Tracked | Billable |
|------|-----------|-----------------|----------|
| Step-1 | Pipeline analysis (local) | None | ✗ No |
| Step-2 | Intermediate YAML | N/A (deterministic) | ✗ No |
| Step-3 (Template) | None | N/A | ✗ No |
| Step-3 (LLM) | Code generation | prompt + completion | ✓ Yes |
| Step-3 (Hybrid) | LLM refinement | prompt + completion | ✓ Yes |

**Total Cost (Scenario B):**
```
Cost = (Step-3 LLM tokens) × price_per_token
     + (Step-3 Hybrid tokens) × price_per_token
```

(Local Step-1 compute excluded; no billed tokens.)

### Cost Calculation

Using DeepInfra 2024 pricing snapshot (from paper appendix):

```python
# Example pricing (check paper for actual rates)
PRICING = {
    "claude_sonnet": {"input": 0.003, "output": 0.015},      # $/1K tokens
    "qwen_72b": {"input": 0.0002, "output": 0.0004},
    "deepseek_r1": {"input": 0.0006, "output": 0.0016}
}

def calculate_cost(tokens, model, is_output=False):
    """Calculate cost for given token count."""
    token_type = "output" if is_output else "input"
    price_per_1k = PRICING[model][token_type]
    return (tokens / 1000) * price_per_1k

# Example: 1000 input tokens + 500 output tokens with Claude Sonnet
input_cost = calculate_cost(1000, "claude_sonnet", is_output=False)    # $0.003
output_cost = calculate_cost(500, "claude_sonnet", is_output=True)     # $0.0075
total_cost = input_cost + output_cost                                   # $0.0105
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: API Key Not Recognized

**Symptom:**
```
Error: DEEPINFRA_API_KEY not found
```

**Solution:**
```bash
# Set environment variable
export DEEPINFRA_API_KEY="your_actual_key"

# Verify
echo $DEEPINFRA_API_KEY

# Or update config file directly (not recommended for production)
sed -i 's/${DEEPINFRA_API_KEY}/your_key/g' config_llm.json
```

#### Issue 2: Orchestrator Not Installed

**Symptom:**
```
ModuleNotFoundError: No module named 'airflow'
```

**Solution:**
```bash
# Install missing orchestrator
pip install apache-airflow==2.8.0

# Verify
python -c "import airflow; print(airflow.__version__)"
```

#### Issue 3: Step-4 Evaluation Fails

**Symptom:**
```
Error: SAT evaluation failed - syntax error in generated DAG
```

**Solution:**
1. Check the generated DAG file for Python syntax errors.
2. Verify the LLM model is not rate-limited or returning truncated output.
3. Check logs: `ls -lah outputs/*/logs/`.
4. Rerun with increased `max_tokens` if Step-3 is truncating output.

#### Issue 4: Semantic Evaluation Dependencies Missing

**Symptom:**
```
ImportError: No module named 'bert_score'
```

**Solution:**
```bash
# Install semantic evaluation dependencies
pip install bert-score rouge-score torch

# For GPU acceleration (optional)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

#### Issue 5: Local Inference (Ollama) Connection Failed

**Symptom:**
```
ConnectionError: Failed to connect to http://localhost:11434
```

**Solution:**
```bash
# Ensure Ollama is running
ollama serve

# In another terminal, check if service is responding
curl http://localhost:11434/api/tags

# Pull a model if not already available
ollama pull llama2:70b
```

#### Issue 6: Inconsistent Results Between Runs

**Symptom:**
```
Different outputs generated on re-run (SAT/PCT changes)
```

**Potential Causes:**
- Temperature is not set to 0 in config (should be `"temperature": 0`).
- Different orchestrator versions installed than those used in paper.
- Dependency version drift (e.g., different Jinja2 behavior).

**Solution:**
```bash
# Fix temperature in config
grep -r "temperature" config_*.json | grep -v ": 0"

# Pin dependency versions
pip install -r requirements_exact.txt
```

#### Issue 7: Token Counts Seem Incorrect

**Symptom:**
```
Token count mismatch between runs
```

**Root Causes:**
- Different tokenizer versions (tiktoken).
- LLM provider changes token accounting methodology.
- Local inference doesn't report token counts.

**Solution:**
```bash
# Verify tiktoken version
pip show tiktoken

# For reproducibility, pin version in requirements
pip install tiktoken==0.5.1
```

---

## Reproducibility Checklist

Use this checklist to ensure reproducibility of your experiments:

- [ ] Python 3.10+ installed
- [ ] Virtual environment created and activated
- [ ] All core and orchestrator dependencies installed (see [Environment Setup](#environment-setup))
- [ ] API key set via environment variable: `echo $DEEPINFRA_API_KEY`
- [ ] Configuration files updated with correct model names and endpoints
- [ ] Test run completed successfully:
  ```bash
  python prompt2dag.py direct \
    --input Pipeline_Description_Dataset/sample_pipeline.txt \
    --orchestrator airflow \
    --output-dir outputs/test
  ```
- [ ] Evaluation outputs verified (SAT/PCT/ORT scores present)
- [ ] Full benchmark dataset available in `Pipeline_Description_Dataset/`
- [ ] Batch processing script ready for full run
- [ ] Aggregation script configured for target scenario (A or B)
- [ ] RQ1 and RQ2 analysis scripts executable

---

## Advanced: Customization and Extension

### Adding a New Orchestrator

To support a new orchestrator platform (e.g., Argo Workflows):

1. **Create mapper**: `scripts/orchestrator_mappers/argo_mapper.py`
   - Inherit from `BaseMapper`
   - Implement `to_code()` method

2. **Create code generator**: Update `scripts/code_generators/*.py` to handle Argo syntax

3. **Create templates**: Add Jinja2 templates in `scripts/templates/argo/`

4. **Create validator**: `scripts/validators/argo_validator.py`

5. **Create compliance checker**: `scripts/evaluators/platform_compliance/argo_compliance.py`

6. **Update CLI**: Add orchestrator to `ORCHESTRATORS` list in `prompt2dag.py`

### Adding a New Evaluation Metric

To add a custom metric (beyond SAT, PCT, \(S_{\text{code}}\), ORT):

1. **Extend evaluator**: Modify `scripts/evaluators/unified_evaluator.py`

2. **Implement compute method**:
   ```python
   def compute_custom_metric(self, dag_code: str) -> float:
       """Custom metric computation."""
       # Your metric logic here
       return score  # ∈ [0, 1]
   ```

3. **Update Step-4**: Add metric to evaluation summary JSON schema

4. **Update analysis**: Include metric in RQ1/RQ2 aggregation scripts

---

## Citation

If you use this work in your research, please cite the paper:

```bibtex
@article{alidu5601120prompt2dag,
  title={Prompt2DAG: A Modular LLM-Prompting Methodology for Data Enrichment Pipeline Generation},
  author={Alidu, Abubakari and Ciavotta, Michele and De Paoli, Flavio},
  journal={Available at SSRN 5601120}
}
```

---

## Support and Issues

For questions, bugs, or feature requests:

1. **Check existing issues**: Search the repository for similar problems.
2. **File a new issue**: Include:
   - Exact command run
   - Python and orchestrator versions
   - Full error message and logs
   - Platform (macOS/Linux/Windows)
3. **Contact us**: Email [a.alidu@campus.unimib.it](mailto:a.alidu@campus.unimib.it) for discussions and support.

---

## License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

This work has been partially funded by the European Innovation Actions *enRichMyData* (HE 101070284) and *DataPACT* (HE 101189771), and the Italian PRIN, a Next Generation EU project, *Discount Quality for Responsible Data Science: Human-in-the-Loop for Quality Data* (202248FWFS).

---

**Last Updated**: 2025  
**Status**: Production-ready for reproducibility  
**Maintained By**: DATA AI Team at University of Milano Bicocca