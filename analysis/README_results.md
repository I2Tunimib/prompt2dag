# Prompt2DAG Results Dataset — README

## 1 Overview

This repository provides the experimental results used in:

**Prompt2DAG: A Modular LLM-Prompting Methodology for Data Pipeline Generation**  
Alidu, Abubakari; Ciavotta, Michele; De Paoli, Flavio

The primary dataset (`all_sessions_cleaned.csv`) contains **8,742 run-level records** covering:

- **38 pipeline descriptions**
- **3 orchestrators**: Airflow, Dagster, Prefect
- **5 generation methodologies**:
  1) Direct (Non-Reasoning)  
  2) Prompt2DAG (Template)  
  3) Prompt2DAG (LLM)  
  4) Prompt2DAG (Hybrid)  
  5) Direct (Reasoning)
- **LLM endpoints via DeepInfra**:
  - Standard models for Direct (Non-Reasoning) and Prompt2DAG
  - Reasoning models for Direct (Reasoning) only

Each row corresponds to a single generation run and includes outcome metrics, issue counts, and (for Prompt2DAG runs) Step‑1 semantic + abstract-graph quality metrics and token accounting.

---

## 2 Dataset files and scenarios

### 2.1 Primary dataset (used for main RQ1 + RQ2 results)

- **File**: `all_sessions_cleaned.csv`  
- **Role in paper**: main dataset used for RQ1 and Scenario A in RQ2  
- **Interpretation**: Step‑1 and Step‑3 were executed using API-accessible models (DeepInfra), and Step‑1 token usage is logged.

### 2.2 Local Step‑1 dataset (used for RQ2 Scenario B and appendix comparisons)

- **File**: `all_sessions_local_llm_synthetic.csv`  
- **Role in paper**: Scenario B for RQ2 (Local Step‑1 billing scenario), plus appendix comparison tables.

**Important note on interpretation:**  
The Local Step‑1 dataset represents a “local Step‑1” configuration where Step‑1 is executed locally and therefore is treated as **not billed at DeepInfra prices** in the RQ2 cost accounting scripts. If the file is produced via a synthetic adjustment script, it should be treated as a derived scenario dataset. If it is produced from actual logged local Step‑1 runs, it should be treated as empirical. (The filename includes “synthetic”; if you want to avoid reviewer confusion, consider renaming to either `..._local_llm_measured.csv` or `..._local_llm_projection.csv` based on provenance.)

---

## 3 Method taxonomy (how to interpret `Workflow`, `Strategy`, and `Method`)

Rows encode method identity using:

- `Workflow` ∈ {`Direct`, `Prompt2DAG`, `Reasoning`}
- `Strategy` distinguishes Prompt2DAG variants (e.g., template/llm/hybrid)
- `Method` is the canonical label used in the paper:
  - `Direct (Non-Reasoning)`
  - `Prompt2DAG (Template)`
  - `Prompt2DAG (LLM)`
  - `Prompt2DAG (Hybrid)`
  - `Direct (Reasoning)`

---

## 4 Core evaluation metrics (paper-aligned definitions)

This dataset supports three key score families:

### 4.1 SAT — Static Analysis Test (`Static_Score`)

- Column: `Static_Score` in \([0,10]\)
- Meaning: static Python code quality measured without executing the workflow.
- Dimension breakdown columns:
  - `StaticDim_correctness`
  - `StaticDim_code_quality`
  - `StaticDim_best_practices`
  - `StaticDim_maintainability`
  - `StaticDim_robustness`

SAT is an **unweighted mean** of these five dimensions in the paper.

### 4.2 PCT — Platform Conformance Test (`Compliance_Score`)

- Column: `Compliance_Score` in \([0,10]\)
- Meaning: orchestrator-aware conformance and loadability under the target platform.
- Dimension breakdown columns:
  - `ComplianceDim_loadability`
  - `ComplianceDim_structure_validity`
  - `ComplianceDim_configuration_validity`
  - `ComplianceDim_task_validity`
  - `ComplianceDim_executability`

PCT is a **gate-based** score in the paper:
- If the platform gate fails (not loadable / missing required structure), `Compliance_Score` is set to **0**.

### 4.3 Unified code-level score (`Combined_Score`)

- Column: `Combined_Score` in \([0,10]\)
- Meaning: unified code-level score \(S_{\mathrm{code}}\) combining SAT and PCT.
- In the paper:  
  \[
  S_{\mathrm{code}} = 0.5\cdot \mathrm{SAT} + 0.5\cdot \mathrm{PCT}
  \]
  and is set to **0** when the platform gate fails (and, for Prompt2DAG, also when intermediate validation fails).

### 4.4 “Passed” and platform gate

- Columns: `Passed`, `Gates_Passed`
- In the paper and analysis: **Pass rate is derived from the platform gate underlying PCT.**  
  Concretely, a run counts as successful iff it passes the platform gate (loadable and minimum orchestrator structure satisfied).

**Recommended interpretation:**
- Treat `Passed` as the main indicator used in RQ1/RQ2 pass-rate calculations.
- Treat `Gates_Passed` as the underlying gate indicator (often identical in practice).

### 4.5 ORT — issue-adjusted robustness (computed from issues; not part of SAT/PCT)

ORT is reported using:

- `ORT_Score_raw`
- `ORT_Score_capped` (clipped to \([0,10]\))
- `ORT_Score_scaled` (min–max scaled to \([0,10]\))

ORT is computed from:
- `Base_Score` (typically equals `Combined_Score` when Passed=1, otherwise 0)
- `Penalty` (severity-weighted issue counts)

Issue counts:
- `Critical_Issues`
- `Major_Issues`
- `Minor_Issues`
- `Total_Issues`

ORT is the **only** metric where severity-weighted penalisation is applied; SAT and PCT are reported without penalty subtraction.

---

## 5 Prompt2DAG Step‑1 metrics (only for `Workflow=Prompt2DAG`)

Step‑1 produces:
1) a natural language analysis (semantic fidelity metrics), and  
2) an abstract pipeline DAG (graph structure metrics).

### 5.1 Step‑1 semantic metrics

Columns include:
- `S1_Sem_BERT_f1` (raw, \([0,1]\))
- `S1_Sem_BERT_norm` (scaled, typically \([0,10]\))
- `S1_Sem_ROUGE1_f1` (raw, \([0,1]\))
- `S1_Sem_ROUGE1_norm` (scaled, typically \([0,10]\))
- Key term overlap features:
  - `S1_Sem_KeyTerm_rate`
  - `S1_Sem_KeyTerm_preserved`
  - `S1_Sem_KeyTerm_missing`
  - `S1_Sem_KeyTerm_total`
- Token overlap:
  - `S1_Sem_tok_overlap_ratio`
- Summary grade:
  - `S1_Sem_final_grade`

### 5.2 Step‑1 abstract graph (DST) metrics

Columns include:
- `S1_Graph_overall_score` (scaled, \([0,10]\))
- `S1_Graph_status` ∈ {PASS, WARNING, FAIL}
- `S1_Graph_total_issues` (integer count)
- Graph structure statistics:
  - `S1_Graph_entry_point_count`
  - `S1_Graph_terminal_node_count`
  - `S1_Graph_total_components`
  - `S1_Graph_total_nodes_in_flow`
  - `S1_Graph_total_edges`
  - `S1_Graph_max_pipeline_depth`

Dimension sub-scores and criteria counts:
- `S1_Graph_*_criteria_passed`, `S1_Graph_*_criteria_total`
- `S1_Graph_*_score` (note: many sub-scores are percentage-like values in \([0,100]\), while `S1_Graph_overall_score` is in \([0,10]\)).

---

## 6 Token accounting

### 6.1 Step‑3 (code generation) tokens

- `Input_Tokens`
- `Output_Tokens`
- `Total_Tokens` (Step‑3 total)

### 6.2 Step‑1 (Prompt2DAG only) tokens

Step‑1 tokens are decomposed by sub-task:

- `S1_Tokens_*_input`
- `S1_Tokens_*_output`
- `S1_Tokens_total_input`
- `S1_Tokens_total_output`
- `S1_Tokens_total`

For Direct methods, Step‑1 token columns are typically 0 / empty.

---

## 7 LLM fields and identifiers

- `Std_LLM`: model used for Direct (Non-Reasoning) and Prompt2DAG runs
- `Reasoning_LLM`: model used for Direct (Reasoning) runs
- `Model_ID`: run configuration identifier (often encodes std + reasoning model selection)

All LLM access in the experiments uses DeepInfra endpoints (OpenAI-compatible interface), with temperature=0.0. See the paper’s model table and RQ2 pricing table for details.

---

## 8 Known caveats / reproducibility notes

1) **Do not interpret “\$/pass” or “free” as total cost of ownership.**  
   RQ2 cost uses token billing based on DeepInfra list prices and does not include local inference hardware/ops cost in the Local Step‑1 scenario.

2) **Template runs have Step‑3 tokens set to zero by design.**  
   Prompt2DAG (Template) uses deterministic template code generation rather than an LLM call for Step‑3.

3) **Outlier model behaviour.**  
   Some configurations (e.g., Qwen3‑14B under certain settings) can behave as near-degenerate under the harness; this is captured directly by Pass% and is documented in the paper’s LLM breakdown tables.

---

## 9 Quick-start: reproducing core paper tables

### 9.1 RQ1: method-level performance (pass rate, SAT/PCT/Combined, ORT)

```python
import pandas as pd

df = pd.read_csv("all_sessions_cleaned.csv")

rq1 = df.groupby("Method").agg(
    N=("Method", "size"),
    PassRate=("Passed", "mean"),
    SAT=("Static_Score", "mean"),
    PCT=("Compliance_Score", "mean"),
    S_code=("Combined_Score", "mean"),
    ORT=("ORT_Score_scaled", "mean"),
).sort_values("PassRate", ascending=False)

print((rq1.assign(PassRate=lambda x: 100*x["PassRate"]).round(3)))
```

### 9.2 RQ2: token usage and cost (computed from tokens + pricing)

Costs are typically computed by the provided analysis scripts (pricing snapshot is in the paper). Minimal token check:

```python
p2d = df[df["Workflow"] == "Prompt2DAG"]
print("Mean Step1 tokens:", (p2d["S1_Tokens_total_input"] + p2d["S1_Tokens_total_output"]).mean())
print("Mean Step3 tokens:", (p2d["Input_Tokens"] + p2d["Output_Tokens"]).mean())
```

---

## 10 Citation

```bibtex
@article{alidu5601120prompt2dag,
  title={Prompt2DAG: A Modular LLM-Prompting Methodology for Data Enrichment Pipeline Generation},
  author={Alidu, Abubakari and Ciavotta, Michele and De Paoli, Flavio},
  journal={Available at SSRN 5601120},
  year={2024}
}
```

---

## 11 Contact

Email: a.alidu@campus.unimib.it