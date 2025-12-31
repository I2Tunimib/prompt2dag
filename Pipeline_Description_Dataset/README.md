# Workflow Pipeline Dataset

## Overview

This repository contains a curated dataset of **38 workflow pipeline descriptions** designed to evaluate automated workflow generation systems, particularly for Apache Airflow. The dataset provides comprehensive coverage of realistic workflow scenarios spanning multiple business domains, control-flow topologies, and service-integration patterns.

## Dataset Composition

### Pipeline Sources

The dataset comprises three distinct categories of workflows:

1. **Real-world workflows** (derived from GitHub repositories)
   - Mined from public Apache Airflow repositories
   - Stratified by repository popularity
   - Extracted from typical DAG directories (`dags/`, `pipelines/`, `workflows/`, `etl/`)
   - Distilled into structured natural-language descriptions while preserving original code

2. **Synthetic workflows** (designed for topology coverage)
   - Created to cover underrepresented topology patterns
   - Designed for specific structural patterns (linear, fan-out/fan-in, branch-merge, sensor-gated)
   - Accompanied by reference implementations in Airflow 2.x

3. **Enrichment scenarios** (SemT-based tabular enrichment)
   - 5 semantic table enrichment workflows
   - Specified as high-level natural-language descriptions
   - Focus on data augmentation through external service integration

### Domain Coverage

The dataset spans **30 distinct domain categories** across major business areas:

| Business Domain | Count | Representative Categories |
|----------------|-------|--------------------------|
| **Data Engineering** | 11 | Data Warehousing (4), ETL/ELT (2), Data Quality (4), Data Transformation (1) |
| **Healthcare** | 5 | Primary Care Data Processing, Healthcare Claims Processing, Genomic Data Processing, Healthcare Infrastructure Analytics |
| **Environmental Monitoring** | 4 | Air Quality Integration/Alerting, Environmental Risk Analysis, Climate Data Processing |
| **Financial Services** | 5 | Sales Analytics (2), Fraud Detection, Regulatory Compliance, Portfolio Management |
| **E-commerce** | 3 | Customer Analytics, E-commerce Analytics, Customer Management |
| **Supply Chain & Retail** | 3 | Inventory Management, Supply Chain Operations |
| **System Operations** | 4 | Infrastructure Maintenance, Database Operations, Data Replication, Data Processing Orchestration |
| **Other** | 3 | Marketing Analytics, Procurement, Content Moderation |

## Structural Characteristics

### Topology Patterns

The dataset exhibits four main control-flow families:

```
┌─────────────────────┬───────┬─────────────────────────────────────┐
│ Topology Pattern    │ Count │ Description                         │
├─────────────────────┼───────┼─────────────────────────────────────┤
│ Linear Sequential   │  15   │ Tasks executed strictly in order    │
│ Fan-out/Fan-in      │  11   │ Parallel branches with convergence  │
│ Branch-merge        │   7   │ Conditional routing with merging    │
│ Sensor-gated        │   5   │ Execution gated by external signals │
└─────────────────────┴───────┴─────────────────────────────────────┘
```

### Parallelization Levels

```
- None (Sequential):     23 pipelines (60.5%)
- Task-level Parallel:   12 pipelines (31.6%)
- Mixed Parallelism:      3 pipelines (7.9%)
```

### Processing Patterns

All pipelines implement batch-style processing with two main classifications:

- **Batch workflows**: 21 pipelines (55.3%)
- **Extract-Transform-Load (ETL)**: 17 pipelines (44.7%)

### Service Integration Patterns

```
┌──────────────────────────┬───────┬──────────────────────────────────────┐
│ Integration Pattern      │ Count │ Description                          │
├──────────────────────────┼───────┼──────────────────────────────────────┤
│ Sequential API Calls     │  13   │ Serial API invocations               │
│ Database-centric         │   9   │ Primary focus on database operations │
│ Orchestrated Services    │   6   │ Multi-service coordination           │
│ File Processing          │   5   │ File-based data transformation       │
│ Parallel API Fan-out     │   4   │ Concurrent API calls                 │
│ API Enrichment           │   1   │ Tight API-database coupling          │
└──────────────────────────┴───────┴──────────────────────────────────────┘
```

## Complexity Distribution

### Standard Complexity Scoring

Each pipeline is assigned a structural complexity score `s(p) ∈ {1,2,...,8}` based on:

**Base Score (Task Count)**
- 1: ≤2 tasks
- 2: 3-4 tasks
- 3: 5-8 tasks
- 4: ≥9 tasks

**Additional Complexity Factors** (+1 point each):
- Parallelism or fan-out/fan-in topology
- Presence of sensors (file, SQL, external DAG, FTP)
- Conditional branching with multiple execution paths
- Complex integration patterns (orchestrated services, parallel API fan-out)
- Multiple distinct external service integrations

### Complexity Bands

```
Simple (1-2):      7 pipelines (18.4%)
Moderate (3-4):   14 pipelines (36.8%)
Complex (5-6):    11 pipelines (28.9%)
Advanced (7-8):    6 pipelines (15.8%)

Average complexity score: 4.3
```

## Technical Stack

### Databases

The most commonly integrated databases:

```
PostgreSQL:               7 instances
MongoDB:                  4 instances
Elasticsearch:            1 instance
Hive:                     1 instance
Snowflake:                1 instance
Airflow MetaStore:        1 instance
```

### External APIs & Services

**Geographic & Weather Services**
- HERE API (geocoding): 3 instances
- OpenMeteo API (weather data): 2 instances
- Geoapify API, WorldPop API

**Data Platform Services**
- Google Cloud Dataform API
- Databricks REST API
- Apache Spark on Kubernetes

**Government & Public Data**
- data.gouv.fr APIs (death records, power plants)
- Mahidol University AQI API

**Healthcare & Scientific**
- Ensembl FTP Server (genomic data)
- GitHub Releases API (ontology data)

**Business Applications**
- Magento GraphQL API
- SAP System Integration
- Slack Webhook API

**Other Services**
- AirVisual API (air quality)
- Wikidata API (entity reconciliation)
- FTP/SFTP servers
- SMTP email services

### Scheduling Patterns

```
Daily/Cron:          17 pipelines
Manual/On-demand:    11 pipelines
Batch:                8 pipelines
Dataset-triggered:    1 pipeline
Dynamic (variable):   1 pipeline
```

## Dataset Statistics

| Metric | Value |
|--------|-------|
| Total pipelines | 38 |
| Average tasks per pipeline | 5.7 |
| Task range | 1-23 tasks |
| Maximum parallel width | 18 (parallel API calls) |
| Pipelines with sensors | 9 (23.7%) |
| Pipelines with branching | 7 (18.4%) |
| Pipelines with fan-out | 18 (47.4%) |
| Pipelines with fan-in | 14 (36.8%) |

## Data Format

Each pipeline in the dataset includes:

### 1. Textual Description (JSON)
```json
{
  "pipeline_name": "string",
  "business_domain": "string",
  "domain_category": "string",
  "primary_objective": "string",
  "enrichment_objective": "string",
  "topology": {
    "pattern": "string",
    "description": "string",
    "has_sensors": boolean,
    "has_branches": boolean,
    "has_fan_out": boolean,
    "has_fan_in": boolean,
    "parallelization_level": "string",
    "max_parallel_width": integer,
    "branch_depth": integer
  },
  "processing": {...},
  "external_services": {...},
  "infrastructure": {...},
  "scheduling": {...},
  "complexity": {...},
  "failure_handling": {...},
  "unique_features": [...]
}
```

### 2. Reference Implementation (when available)
- Original DAG Python code
- Synthetic reference DAG (for designed scenarios)
- Not exposed during automated generation evaluation

### 3. Structural Analysis
- Complexity scores (model-based and standard)
- Topology classification
- Service integration patterns
- Task dependency graphs

## Use Cases

This dataset is designed to support:

1. **Automated Workflow Generation Research**
   - Evaluation of natural language to workflow translation systems
   - Benchmark for LLM-based code generation
   - Testing prompt engineering approaches

2. **Workflow Pattern Analysis**
   - Control-flow topology studies
   - Service integration pattern analysis
   - Complexity assessment methodologies

3. **Data Pipeline Education**
   - Teaching workflow orchestration concepts
   - Demonstrating real-world integration patterns
   - Illustrating topology design principles

4. **System Benchmarking**
   - Evaluating workflow management platforms
   - Testing workflow generation tools
   - Assessing structural correctness metrics


## Dataset Quality Assurance

### Validation Criteria

Each pipeline description has been validated for:

✓ **Structural completeness**: All topology elements clearly specified  
✓ **Service clarity**: External dependencies explicitly documented  
✓ **Scheduling accuracy**: Execution patterns properly defined  
✓ **Domain relevance**: Realistic business context and objectives  
✓ **Technical feasibility**: Implementable with specified technologies  

### Annotation Methodology

1. **Real-world DAGs**: Manual code inspection and documentation extraction
2. **Synthetic DAGs**: Design-first approach with explicit topology constraints
3. **Enrichment scenarios**: Domain expert specification with validation

## Limitations

- **Temporal scope**: Dataset reflects workflow patterns as of 2024
- **Platform focus**: Primarily Apache Airflow 2.x constructs
- **Domain coverage**: Skewed toward data engineering and healthcare
- **Complexity ceiling**: Maximum complexity score of 8 may not capture all nuances
- **Single execution model**: Batch processing only (no streaming workflows)

## Citation

If you use this dataset in your research, please cite:

```bibtex
@dataset{workflow_pipeline_dataset_2024,
  title={Curated Workflow Pipeline Dataset for Automated DAG Generation},
  author={[Authors]},
  year={2024},
  url={https://github.com/I2Tunimib/prompt2dag},
  note={38 pipelines spanning 30 domain categories}
}
```

## License



## Maintenance & Updates

**Version**: 1.0  
**Last Updated**: December 2025 
**Maintainers**: a.alidu@campus.unimib.it

For questions, issues, or contributions, please open an issue in the repository or contact the maintainers directly.

## Acknowledgments

This dataset includes workflows derived from open-source DAGs. We acknowledge the original authors and contributors of these implementations. All real-world workflows have been anonymized and restructured as textual descriptions to respect intellectual property while enabling research use.

---

**Repository**: https://github.com/I2Tunimib/prompt2dag  