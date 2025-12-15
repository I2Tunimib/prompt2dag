#!/usr/bin/env python3

import json
import logging
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional

# Add parent directory to Python path for importing utils package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Optional dependency checks (for convenience when running standalone)
def check_dependencies():
    """Check for required dependencies and provide helpful error messages."""
    missing_dependencies = []
    try:
        import openai  # noqa: F401
    except ImportError:
        missing_dependencies.append("openai")
    try:
        import anthropic  # noqa: F401
    except ImportError:
        missing_dependencies.append("anthropic")
    if missing_dependencies:
        print(f"Error: The following required dependencies are missing: {', '.join(missing_dependencies)}")
        print("Please install them with: pip install " + " ".join(missing_dependencies))
        return False
    return True

# Try to import utils (LLMProvider + config loader)
try:
    from utils.config_loader import load_config, validate_api_keys
    from utils.llm_provider import LLMProvider
except ImportError:
    # Fallback minimal stubs (for ad-hoc testing only)
    print("Unable to import utility modules from utils. Using fallback stubs for testing.")
    def load_config(config_path=None):
        if not config_path or not os.path.exists(config_path):
            return {}
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
            return {}
    def validate_api_keys(config):
        return True
    class LLMProvider:  # minimal stand-in
        def __init__(self, config, model_key=None):
            self._config = config
            self._model = model_key or "unknown"
        def generate_completion(self, system_prompt, user_prompt):
            # This stub is for local fallback only
            content = "# Dummy code output; real LLMProvider required.\nprint('Hello')\n"
            tokens = {'input_tokens': 0, 'output_tokens': 0}
            return content, tokens
        def get_model_info(self):
            return {'provider': 'stub', 'model_name': self._model}

ORCHESTRATOR_CHOICES = ['airflow', 'prefect', 'dagster']

def _orchestrator_guidance(orchestrator: str, docker_based: bool) -> Tuple[str, str]:
    """
    Build orchestrator-specific guidance for system and user prompts.
    Returns: (system_addon, user_addon)
    """
    orchestrator = orchestrator.lower().strip()
    docker_hint_common = ""
    if docker_based:
        if orchestrator == 'airflow':
            docker_hint_common = (
                "- If tasks are containerized, prefer DockerOperator (pass image, environment, volumes, network if applicable).\n"
            )
        elif orchestrator == 'prefect':
            docker_hint_common = (
                "- If tasks are containerized, you may use prefect-docker (DockerContainer) or shell-based invocation. "
                "Show a clear pattern to run container images with env vars.\n"
            )
        elif orchestrator == 'dagster':
            docker_hint_common = (
                "- If tasks are containerized, you may reference a docker-based resource or shell commands for container "
                "invocation. Provide a clean pattern for configuring image/env if appropriate.\n"
            )
    if orchestrator == 'airflow':
        system_addon = (
            "You are an expert Apache Airflow developer. Generate complete, executable Apache Airflow DAG Python code.\n"
            "Follow best practices: PEP 8, proper imports, DAG context manager, clear task naming, explicit dependencies, "
            "and minimal but meaningful default_args.\n"
        )
        user_addon = (
            "CONSTRAINTS FOR AIRFLOW:\n"
            "- Use Airflow 2.x style imports (e.g., from airflow import DAG; from airflow.operators.python import PythonOperator).\n"
            "- Define a DAG with schedule_interval (or None), catchup, default_args, and clear task dependencies using >>.\n"
            "- If sensors appear in the description, demonstrate proper use (e.g., FileSensor, ExternalTaskSensor, SqlSensor).\n"
            "- If branching is required, use BranchPythonOperator or suitable equivalent.\n"
            f"{docker_hint_common}"
        )
    elif orchestrator == 'prefect':
        system_addon = (
            "You are an expert Prefect developer. Generate complete, executable Prefect 2.x Python code.\n"
            "Follow best practices: PEP 8, from prefect import flow, task; use @task for units of work and @flow for orchestration.\n"
        )
        user_addon = (
            "CONSTRAINTS FOR PREFECT 2.x:\n"
            "- Use Prefect 2.x style APIs: from prefect import flow, task.\n"
            "- Define @task functions for each step, and a single @flow that orchestrates them in the described order.\n"
            "- For parallel steps, use .submit() to run tasks concurrently; for sequential steps, call tasks normally.\n"
            "- Add an if __name__ == '__main__': guard that calls the flow for local execution.\n"
            "- If schedules are mentioned, include a brief comment indicating deployment/schedule configuration (optional).\n"
            f"{docker_hint_common}"
        )
    elif orchestrator == 'dagster':
        system_addon = (
            "You are an expert Dagster developer. Generate complete, executable Dagster Python code.\n"
            "Follow best practices: PEP 8, from dagster import op, job (or graph) to define computation units and orchestration.\n"
        )
        user_addon = (
            "CONSTRAINTS FOR DAGSTER:\n"
            "- Use Dagster ops (@op) to represent each step and a job (@job) that sets dependencies.\n"
            "- If you need resources/config, provide simplified resource or config examples as comments or minimal stubs.\n"
            "- If branching/conditional paths appear, model them using standard Python branching within ops or via fan-in patterns.\n"
            "- Provide a minimal launch pattern (e.g., if __name__ == '__main__': result = your_job.execute_in_process()).\n"
            f"{docker_hint_common}"
        )
    else:
        system_addon = "You are an expert workflow developer. Generate clean Python code for the requested orchestrator.\n"
        user_addon = "Use idiomatic constructs for the requested orchestrator.\n"
    return system_addon, user_addon

class DirectDAGGenerator:
    """Generate orchestrator code (Airflow | Prefect | Dagster) from a natural language pipeline description."""
    
    def __init__(self, config: Dict, llm_provider: LLMProvider, orchestrator: str = 'airflow'):
        """
        Args:
            config: Configuration dictionary
            llm_provider: LLM provider instance for generating completions
            orchestrator: 'airflow' | 'prefect' | 'dagster'
        """
        self.config = config or {}
        self.llm_provider = llm_provider
        self.orchestrator = orchestrator.lower().strip() if orchestrator else 'airflow'
        self.token_usage_history = {'initial': {'input_tokens': 0, 'output_tokens': 0}}
    
    def _build_prompts(self, pipeline_description: str) -> Tuple[str, str]:
        """
        Build system and user prompts based on the requested orchestrator and config flags.
        """
        docker_based = self.config.get('direct_prompting', {}).get('dag_settings', {}).get('docker_based', False)
        sys_addon, user_addon = _orchestrator_guidance(self.orchestrator, docker_based)
        
        system_prompt = (
            f"{sys_addon}"
            "Return ONLY valid Python code for a single module. Do not include backticks or explanations.\n"
        )
        # Add orchestrator aware title/hint
        orchestrator_title = {
            'airflow': 'Apache Airflow DAG',
            'prefect': 'Prefect 2 Flow',
            'dagster': 'Dagster Job'
        }.get(self.orchestrator, 'Workflow')
        
        # Common user instructions
        base_user_prompt = (
            f"Convert the following pipeline description into a {orchestrator_title} Python module.\n\n"
            f"{user_addon}\n"
            "GENERAL CONSTRAINTS:\n"
            "- The code must be executable (assuming required packages are installed) and PEP 8 compliant.\n"
            "- Use clear function/task/op names derived from the pipeline steps.\n"
            "- Tasks/ops/steps must have explicit dependencies that reflect the described order and any branching/parallelism.\n"
            "- Avoid placeholders like <TODO> in code; if something is unknown, add minimal, safe defaults or comments.\n"
            "- Include minimal docstrings and comments where helpful.\n"
            "- Output only the complete Python code (no markdown, no comments above the module about usage).\n\n"
            "PIPELINE DESCRIPTION:\n"
            f"{pipeline_description}\n"
        )
        return system_prompt, base_user_prompt
    
    def generate_initial_dag(self, pipeline_description: str) -> Tuple[str, Dict]:
        """
        Generate initial orchestrator code directly from pipeline description.
        
        Returns:
            Tuple of (python_code, token_usage)
        """
        logging.info(f"Generating {self.orchestrator} code from pipeline description")
        
        system_prompt, user_prompt = self._build_prompts(pipeline_description)
        raw_code, token_usage = self.llm_provider.generate_completion(system_prompt, user_prompt)
        
        clean_code = self.extract_code(raw_code)
        self.token_usage_history['initial'] = token_usage or {'input_tokens': 0, 'output_tokens': 0}
        return clean_code, token_usage
    
    @staticmethod
    def extract_code(content: str) -> str:
        """
        Extract clean Python code from LLM response that may contain markdown fences.
        """
        if not content:
            return ""
        text = content.strip()
        # Handle code fences
        if "```python" in text:
            parts = text.split("```python", 1)[1]
            return parts.split("```", 1)[0].strip()
        if "```py" in text:
            parts = text.split("```py", 1)[1]
            return parts.split("```", 1)[0].strip()
        if "```" in text:
            parts = text.split("```", 1)
            if len(parts) > 1:
                return parts[1].split("```", 1)[0].strip()
        return text
    
    def _default_filename(self) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if self.orchestrator == 'airflow':
            return f"airflow_dag_{ts}.py"
        elif self.orchestrator == 'prefect':
            return f"prefect_flow_{ts}.py"
        elif self.orchestrator == 'dagster':
            return f"dagster_job_{ts}.py"
        else:
            return f"workflow_{ts}.py"
    
    def save_code(self, code: str, output_dir: Path, filename: Optional[str] = None) -> Path:
        """
        Save Python code to file.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = filename or self._default_filename()
        output_file = output_dir / filename
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(code)
        logging.info(f"Code saved to: {output_file}")
        return output_file
    
    def generate_dag(self, pipeline_description: str, output_dir: Path, output_filename: Optional[str] = None) -> Dict:
        """
        Generate and save orchestrator code from a pipeline description.
        """
        code, token_usage = self.generate_initial_dag(pipeline_description)
        out_file = self.save_code(code, output_dir, output_filename)
        results = {
            'orchestrator': self.orchestrator,
            'output_file': str(out_file),
            'token_usage': self.token_usage_history['initial'],
            'timestamp': datetime.now().isoformat()
        }
        # Save generation metadata
        meta_file = output_dir / "generation_metadata.json"
        try:
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            logging.warning(f"Failed to save generation metadata: {e}")
        return results


def main():
    parser = argparse.ArgumentParser(
        description='Step 0 - Direct Prompting: Generate orchestrator code from a pipeline description'
    )
    parser.add_argument('--config', default='config_llm.json', help='Path to LLM configuration file')
    parser.add_argument('--input', required=True, help='Path to pipeline description file (text)')
    parser.add_argument('--output-dir', default='outputs', help='Output directory for generated files')
    parser.add_argument('--output-filename', help='Custom filename for the generated file')
    parser.add_argument('--provider', choices=['deepinfra', 'openai', 'claude', 'azureopenai', 'ollama'],
                        help='Override the LLM provider specified in the config')
    parser.add_argument('--model', help='Override the model key (for selected provider)')
    parser.add_argument('--orchestrator', choices=ORCHESTRATOR_CHOICES, default='airflow',
                        help='Target orchestrator to generate code for')
    parser.add_argument('--skip-dependency-check', action='store_true', help='Skip dependency checks')
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Optional dependency checks (useful when running against OpenAI/Anthropic directly)
    if not args.skip_dependency_check and not check_dependencies():
        print("You can run with --skip-dependency-check to bypass this check")
        sys.exit(1)
    
    try:
        # Load configuration
        config = load_config(args.config)
        if not config:
            logging.error(f"Failed to load configuration from {args.config}")
            sys.exit(1)
        
        # Override provider if specified
        if args.provider:
            config.setdefault('model_settings', {}).setdefault('active_provider', args.provider)
            config['model_settings']['active_provider'] = args.provider
        
        # Validate API keys (unless skipped)
        if not args.skip_dependency_check and not validate_api_keys(config):
            logging.error("API key validation failed. Please check your configuration.")
            sys.exit(1)
        
        # Read pipeline description
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                pipeline_description = f.read().strip()
        except Exception as e:
            logging.error(f"Failed to read pipeline description from {args.input}: {e}")
            sys.exit(1)
        
        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize LLM provider
        try:
            llm_provider = LLMProvider(config, args.model)
            model_info = llm_provider.get_model_info()
            logging.info(f"Using provider={model_info.get('provider')} model={model_info.get('model_name')}")
        except Exception as e:
            logging.error(f"Failed to initialize LLM provider: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        # Generate code for the requested orchestrator
        generator = DirectDAGGenerator(config=config, llm_provider=llm_provider, orchestrator=args.orchestrator)
        results = generator.generate_dag(
            pipeline_description=pipeline_description,
            output_dir=output_dir,
            output_filename=args.output_filename
        )
        
        # Print final results
        total_tokens = (results['token_usage'].get('input_tokens', 0) +
                        results['token_usage'].get('output_tokens', 0))
        print("\nGeneration completed.")
        print(f"Orchestrator: {results['orchestrator']}")
        print(f"Tokens used:  {total_tokens}")
        print(f"Code file:    {results['output_file']}")
        print(f"main_output_path: {results['output_file']}")
    
    except Exception as e:
        logging.error(f"Generation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()