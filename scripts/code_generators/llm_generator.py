"""
LLM-based code generator.
Uses language models to generate orchestrator code from intermediate YAML.
Flexible but non-deterministic output.
"""

import time
import json
from typing import Dict, Any, Optional
from pathlib import Path

from code_generators.base_generator import (
    BaseCodeGenerator, GenerationResult, IntermediateYAML
)
from utils.llm_provider import LLMProvider


class LLMGenerator(BaseCodeGenerator):
    """
    Strategy 2: Pure LLM-based generation.
    
    Characteristics:
    - Flexible, can handle unusual patterns
    - Non-deterministic output
    - Slower due to API calls
    - May produce errors requiring iteration
    - Best for complex or unusual patterns
    """
    
    STRATEGY_NAME = "llm"
    
    # System prompts for each orchestrator
    SYSTEM_PROMPTS = {
        'airflow': """You are an expert Apache Airflow developer. Generate production-ready Airflow DAG code based on the provided pipeline specification.

REQUIREMENTS:
1. Use modern Airflow 2.x syntax with TaskFlow API where appropriate
2. Include all necessary imports
3. Use DockerOperator for docker tasks
4. Set proper task dependencies using >> operator or chain()
5. Include error handling and retries
6. Add meaningful comments
7. Follow PEP 8 style guidelines

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks (no ```)
- The code must be directly executable
- Include a header comment with generation metadata""",

        'prefect': """You are an expert Prefect developer. Generate production-ready Prefect 2.x flow code based on the provided pipeline specification.

REQUIREMENTS:
1. Use @flow and @task decorators
2. Use appropriate task runners (Sequential, Concurrent)
3. Handle Docker tasks using infrastructure blocks or subprocess
4. Set proper task dependencies through function calls
5. Include error handling with retries parameter
6. Add meaningful docstrings
7. Follow PEP 8 style guidelines

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks (no ```)
- The code must be directly executable
- Include a header comment with generation metadata""",

        'dagster': """You are an expert Dagster developer. Generate production-ready Dagster job code based on the provided pipeline specification.

REQUIREMENTS:
1. Use @op and @job decorators
2. Define proper In/Out for data flow
3. Handle Docker execution using docker_executor or ops
4. Set proper op dependencies through function parameters
5. Include retry policies where specified
6. Add meaningful docstrings
7. Follow PEP 8 style guidelines

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks (no ```)
- The code must be directly executable
- Include a header comment with generation metadata"""
    }
    
    def __init__(
        self,
        templates_dir: Path,
        output_dir: Path,
        llm_provider: LLMProvider,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize LLM generator.
        
        Args:
            templates_dir: Templates directory (used by base class)
            output_dir: Output directory
            llm_provider: Configured LLM provider
            config: Optional configuration
        """
        super().__init__(templates_dir, output_dir, config)
        self.llm_provider = llm_provider
    
    def generate(
        self,
        intermediate_yaml: IntermediateYAML,
        **kwargs
    ) -> GenerationResult:
        """
        Generate code using LLM.
        
        Process:
        1. Build prompt from intermediate YAML
        2. Call LLM API
        3. Clean and validate response
        4. Save generated code
        """
        start_time = time.time()
        warnings = []
        errors = []
        token_usage = {}
        
        orchestrator = intermediate_yaml.orchestrator
        pattern = intermediate_yaml.detect_pattern()
        
        self.logger.info(
            f"LLM generation for {orchestrator}, pattern: {pattern}"
        )
        
        try:
            # Get system prompt for orchestrator
            system_prompt = self.SYSTEM_PROMPTS.get(orchestrator)
            if not system_prompt:
                raise ValueError(f"No system prompt for orchestrator: {orchestrator}")
            
            # Build user prompt with YAML data
            user_prompt = self._build_user_prompt(intermediate_yaml)
            
            # Call LLM
            self.logger.info("Calling LLM for code generation...")
            response, tokens = self.llm_provider.generate_completion(
                system_prompt=system_prompt,
                user_input=user_prompt
            )
            token_usage = tokens
            
            # Clean response (remove markdown code blocks if present)
            code = self._clean_llm_response(response)
            
            # Basic validation
            validation_errors = self._validate_generated_code(code, orchestrator)
            if validation_errors:
                warnings.extend(validation_errors)
            
            # Save code
            output_path = self.save_code(code, intermediate_yaml)
            
            generation_time = (time.time() - start_time) * 1000
            
            return GenerationResult(
                strategy=self.STRATEGY_NAME,
                orchestrator=orchestrator,
                success=True,
                code=code,
                output_path=output_path,
                generation_time_ms=generation_time,
                token_usage=token_usage,
                warnings=warnings,
                metadata={
                    'pattern': pattern,
                    'llm_model': self.llm_provider.get_model_info().get('model_name', 'unknown'),
                    'task_count': len(intermediate_yaml.tasks)
                }
            )
            
        except Exception as e:
            self.logger.error(f"LLM generation failed: {e}", exc_info=True)
            generation_time = (time.time() - start_time) * 1000
            
            return GenerationResult(
                strategy=self.STRATEGY_NAME,
                orchestrator=orchestrator,
                success=False,
                generation_time_ms=generation_time,
                token_usage=token_usage,
                errors=[str(e)],
                warnings=warnings
            )
    
    def _build_user_prompt(self, intermediate_yaml: IntermediateYAML) -> str:
        """Build detailed user prompt from intermediate YAML."""
        pattern = intermediate_yaml.detect_pattern()
        
        prompt_parts = [
            f"Generate a complete {intermediate_yaml.orchestrator} pipeline with the following specification:",
            "",
            f"## Pipeline Metadata",
            f"- Name: {intermediate_yaml.pipeline_name}",
            f"- Description: {intermediate_yaml.pipeline_description}",
            f"- Pattern: {pattern}",
            "",
            "## Schedule Configuration",
            f"- Enabled: {intermediate_yaml.schedule.get('enabled', False)}",
            f"- Expression: {intermediate_yaml.schedule.get('schedule_expression', 'None')}",
            f"- Timezone: {intermediate_yaml.schedule.get('timezone', 'UTC')}",
            f"- Catchup: {intermediate_yaml.schedule.get('catchup', False)}",
            "",
            "## Connections/Resources",
        ]
        
        for conn in intermediate_yaml.connections:
            prompt_parts.append(f"- {conn['conn_id']}: {conn['conn_type']} - {conn.get('description', '')}")
        
        prompt_parts.extend([
            "",
            "## Tasks (in execution order)",
        ])
        
        task_order = intermediate_yaml.get_task_order()
        for task_id in task_order:
            task = next((t for t in intermediate_yaml.tasks if t['task_id'] == task_id), None)
            if task:
                prompt_parts.extend(self._format_task_for_prompt(task))
        
        prompt_parts.extend([
            "",
            "## Task Dependencies",
        ])
        
        for task in intermediate_yaml.tasks:
            upstreams = task.get('upstream_task_ids', [])
            if upstreams:
                prompt_parts.append(f"- {task['task_id']} depends on: {', '.join(upstreams)}")
        
        # Add orchestrator-specific hints
        specific = intermediate_yaml.orchestrator_specific
        if specific:
            prompt_parts.extend([
                "",
                "## Orchestrator-Specific Configuration",
                json.dumps(specific, indent=2)
            ])
        
        return "\n".join(prompt_parts)
    
    def _format_task_for_prompt(self, task: Dict[str, Any]) -> list:
        """Format a single task for the prompt."""
        lines = [
            "",
            f"### Task: {task['task_id']}",
            f"- Name: {task.get('task_name', task['task_id'])}",
            f"- Operator: {task.get('operator_class', 'Unknown')}",
            f"- Retries: {task.get('retries', 0)}",
        ]
        
        config = task.get('config', {})
        
        # Docker-specific
        if 'image' in config:
            lines.append(f"- Image: {config['image']}")
        
        # Environment variables
        env_key = 'environment' if 'environment' in config else 'env'
        if env_key in config:
            env = config[env_key]
            if env:
                lines.append(f"- Environment Variables:")
                for k, v in env.items():
                    lines.append(f"    - {k}: {v}")
        
        # Infrastructure config (Prefect/Dagster)
        if 'infrastructure' in config:
            infra = config['infrastructure'].get('config', {})
            if infra.get('image'):
                lines.append(f"- Image: {infra['image']}")
            if infra.get('env'):
                lines.append(f"- Environment Variables:")
                for k, v in infra['env'].items():
                    lines.append(f"    - {k}: {v}")
        
        # Executor config (Dagster)
        if 'executor' in config:
            executor = config['executor']
            lines.append(f"- Executor: {executor.get('type', 'unknown')}")
        
        return lines
    
    def _clean_llm_response(self, response: str) -> str:
        """Clean LLM response to extract valid Python code."""
        code = response.strip()
        
        # Remove markdown code blocks
        if code.startswith('```python'):
            code = code[9:]
        elif code.startswith('```'):
            code = code[3:]
        
        if code.endswith('```'):
            code = code[:-3]
        
        return code.strip()
    
    def _validate_generated_code(self, code: str, orchestrator: str) -> list:
        """Basic validation of generated code."""
        warnings = []
        
        # Check for syntax errors
        try:
            compile(code, '<string>', 'exec')
        except SyntaxError as e:
            warnings.append(f"Syntax error in generated code: {e}")
        
        # Check for expected imports
        expected_imports = {
            'airflow': ['from airflow', 'DAG'],
            'prefect': ['from prefect', '@flow', '@task'],
            'dagster': ['from dagster', '@op', '@job']
        }
        
        for pattern in expected_imports.get(orchestrator, []):
            if pattern not in code:
                warnings.append(f"Missing expected pattern: {pattern}")
        
        return warnings