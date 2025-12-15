"""
Hybrid code generator combining templates and LLM.
Uses templates for task definitions, LLM for assembly and complex logic.
Balances determinism with flexibility.
"""

import time
import json
from typing import Dict, Any, Optional, List
from pathlib import Path

from code_generators.base_generator import (
    BaseCodeGenerator, GenerationResult, IntermediateYAML
)
from code_generators.template_generator import TemplateGenerator
from utils.llm_provider import LLMProvider


class HybridGenerator(BaseCodeGenerator):
    """
    Strategy 3: Hybrid template + LLM generation.
    
    Approach:
    1. Use templates to generate individual task definitions (deterministic)
    2. Use LLM to assemble tasks and handle complex orchestration logic
    
    Characteristics:
    - More consistent task definitions
    - Flexible orchestration logic
    - Handles complex patterns better than pure template
    - More reliable than pure LLM for task configs
    """
    
    STRATEGY_NAME = "hybrid"
    
    # LLM prompts for assembly
    ASSEMBLY_PROMPTS = {
        'airflow': """You are an expert Apache Airflow developer. I will provide you with:
1. Pre-generated task definitions (Python code snippets)
2. Pipeline metadata and dependency information

Your job is to assemble these into a complete, production-ready Airflow DAG.

REQUIREMENTS:
1. Use the provided task definitions exactly as given
2. Add necessary imports at the top
3. Create the DAG context manager
4. Set task dependencies using >> operator
5. Add any missing boilerplate code
6. Handle the specified pattern: {pattern}

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks
- The code must be directly executable""",

        'prefect': """You are an expert Prefect developer. I will provide you with:
1. Pre-generated task definitions (Python code snippets)
2. Pipeline metadata and dependency information

Your job is to assemble these into a complete, production-ready Prefect flow.

REQUIREMENTS:
1. Use the provided task definitions exactly as given
2. Add necessary imports at the top
3. Create the @flow decorated function
4. Call tasks in the correct order with proper dependencies
5. Handle the specified pattern: {pattern}

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks
- The code must be directly executable""",

        'dagster': """You are an expert Dagster developer. I will provide you with:
1. Pre-generated op definitions (Python code snippets)
2. Pipeline metadata and dependency information

Your job is to assemble these into a complete, production-ready Dagster job.

REQUIREMENTS:
1. Use the provided op definitions exactly as given
2. Add necessary imports at the top
3. Create the @job decorated function
4. Wire ops together with proper inputs/outputs
5. Handle the specified pattern: {pattern}

OUTPUT FORMAT:
- Return ONLY valid Python code
- Do NOT include markdown code blocks
- The code must be directly executable"""
    }
    
    def __init__(
        self,
        templates_dir: Path,
        output_dir: Path,
        llm_provider: LLMProvider,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize hybrid generator.
        
        Args:
            templates_dir: Templates directory
            output_dir: Output directory
            llm_provider: Configured LLM provider
            config: Optional configuration
        """
        super().__init__(templates_dir, output_dir, config)
        self.llm_provider = llm_provider
        self.pattern = 'sequential'
        
        # Initialize template generator for task generation
        self.template_generator = TemplateGenerator(
            templates_dir=templates_dir,
            output_dir=output_dir,
            config=config
        )
    
    def generate(
        self,
        intermediate_yaml: IntermediateYAML,
        **kwargs
    ) -> GenerationResult:
        """
        Generate code using hybrid approach.
        
        Process:
        1. Generate task definitions using templates
        2. Build assembly prompt with task snippets
        3. Call LLM to assemble complete pipeline
        4. Validate and save
        """
        start_time = time.time()
        warnings = []
        errors = []
        token_usage = {}
        
        orchestrator = intermediate_yaml.orchestrator
        pattern = intermediate_yaml.detect_pattern()
        self.pattern = pattern
        
        self.logger.info(
            f"Hybrid generation for {orchestrator}, pattern: {pattern}"
        )
        
        try:
            # Step 1: Generate task snippets using templates
            self.logger.info("Step 1: Generating task snippets from templates...")
            task_snippets = self._generate_task_snippets(intermediate_yaml)
            
            if not task_snippets:
                warnings.append("No task snippets generated, falling back to pure LLM")
            
            # Step 2: Build assembly prompt
            self.logger.info("Step 2: Building assembly prompt...")
            system_prompt = self.ASSEMBLY_PROMPTS.get(orchestrator, '').format(pattern=pattern)
            user_prompt = self._build_assembly_prompt(
                intermediate_yaml,
                task_snippets,
                pattern
            )
            
            # Step 3: Call LLM for assembly
            self.logger.info("Step 3: Calling LLM for assembly...")
            response, tokens = self.llm_provider.generate_completion(
                system_prompt=system_prompt,
                user_input=user_prompt
            )
            token_usage = tokens
            
            # Step 4: Clean and validate
            code = self._clean_llm_response(response)
            
            validation_warnings = self._validate_generated_code(code, orchestrator)
            if validation_warnings:
                warnings.extend(validation_warnings)
            
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
                    'task_snippets_count': len(task_snippets),
                    'llm_model': self.llm_provider.get_model_info().get('model_name', 'unknown'),
                    'task_count': len(intermediate_yaml.tasks)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Hybrid generation failed: {e}", exc_info=True)
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
    
    def _generate_task_snippets(
        self,
        intermediate_yaml: IntermediateYAML
    ) -> Dict[str, str]:
        """Generate individual task snippets using templates."""
        orchestrator = intermediate_yaml.orchestrator
        snippets = {}
        
        # Get appropriate task template
        task_template_map = {
            'airflow': 'task_docker.py.jinja2',
            'prefect': 'task_docker.py.jinja2',
            'dagster': 'op_docker.py.jinja2'
        }
        
        template_name = task_template_map.get(orchestrator)
        if not template_name:
            self.logger.warning(f"No task template for {orchestrator}")
            return snippets
        
        # Check if template exists
        template_path = self.get_template_path(orchestrator, template_name)
        if not template_path.exists():
            self.logger.warning(f"Task template not found: {template_path}")
            # Fall back to generating snippets manually
            return self._generate_task_snippets_fallback(intermediate_yaml)
        
        # Process each task
        for task in intermediate_yaml.tasks:
            try:
                context = self._build_task_context(task, orchestrator)
                snippet = self.render_template(orchestrator, template_name, context)
                snippets[task['task_id']] = snippet.strip()
            except Exception as e:
                self.logger.warning(f"Failed to generate snippet for {task['task_id']}: {e}")
                # Generate fallback snippet
                snippets[task['task_id']] = self._generate_fallback_snippet(task, orchestrator)
        
        return snippets
    
    def _generate_task_snippets_fallback(
        self,
        intermediate_yaml: IntermediateYAML
    ) -> Dict[str, str]:
        """Generate task snippets without templates."""
        orchestrator = intermediate_yaml.orchestrator
        snippets = {}
        
        for task in intermediate_yaml.tasks:
            snippets[task['task_id']] = self._generate_fallback_snippet(task, orchestrator)
        
        return snippets
    
    def _generate_fallback_snippet(self, task: Dict, orchestrator: str) -> str:
        """Generate a basic task snippet without templates."""
        task_id = task['task_id']
        config = task.get('config', {})
        
        if orchestrator == 'airflow':
            image = config.get('image', 'python:3.9')
            env = config.get('environment', {})
            env_str = json.dumps(env, indent=8)
            
            return f'''
{task_id} = DockerOperator(
    task_id='{task_id}',
    image='{image}',
    environment={env_str},
    network_mode='{config.get('network_mode', 'bridge')}',
    auto_remove={config.get('auto_remove', True)},
    docker_url='{config.get('docker_url', 'unix://var/run/docker.sock')}',
)'''
        
        elif orchestrator == 'prefect':
            infra = config.get('infrastructure', {}).get('config', {})
            image = infra.get('image', 'python:3.9')
            
            return f'''
@task(name='{task_id}', retries={task.get('retries', 0)})
def {task_id}():
    """Task: {task.get('task_name', task_id)}"""
    # Docker execution via infrastructure
    # Image: {image}
    pass'''
        
        elif orchestrator == 'dagster':
            executor = config.get('executor', {}).get('config', {})
            image = executor.get('image', 'python:3.9')
            
            return f'''
@op(
    name='{task_id}',
    description='{task.get('task_name', task_id)}',
)
def {task_id}(context):
    """Op: {task.get('task_name', task_id)}"""
    # Docker execution
    # Image: {image}
    pass'''
        
        return f"# Task: {task_id}"
    
    def _build_task_context(self, task: Dict, orchestrator: str) -> Dict[str, Any]:
        """Build context for task template rendering."""
        config = task.get('config', {})
        
        context = {
            'task_id': task['task_id'],
            'task_name': task.get('task_name', task['task_id']),
            'operator_class': task.get('operator_class', ''),
            'retries': task.get('retries', 0),
            'retry_delay_seconds': task.get('retry_delay_seconds', 0),
            'detected_pattern': self.pattern or 'sequential',  # ✅ ADD THIS
            'config': config,  # ✅ ADD THIS
        }
        
        # Extract Docker config based on orchestrator
        if orchestrator == 'airflow':
            context['image'] = config.get('image', '')
            context['command'] = config.get('command')
            context['environment'] = config.get('environment', {})
            context['network_mode'] = config.get('network_mode', 'bridge')
            context['auto_remove'] = config.get('auto_remove', True)
            context['docker_url'] = config.get('docker_url', 'unix://var/run/docker.sock')
        
        elif orchestrator == 'prefect':
            infra = config.get('infrastructure', {}).get('config', {})
            context['image'] = infra.get('image', '')
            context['command'] = infra.get('command')
            context['environment'] = infra.get('env', {})
            context['networks'] = infra.get('networks', [])
            context['auto_remove'] = infra.get('auto_remove', True)
        
        elif orchestrator == 'dagster':
            executor = config.get('executor', {}).get('config', {})
            container_kwargs = executor.get('container_kwargs', {})
            context['image'] = executor.get('image', '')
            context['command'] = container_kwargs.get('command')
            context['environment'] = container_kwargs.get('environment', {})
            context['network_mode'] = container_kwargs.get('network_mode', 'bridge')
            context['ins'] = config.get('ins', [])
            context['outs'] = config.get('outs', [])
        
        return context
    
    def _build_assembly_prompt(
        self,
        intermediate_yaml: IntermediateYAML,
        task_snippets: Dict[str, str],
        pattern: str
    ) -> str:
        """Build prompt for LLM assembly."""
        parts = [
            "## Pipeline Information",
            f"- Name: {intermediate_yaml.pipeline_name}",
            f"- Description: {intermediate_yaml.pipeline_description}",
            f"- Pattern: {pattern}",
            f"- Orchestrator: {intermediate_yaml.orchestrator}",
            "",
            "## Schedule",
            f"- Enabled: {intermediate_yaml.schedule.get('enabled', False)}",
            f"- Expression: {intermediate_yaml.schedule.get('schedule_expression', 'None')}",
            "",
            "## Pre-Generated Task Definitions",
            "Use these EXACTLY as provided:",
            ""
        ]
        
        # Add task snippets
        task_order = intermediate_yaml.get_task_order()
        for task_id in task_order:
            if task_id in task_snippets:
                parts.extend([
                    f"### Task: {task_id}",
                    "```python",
                    task_snippets[task_id],
                    "```",
                    ""
                ])
        
        # Add dependency information
        parts.extend([
            "## Task Dependencies (must be preserved)",
        ])
        
        for task in intermediate_yaml.tasks:
            upstreams = task.get('upstream_task_ids', [])
            if upstreams:
                parts.append(f"- {task['task_id']} depends on: {', '.join(upstreams)}")
            else:
                parts.append(f"- {task['task_id']} has no dependencies (entry point)")
        
        # Add orchestrator-specific metadata
        specific = intermediate_yaml.orchestrator_specific
        if specific:
            parts.extend([
                "",
                "## Orchestrator-Specific Configuration",
                json.dumps(specific, indent=2)
            ])
        
        parts.extend([
            "",
            "## Instructions",
            "1. Add all necessary imports at the top of the file",
            "2. Include the task definitions exactly as provided above",
            "3. Create the main pipeline structure with proper dependencies",
            "4. Ensure the code is complete and executable",
            f"5. Handle the '{pattern}' pattern appropriately"
        ])
        
        return "\n".join(parts)
    
    def _clean_llm_response(self, response: str) -> str:
        """Clean LLM response."""
        code = response.strip()
        
        if code.startswith('```python'):
            code = code[9:]
        elif code.startswith('```'):
            code = code[3:]
        
        if code.endswith('```'):
            code = code[:-3]
        
        return code.strip()
    
    def _validate_generated_code(self, code: str, orchestrator: str) -> List[str]:
        """Basic validation of generated code."""
        warnings = []
        
        try:
            compile(code, '<string>', 'exec')
        except SyntaxError as e:
            warnings.append(f"Syntax error: {e}")
        
        expected = {
            'airflow': ['from airflow', 'DAG'],
            'prefect': ['from prefect', '@flow'],
            'dagster': ['from dagster', '@job']
        }
        
        for pattern in expected.get(orchestrator, []):
            if pattern not in code:
                warnings.append(f"Missing expected pattern: {pattern}")
        
        return warnings