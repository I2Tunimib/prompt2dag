"""
Template-based code generator.
Uses pure Jinja2 templates to generate orchestrator code.
Deterministic output - same input always produces same output.
"""

import time
from typing import Dict, Any, Optional
from pathlib import Path

from code_generators.base_generator import (
    BaseCodeGenerator, GenerationResult, IntermediateYAML
)


class TemplateGenerator(BaseCodeGenerator):
    """
    Strategy 1: Pure template-based generation.
    
    Characteristics:
    - Deterministic output
    - Fast generation
    - Limited flexibility for unusual patterns
    - Best for well-defined, common patterns
    """
    
    STRATEGY_NAME = "template"
    
    # Template mapping by orchestrator and pattern
    TEMPLATE_MAP = {
        'airflow': {
            'base': 'dag_base.py.jinja2',
            'sequential': 'pattern_sequential.py.jinja2',
            'fanout': 'pattern_fanout.py.jinja2',
            'fanin': 'pattern_fanout.py.jinja2',  # Same template handles both
            'fanout_fanin': 'pattern_fanout.py.jinja2',
            'branching': 'pattern_branch.py.jinja2',
            'sensor_driven': 'pattern_sensor.py.jinja2',
            'task_docker': 'task_docker.py.jinja2',
            'task_python': 'task_python.py.jinja2',
        },
        'prefect': {
            'base': 'flow_base.py.jinja2',
            'sequential': 'pattern_sequential.py.jinja2',
            'fanout': 'pattern_fanout.py.jinja2',
            'fanin': 'pattern_fanout.py.jinja2',
            'fanout_fanin': 'pattern_fanout.py.jinja2',
            'branching': 'pattern_branch.py.jinja2',
            'sensor_driven': 'pattern_sensor.py.jinja2',
            'task_docker': 'task_docker.py.jinja2',
        },
        'dagster': {
            'base': 'job_base.py.jinja2',
            'sequential': 'pattern_sequential.py.jinja2',
            'fanout': 'pattern_fanout.py.jinja2',
            'fanin': 'pattern_fanout.py.jinja2',
            'fanout_fanin': 'pattern_fanout.py.jinja2',
            'branching': 'pattern_branch.py.jinja2',
            'sensor_driven': 'pattern_sensor.py.jinja2',
            'op_docker': 'op_docker.py.jinja2',
        }
    }
    
    def generate(
        self,
        intermediate_yaml: IntermediateYAML,
        **kwargs
    ) -> GenerationResult:
        """
        Generate code using templates.
        
        Process:
        1. Detect pattern
        2. Select appropriate templates
        3. Build context
        4. Render templates
        5. Assemble final code
        """
        start_time = time.time()
        warnings = []
        errors = []
        
        orchestrator = intermediate_yaml.orchestrator
        pattern = intermediate_yaml.detect_pattern()
        
        self.logger.info(
            f"Template generation for {orchestrator}, pattern: {pattern}"
        )
        
        try:
            # Validate orchestrator is supported
            if orchestrator not in self.TEMPLATE_MAP:
                raise ValueError(f"Unsupported orchestrator: {orchestrator}")
            
            templates = self.TEMPLATE_MAP[orchestrator]
            
            # Check if we have a template for this pattern
            if pattern not in templates:
                warnings.append(
                    f"No specific template for pattern '{pattern}', falling back to sequential"
                )
                pattern = 'sequential'
            
            # Build context
            context = self._build_template_context(intermediate_yaml, pattern)
            
            # Generate code based on orchestrator
            if orchestrator == 'airflow':
                code = self._generate_airflow(intermediate_yaml, context, pattern)
            elif orchestrator == 'prefect':
                code = self._generate_prefect(intermediate_yaml, context, pattern)
            elif orchestrator == 'dagster':
                code = self._generate_dagster(intermediate_yaml, context, pattern)
            else:
                raise ValueError(f"Unknown orchestrator: {orchestrator}")
            
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
                warnings=warnings,
                metadata={
                    'pattern': pattern,
                    'template_used': templates.get(pattern, 'sequential'),
                    'task_count': len(intermediate_yaml.tasks)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Template generation failed: {e}", exc_info=True)
            generation_time = (time.time() - start_time) * 1000
            
            return GenerationResult(
                strategy=self.STRATEGY_NAME,
                orchestrator=orchestrator,
                success=False,
                generation_time_ms=generation_time,
                errors=[str(e)],
                warnings=warnings
            )
    
    def _build_template_context(
        self,
        intermediate_yaml: IntermediateYAML,
        pattern: str
    ) -> Dict[str, Any]:
        """Build context for template rendering."""
        context = self.build_base_context(intermediate_yaml)
        
        # Add template-specific context
        context['pattern'] = pattern
        
        # Process tasks for template consumption
        context['processed_tasks'] = self._process_tasks_for_template(
            intermediate_yaml.tasks,
            intermediate_yaml.orchestrator
        )
        
        # Build dependency chains
        context['dependencies'] = self._build_dependency_list(intermediate_yaml.tasks)
        
        # Default values for Airflow
        if intermediate_yaml.orchestrator == 'airflow':
            context['default_retries'] = self.config.get('default_retries', 1)
            context['default_retry_delay'] = self.config.get('default_retry_delay', 5)
            context['start_date'] = {
                'year': 2024, 'month': 1, 'day': 1
            }
            context['dag_id'] = intermediate_yaml.pipeline_name
            context['tags'] = ['generated', 'template', pattern]
            context['host_data_dir_default'] = self.config.get(
                'host_data_dir', '/tmp/airflow/data'
            )
        
        # Prefect-specific
        elif intermediate_yaml.orchestrator == 'prefect':
            specific = intermediate_yaml.orchestrator_specific
            context['flow_name'] = specific.get('flow_name', intermediate_yaml.pipeline_name)
            context['task_runner'] = specific.get('task_runner', 'SequentialTaskRunner')
            context['work_pool'] = specific.get('work_pool', 'default-agent-pool')
        
        # Dagster-specific
        elif intermediate_yaml.orchestrator == 'dagster':
            specific = intermediate_yaml.orchestrator_specific
            context['job_name'] = specific.get('job_name', intermediate_yaml.pipeline_name)
            context['executor_type'] = specific.get('executor_type', 'in_process_executor')
            context['required_resources'] = specific.get('required_resources', [])
        
        return context
    
    def _process_tasks_for_template(
        self,
        tasks: list,
        orchestrator: str
    ) -> list:
        """Process tasks into template-friendly format."""
        processed = []
        
        for task in tasks:
            processed_task = {
                'task_id': task['task_id'],
                'task_name': task.get('task_name', task['task_id']),
                'operator_class': task.get('operator_class', ''),
                'operator_module': task.get('operator_module', ''),
                'upstream_task_ids': task.get('upstream_task_ids', []),
                'trigger_rule': task.get('trigger_rule', 'all_success'),
                'retries': task.get('retries', 1),
                'retry_delay_seconds': task.get('retry_delay_seconds', 0),
            }
            
            # Extract config
            config = task.get('config', {})
            
            # Docker-specific
            if 'docker' in task.get('operator_class', '').lower():
                if orchestrator == 'airflow':
                    processed_task['image'] = config.get('image', '')
                    processed_task['command'] = config.get('command')
                    processed_task['environment'] = config.get('environment', {})
                    processed_task['network_mode'] = config.get('network_mode', 'bridge')
                    processed_task['auto_remove'] = config.get('auto_remove', True)
                    processed_task['docker_url'] = config.get(
                        'docker_url', 'unix://var/run/docker.sock'
                    )
                
                elif orchestrator == 'prefect':
                    infra = config.get('infrastructure', {}).get('config', {})
                    processed_task['image'] = infra.get('image', '')
                    processed_task['command'] = infra.get('command')
                    processed_task['environment'] = infra.get('env', {})
                    processed_task['networks'] = infra.get('networks', [])
                    processed_task['auto_remove'] = infra.get('auto_remove', True)
                    processed_task['stream_output'] = infra.get('stream_output', True)
                
                elif orchestrator == 'dagster':
                    executor = config.get('executor', {}).get('config', {})
                    container_kwargs = executor.get('container_kwargs', {})
                    processed_task['image'] = executor.get('image', '')
                    processed_task['command'] = container_kwargs.get('command')
                    processed_task['environment'] = container_kwargs.get('environment', {})
                    processed_task['network_mode'] = container_kwargs.get('network_mode', 'bridge')
                    processed_task['ins'] = config.get('ins', [])
                    processed_task['outs'] = config.get('outs', [])
            
            processed.append(processed_task)
        
        return processed
    
    def _build_dependency_list(self, tasks: list) -> list:
        """Build list of (upstream, downstream) dependency tuples."""
        dependencies = []
        
        for task in tasks:
            task_id = task['task_id']
            for upstream_id in task.get('upstream_task_ids', []):
                dependencies.append((upstream_id, task_id))
        
        return dependencies
    
    def _generate_airflow(
        self,
        intermediate_yaml: IntermediateYAML,
        context: Dict[str, Any],
        pattern: str
    ) -> str:
        """Generate Airflow DAG code."""
        # For now, use the main pattern template which includes everything
        template_name = self.TEMPLATE_MAP['airflow'].get(pattern, 'pattern_sequential.py.jinja2')
        
        return self.render_template('airflow', template_name, context)
    
    def _generate_prefect(
        self,
        intermediate_yaml: IntermediateYAML,
        context: Dict[str, Any],
        pattern: str
    ) -> str:
        """Generate Prefect flow code."""
        template_name = self.TEMPLATE_MAP['prefect'].get(pattern, 'pattern_sequential.py.jinja2')
        
        return self.render_template('prefect', template_name, context)
    
    def _generate_dagster(
        self,
        intermediate_yaml: IntermediateYAML,
        context: Dict[str, Any],
        pattern: str
    ) -> str:
        """Generate Dagster job code."""
        template_name = self.TEMPLATE_MAP['dagster'].get(pattern, 'pattern_sequential.py.jinja2')
        
        return self.render_template('dagster', template_name, context)