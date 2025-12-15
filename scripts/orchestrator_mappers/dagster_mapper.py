"""
Dagster-specific mapper implementation.
Converts orchestrator-agnostic pipeline to Dagster 1.x job definition.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from orchestrator_mappers.base_mapper import BaseOrchestratorMapper
from utils.prompt2dag_schema import (
    Component, FlowNode, OrchestratorTask, OrchestratorConnection,
    OrchestratorSchedule, Step2IntermediateYAML
)

class DagsterMapper(BaseOrchestratorMapper):
    """Maps orchestrator-agnostic pipeline to Dagster 1.x-specific format."""
    
    def get_orchestrator_name(self) -> str:
        return "dagster"
    
    def map_component_to_task(self, component: Component, node: FlowNode) -> OrchestratorTask:
        """
        Map component to Dagster op definition.
        
        Dagster Concepts:
        - Ops (operations) are the fundamental unit of computation
        - Jobs compose ops into executable workflows
        - Resources provide external connections
        - Executors determine how ops run (in-process, docker, k8s, etc.)
        """
        executor_type = component.executor_type
        executor_mapping = self.mapping_config['executor_mappings'].get(executor_type)
        
        if not executor_mapping:
            self.logger.warning(
                f"No mapping for executor type '{executor_type}', defaulting to in_process_executor"
            )
            executor_mapping = self.mapping_config['executor_mappings']['python']
        
        # Validate and get normalized config
        executor_config_dict = self.validate_executor_config(component)
        
        # Build op config
        op_config = {
            'op_decorator': executor_mapping.get('op_decorator', '@op'),
            'executor': {
                'type': executor_mapping.get('executor_type', 'in_process_executor'),
                'module': executor_mapping.get('executor_module', 'dagster')
            },
            'config_schema': {},
            'required_resource_keys': set()
        }
        
        # Map executor config based on type
        config_mapping = executor_mapping.get('config_mapping', {})
        executor_config = {}
        
        for step1_key, dagster_key in config_mapping.items():
            value = executor_config_dict.get(step1_key)
            
            # Handle nested keys (e.g., "container_kwargs.command")
            if step1_key == 'command':
                if value is None:
                    self.logger.info(
                        f"Op '{component.id}': No command specified, using image default"
                    )
                    continue
                elif isinstance(value, list):
                    # Dagster expects list for docker commands
                    self._set_nested_config(executor_config, dagster_key, value)
                else:
                    self._set_nested_config(executor_config, dagster_key, value)
            
            # Handle environment variables
            elif step1_key == 'environment':
                if value:
                    self._set_nested_config(executor_config, dagster_key, value)
            
            # Handle network
            elif step1_key == 'network':
                if value:
                    self._set_nested_config(executor_config, dagster_key, value)
            
            elif value is not None:
                self._set_nested_config(executor_config, dagster_key, value)
        
        # Handle resources separately
        resource_mapping = executor_mapping.get('resource_mapping', {})
        if resource_mapping:
            resources = executor_config_dict.get('resources', {})
            for step1_key, dagster_key in resource_mapping.items():
                value = resources.get(step1_key)
                if value is not None:
                    self._set_nested_config(executor_config, dagster_key, value)
        
        # Add additional executor params
        additional_params = executor_mapping.get('additional_params', {})
        if additional_params:
            for key, value in additional_params.items():
                if key not in executor_config:
                    executor_config[key] = value
        
        op_config['executor']['config'] = executor_config
        
        # Map retry policy (Dagster uses RetryPolicy objects)
        retry_policy_dict = component.retry_policy.model_dump()
        if retry_policy_dict.get('max_attempts', 0) > 0:
            op_config['retry_policy'] = {
                'max_retries': retry_policy_dict['max_attempts'],
                'delay': retry_policy_dict.get('delay_seconds', 1),
                'backoff': 'EXPONENTIAL' if retry_policy_dict.get('exponential_backoff') else 'CONSTANT'
            }
        
        # Map timeout
        upstream_policy_dict = node.upstream_policy.model_dump()
        if upstream_policy_dict.get('timeout_seconds'):
            op_config['timeout_seconds'] = upstream_policy_dict['timeout_seconds']
        
        # Map I/O specifications to ins/outs
        io_spec_dict = [spec.model_dump() for spec in component.io_spec]
        op_config['ins'] = []
        op_config['outs'] = []
        
        for io_spec in io_spec_dict:
            conn_id = io_spec.get('connection_id')
            if conn_id:
                resource_key = self._connection_to_resource_key(conn_id)
                op_config['required_resource_keys'].add(resource_key)
            if io_spec['direction'] == 'input':
                op_config['ins'].append({
                    'name': io_spec['name'].replace('-', '_'),
                    'dagster_type': self._map_io_type(io_spec['kind']),
                    'description': io_spec.get('path_pattern', f"Input {io_spec['name']}")  # Use path_pattern
                })
            elif io_spec['direction'] == 'output':
                op_config['outs'].append({
                    'name': io_spec['name'].replace('-', '_'),
                    'dagster_type': self._map_io_type(io_spec['kind']),
                    'description': f"Output to {io_spec.get('connection_id', 'sink')}"
                })
        
        # Identify required resources from connections
        for conn in component.connections:
            resource_key = self._connection_to_resource_key(conn.id)
            op_config['required_resource_keys'].add(resource_key)
        
        # Convert set to list for JSON serialization
        op_config['required_resource_keys'] = list(op_config['required_resource_keys'])
        
        # Build validation warnings
        validation_warnings = []
        if executor_type == 'docker' and not executor_config_dict.get('command'):
            validation_warnings.append(
                "Uses image default command - ensure Dockerfile has ENTRYPOINT/CMD"
            )
        
        # Dagster uses "op" terminology instead of "task"
        return OrchestratorTask(
            task_id=node.component_type_id or component.id,
            task_name=component.name,
            operator_class='Op',  # Generic - actual type in executor config
            operator_module='dagster',
            component_ref=component.id,
            config=op_config,
            upstream_task_ids=[],  # Filled by base class
            trigger_rule=None,  # Dagster doesn't use trigger rules like Airflow
            retries=retry_policy_dict.get('max_attempts', 0),
            retry_delay_seconds=retry_policy_dict.get('delay_seconds', 0),
            validation_warnings=validation_warnings
        )
    
    def map_upstream_policy(self, policy_type: str) -> str:
        """
        Map upstream policy to Dagster pattern.
        
        Dagster Concepts:
        - By default, ops wait for all upstream ops to succeed
        - Custom policies require dynamic execution or hooks
        - Nothing blocks are used for explicit dependencies without data flow
        """
        mappings = self.mapping_config['upstream_policy_mappings']
        dagster_policy = mappings.get(policy_type, 'default')
        
        if policy_type in ['any_success', 'one_success', 'custom']:
            self.logger.warning(
                f"Dagster doesn't natively support '{policy_type}' upstream policy. "
                f"Will require custom implementation using hooks or dynamic execution."
            )
        
        return dagster_policy
    
    def map_connection(self, connection: Any) -> OrchestratorConnection:
        """
        Map connection to Dagster resource.
        
        Dagster Concepts:
        - Resources are reusable objects passed to ops
        - Common resources: database connections, API clients, I/O managers
        - Resources are defined at job level and passed to ops via context
        """
        conn_dict = connection.model_dump() if hasattr(connection, 'model_dump') else connection
        
        conn_type = conn_dict.get('type')
        connection_mappings = self.mapping_config['connection_mappings']
        
        # Handle nested mappings (e.g., database.postgres)
        protocol = conn_dict.get('config', {}).get('protocol', '')
        
        resource_mapping = None
        
        # Check for protocol-specific mapping
        if conn_type == 'database' and protocol:
            db_mappings = connection_mappings.get('database', {})
            resource_mapping = db_mappings.get(protocol)
        elif conn_type == 'object_storage' and protocol:
            storage_mappings = connection_mappings.get('object_storage', {})
            resource_mapping = storage_mappings.get(protocol)
        else:
            resource_mapping = connection_mappings.get(conn_type, {})
        
        if not resource_mapping or not isinstance(resource_mapping, dict):
            self.logger.warning(
                f"No Dagster resource mapping for connection type '{conn_type}' "
                f"(protocol: '{protocol}'), using generic resource"
            )
            resource_mapping = {
                'resource_type': 'resource',
                'resource_module': 'dagster'
            }
        
        # Build resource config
        resource_config = {
            'resource_type': resource_mapping.get('resource_type', 'resource'),
            'resource_module': resource_mapping.get('resource_module', 'dagster'),
            'resource_key': self._connection_to_resource_key(conn_dict.get('id')),
        }
        
        # Map connection config to resource config
        config = dict(conn_dict.get('config', {}))
        auth = conn_dict.get('authentication', {})
        
        # Handle authentication
        if auth.get('type') == 'token':
            config['token'] = f"EnvVar('{auth.get('token_env_var', 'API_TOKEN')}')"
        elif auth.get('type') == 'basic':
            config['username'] = f"EnvVar('{auth.get('username_env_var', 'USERNAME')}')"
            config['password'] = f"EnvVar('{auth.get('password_env_var', 'PASSWORD')}')"
        
        resource_config['config'] = config
        
        return OrchestratorConnection(
            conn_id=conn_dict.get('id'),
            conn_type=resource_mapping.get('resource_type', 'resource'),
            description=conn_dict.get('name'),
            config=resource_config
        )
    
    def map_schedule(self) -> OrchestratorSchedule:
        """
        Map schedule to Dagster format.
        
        Dagster Concepts:
        - Schedules are defined separately from jobs
        - Use @schedule decorator
        - Supports cron expressions and interval schedules
        - Can have multiple schedules per job
        """
        schedule_params = self.step1_data.parameters.schedule
        
        # Extract schedule configuration
        cron_expr = self._extract_param_value(schedule_params, 'cron_expression')
        start_date_str = self._extract_param_value(schedule_params, 'start_date')
        is_enabled = self._extract_param_value(schedule_params, 'enabled', False)
        timezone = self._extract_param_value(schedule_params, 'timezone', 'UTC')
        
        # Dagster doesn't use catchup in the same way
        catchup_val = self._extract_param_value(schedule_params, 'catchup', False)
        if catchup_val:
            self.logger.info(
                "Catchup enabled - note that Dagster handles backfills through sensors "
                "or partition-based execution, not automatic catchup."
            )
        
        return OrchestratorSchedule(
            enabled=is_enabled,
            schedule_expression=cron_expr,
            start_date=start_date_str,
            catchup=False,  # Dagster uses partitions for backfills
            timezone=timezone,
            end_date=None
        )
    
    def generate_intermediate_yaml(self) -> 'Step2IntermediateYAML':
        """
        Override to add Dagster-specific metadata.
        """
        # Call parent implementation
        intermediate = super().generate_intermediate_yaml()
        
        # Add Dagster-specific metadata
        dagster_metadata = {
            'job_name': self.step1_data.pipeline_summary.name.lower().replace(' ', '_').replace('-', '_'),
            'description': self.step1_data.pipeline_summary.description,
            'executor_type': self._determine_executor(),
            'io_manager': self.mapping_config.get('assets', {}).get('io_manager', 'fs_io_manager'),
            'dagster_version': self.mapping_config.get('version', '1.5.0'),
            'use_assets': self.mapping_config.get('assets', {}).get('enable_asset_definitions', False)
        }
        
        # Collect all required resource keys
        all_resource_keys = set()
        for task in intermediate.tasks:
            task_resources = task.config.get('required_resource_keys', [])
            all_resource_keys.update(task_resources)
        
        dagster_metadata['required_resources'] = list(all_resource_keys)
        
        # Store in orchestrator_specific field (FIX)
        intermediate.metadata.orchestrator_specific = dagster_metadata
        
        self.logger.info(
            f"Generated Dagster intermediate YAML: "
            f"Job={dagster_metadata['job_name']}, "
            f"Ops={len(intermediate.tasks)}, "
            f"Resources={len(all_resource_keys)}"
        )
        
        return intermediate
    
    def _determine_executor(self) -> str:
        """
        Determine appropriate Dagster executor based on pipeline characteristics.
        
        Executors:
        - in_process_executor: Default, runs ops in same process
        - multiprocess_executor: Runs ops in separate processes
        - docker_executor: Runs ops in Docker containers
        - k8s_job_executor: Runs ops as Kubernetes jobs
        """
        analysis = self.step1_data.metadata.analysis_results
        executors_used = analysis.task_executors_used
        
        if 'docker' in executors_used or 'kubernetes' in executors_used:
            if 'kubernetes' in executors_used:
                return 'k8s_job_executor'
            return 'docker_executor'
        elif analysis.has_parallelism:
            return 'multiprocess_executor'
        else:
            return 'in_process_executor'
    
    def _set_nested_config(self, config: Dict, key_path: str, value: Any):
        """
        Set nested configuration value using dot notation.
        
        Example: "container_kwargs.command" -> config['container_kwargs']['command'] = value
        """
        keys = key_path.split('.')
        current = config
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def _map_io_type(self, kind: str) -> str:
        """Map Step 1 I/O kind to Dagster type."""
        mapping = {
            'file': 'String',  # File path
            'table': 'String',  # Table name
            'object': 'Any',
            'api': 'String',  # URL or endpoint
            'stream': 'String'
        }
        return mapping.get(kind, 'Any')
    
    def _connection_to_resource_key(self, conn_id: str) -> str:
        """Convert connection ID to valid Dagster resource key."""
        # Dagster resource keys must be valid Python identifiers
        return conn_id.replace('-', '_').replace('.', '_').lower()