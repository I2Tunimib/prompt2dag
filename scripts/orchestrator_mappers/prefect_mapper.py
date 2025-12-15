"""
Prefect-specific mapper implementation.
Converts orchestrator-agnostic pipeline to Prefect 2.x flow definition.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from orchestrator_mappers.base_mapper import BaseOrchestratorMapper
from utils.prompt2dag_schema import (
    Component, FlowNode, OrchestratorTask, OrchestratorConnection,
    OrchestratorSchedule, Step2IntermediateYAML
)

class PrefectMapper(BaseOrchestratorMapper):
    """Maps orchestrator-agnostic pipeline to Prefect 2.x-specific format."""
    
    def get_orchestrator_name(self) -> str:
        return "prefect"
    
    def map_component_to_task(self, component: Component, node: FlowNode) -> OrchestratorTask:
        """
        Map component to Prefect task definition.
        
        Prefect Concepts:
        - Tasks are Python functions decorated with @task
        - Infrastructure is separate from task logic
        - Docker tasks use DockerContainer infrastructure
        """
        executor_type = component.executor_type
        executor_mapping = self.mapping_config['executor_mappings'].get(executor_type)
        
        if not executor_mapping:
            self.logger.warning(
                f"No mapping for executor type '{executor_type}', defaulting to Process infrastructure"
            )
            executor_mapping = self.mapping_config['executor_mappings']['python']
        
        # Validate and get normalized config
        executor_config_dict = self.validate_executor_config(component)
        
        # Build task config (infrastructure + task parameters)
        task_config = {
            'task_decorator': executor_mapping.get('task_decorator', '@task'),
            'infrastructure': {}
        }
        
        # Map infrastructure type
        if 'infrastructure_type' in executor_mapping:
            task_config['infrastructure']['type'] = executor_mapping['infrastructure_type']
            task_config['infrastructure']['module'] = executor_mapping['infrastructure_module']
        
        # Map executor config to infrastructure config
        config_mapping = executor_mapping.get('config_mapping', {})
        infrastructure_config = {}
        
        for step1_key, prefect_key in config_mapping.items():
            value = executor_config_dict.get(step1_key)
            
            # Special handling for command
            if step1_key == 'command':
                if value is None:
                    self.logger.info(
                        f"Task '{component.id}': No command specified, using image default"
                    )
                    continue
                elif isinstance(value, list):
                    # Prefect accepts list or string
                    infrastructure_config[prefect_key] = value
                else:
                    infrastructure_config[prefect_key] = value
            
            # Special handling for network (Prefect expects list)
            elif step1_key == 'network':
                if value:
                    infrastructure_config[prefect_key] = [value]  # Wrap in list
            
            elif value is not None:
                infrastructure_config[prefect_key] = value
        
        # Handle resources separately
        resource_mapping = executor_mapping.get('resource_mapping', {})
        if resource_mapping:
            resources = executor_config_dict.get('resources', {})
            for step1_key, prefect_key in resource_mapping.items():
                value = resources.get(step1_key)
                if value is not None:
                    infrastructure_config[prefect_key] = value
        
        # Add additional infrastructure params
        additional_params = executor_mapping.get('additional_params', {})
        if additional_params:
            infrastructure_config.update(additional_params)
        
        task_config['infrastructure']['config'] = infrastructure_config
        
        # Map retry policy (Prefect uses task-level retries)
        retry_policy_dict = component.retry_policy.model_dump()
        task_config['retries'] = retry_policy_dict.get('max_attempts', 0)
        
        if retry_policy_dict.get('delay_seconds', 0) > 0:
            task_config['retry_delay_seconds'] = retry_policy_dict['delay_seconds']
        
        # Map timeout
        upstream_policy_dict = node.upstream_policy.model_dump()
        if upstream_policy_dict.get('timeout_seconds'):
            task_config['timeout_seconds'] = upstream_policy_dict['timeout_seconds']
        
        # Prefect-specific: Task runner for concurrency
        concurrency_dict = component.concurrency.model_dump()
        if concurrency_dict.get('supports_parallelism'):
            task_config['task_runner'] = 'ConcurrentTaskRunner'
        
        # Build validation warnings
        validation_warnings = []
        if executor_type == 'docker' and not executor_config_dict.get('command'):
            validation_warnings.append(
                "Uses image default command - ensure Dockerfile has ENTRYPOINT/CMD"
            )
        
        return OrchestratorTask(
            task_id=node.component_type_id or component.id,
            task_name=component.name,
            operator_class=executor_mapping.get('infrastructure_type', 'Process'),
            operator_module=executor_mapping.get('infrastructure_module', 'prefect.infrastructure.process'),
            component_ref=component.id,
            config=task_config,
            upstream_task_ids=[],  # Filled by base class
            trigger_rule=None,  # Filled by base class
            retries=retry_policy_dict.get('max_attempts', 0),
            retry_delay_seconds=retry_policy_dict.get('delay_seconds', 0),
            validation_warnings=validation_warnings
        )
    
    def map_upstream_policy(self, policy_type: str) -> str:
        """
        Map upstream policy to Prefect wait pattern.
        
        Prefect Concepts:
        - wait_for='all': Wait for all upstream tasks (default)
        - wait_for='any': Wait for any upstream task
        - No built-in none_failed equivalent
        """
        mappings = self.mapping_config['upstream_policy_mappings']
        prefect_policy = mappings.get(policy_type, "wait_for='all'")
        
        if policy_type == 'none_failed':
            self.logger.warning(
                "Prefect doesn't have native 'none_failed' policy. "
                "Mapping to 'wait_for=all' with manual state checking required."
            )
        
        return prefect_policy
    
    def map_connection(self, connection: Any) -> OrchestratorConnection:
        """
        Map connection to Prefect block.
        
        Prefect Concepts:
        - Blocks are reusable configuration objects
        - Common blocks: Secret, DatabaseCredentials, S3Bucket, etc.
        """
        conn_dict = connection.model_dump() if hasattr(connection, 'model_dump') else connection
        
        conn_type = conn_dict.get('type')
        connection_mappings = self.mapping_config['connection_mappings']
        
        # Handle nested mappings (e.g., object_storage.s3)
        protocol = conn_dict.get('config', {}).get('protocol', '')
        
        
        block_mapping = None
        if conn_type == 'object_storage' and protocol in ['s3', 'gcs', 'azure_blob']:
            block_mapping = connection_mappings.get('object_storage', {}).get(protocol)
        else:
            block_mapping = connection_mappings.get(conn_type, {})
        
        if not block_mapping:
            self.logger.warning(
                f"No Prefect block mapping for connection type '{conn_type}', using Secret block"
            )
            block_mapping = {
                'block_type': 'Secret',
                'block_module': 'prefect.blocks.system'
            }
        
        # Build block config
        block_config = {
            'block_type': block_mapping.get('block_type', 'Secret'),
            'block_module': block_mapping.get('block_module', 'prefect.blocks.system'),
            'block_name': conn_dict.get('id'),  # Will be used as block name in Prefect
        }
        
        # Map connection config to block fields
        config = dict(conn_dict.get('config', {}))
        auth = conn_dict.get('authentication', {})
        
        # Handle authentication
        if auth.get('type') == 'token':
            config['token_secret_name'] = auth.get('token_env_var', 'API_TOKEN')
        elif auth.get('type') == 'basic':
            config['username_secret_name'] = auth.get('username_env_var')
            config['password_secret_name'] = auth.get('password_env_var')
        
        block_config['config'] = config
        
        return OrchestratorConnection(
            conn_id=conn_dict.get('id'),
            conn_type=block_mapping.get('block_type', 'Secret'),
            description=conn_dict.get('name'),
            config=block_config
        )
    
    def map_schedule(self) -> OrchestratorSchedule:
        """
        Map schedule to Prefect format.
        
        Prefect Concepts:
        - Supports cron, interval, and rrule schedules
        - Schedules are defined in deployment, not flow
        - Can have multiple schedules per flow
        """
        schedule_params = self.step1_data.parameters.schedule
        
        # Extract schedule configuration
        schedule_type = None
        schedule_value = None
        
        # Check for cron expression
        cron_expr = self._extract_param_value(schedule_params, 'cron_expression')
        if cron_expr:
            schedule_type = 'cron'
            schedule_value = cron_expr
        
        # Check for interval (Prefect extension)
        interval = self._extract_param_value(schedule_params, 'interval')
        if interval and not cron_expr:
            schedule_type = 'interval'
            schedule_value = interval
        
        # Extract other schedule parameters
        start_date_str = self._extract_param_value(schedule_params, 'start_date')
        is_enabled = self._extract_param_value(schedule_params, 'enabled', False)
        timezone = self._extract_param_value(schedule_params, 'timezone', 'UTC')
        
        # Prefect doesn't use catchup like Airflow
        # Instead, it has "backfill" which is deployment-specific
        catchup_val = self._extract_param_value(schedule_params, 'catchup', False)
        if catchup_val:
            self.logger.info(
                "Catchup enabled - note that Prefect handles backfilling differently than Airflow. "
                "Use deployment backfill commands instead."
            )
        
        # Build schedule config
        schedule_config = {
            'schedule_type': schedule_type,
            'schedule_value': schedule_value
        }
        
        return OrchestratorSchedule(
            enabled=is_enabled,
            schedule_expression=schedule_value,
            start_date=start_date_str,
            catchup=False,  # Prefect doesn't use catchup in same way
            timezone=timezone,
            # Add Prefect-specific metadata
            end_date=None  # Prefect supports end_date in deployments
        )
    
    def generate_intermediate_yaml(self) -> 'Step2IntermediateYAML':
        """
        Override to add Prefect-specific metadata.
        """
        # Call parent implementation
        intermediate = super().generate_intermediate_yaml()
        
        # Add Prefect-specific metadata
        prefect_metadata = {
            'flow_name': self.step1_data.pipeline_summary.name.lower().replace(' ', '_'),
            'deployment_name': f"{self.step1_data.pipeline_summary.name.lower().replace(' ', '_')}_deployment",
            'work_pool': self.mapping_config.get('deployment', {}).get('work_pool', 'default-agent-pool'),
            'task_runner': self._determine_task_runner(),
            'prefect_version': self.mapping_config.get('version', '2.14.0')
        }
        
        # Store in orchestrator_specific field (FIX)
        intermediate.metadata.orchestrator_specific = prefect_metadata
        
        self.logger.info(
            f"Generated Prefect intermediate YAML: "
            f"Flow={prefect_metadata['flow_name']}, "
            f"Deployment={prefect_metadata['deployment_name']}"
        )
        
        return intermediate
    
    def _determine_task_runner(self) -> str:
        """
        Determine appropriate Prefect task runner based on pipeline characteristics.
        
        Task Runners:
        - SequentialTaskRunner: Default, runs tasks sequentially
        - ConcurrentTaskRunner: For concurrent/parallel tasks
        - DaskTaskRunner: For distributed execution
        """
        analysis = self.step1_data.metadata.analysis_results
        
        if analysis.has_parallelism:
            return 'ConcurrentTaskRunner'
        else:
            return 'SequentialTaskRunner'