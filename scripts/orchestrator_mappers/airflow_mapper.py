"""
Airflow-specific mapper implementation.
"""

from typing import Dict, Any
from datetime import datetime
import logging

from orchestrator_mappers.base_mapper import BaseOrchestratorMapper
from utils.prompt2dag_schema import (
    Component, FlowNode, OrchestratorTask, OrchestratorConnection,
    OrchestratorSchedule
)

class AirflowMapper(BaseOrchestratorMapper):
    """Maps orchestrator-agnostic pipeline to Airflow-specific format."""
    
    def get_orchestrator_name(self) -> str:
        return "airflow"
    
    def map_component_to_task(self, component: Component, node: FlowNode) -> OrchestratorTask:
        """Map component to Airflow task definition."""
        executor_type = component.executor_type
        executor_mapping = self.mapping_config['executor_mappings'].get(executor_type)
        
        if not executor_mapping:
            self.logger.warning(f"No mapping for executor type '{executor_type}', defaulting to PythonOperator")
            executor_mapping = self.mapping_config['executor_mappings']['python']
        
        # Build task config
        task_config = {}
        
        # Validate and get normalized config
        executor_config_dict = self.validate_executor_config(component)
        
        config_mapping = executor_mapping.get('config_mapping', {})
        for step1_key, airflow_key in config_mapping.items():
            if isinstance(airflow_key, dict):
                continue
            
            value = executor_config_dict.get(step1_key)
            
            # Special handling for command
            if step1_key == 'command':
                if value is None:
                    # Command not specified - don't add to config (use image default)
                    self.logger.info(f"Task '{component.id}': Using image default command")
                    continue
                elif isinstance(value, list):
                    # Convert list to space-separated string if needed
                    # NOTE: DockerOperator accepts both list and string
                    task_config[airflow_key] = value  # Keep as list for Airflow 2.0+
                else:
                    task_config[airflow_key] = value
            elif value is not None:
                task_config[airflow_key] = value
        
        # Handle resources
        resource_mapping = executor_mapping.get('resource_mapping', {})
        if resource_mapping:
            resources = executor_config_dict.get('resources', {})
            for step1_key, airflow_key in resource_mapping.items():
                value = resources.get(step1_key)
                if value is not None:
                    task_config[airflow_key] = value
        
        # Add executor-specific additional params
        additional_params = executor_mapping.get('additional_params', {})
        if additional_params:
            task_config.update(additional_params)
        
        # Map retry policy
        retry_policy_dict = component.retry_policy.model_dump()
        
        if retry_policy_dict.get('delay_seconds', 0) > 0:
            task_config['retry_delay'] = f"timedelta(seconds={retry_policy_dict['delay_seconds']})"
        
        # Map timeout
        upstream_policy_dict = node.upstream_policy.model_dump()
        if upstream_policy_dict.get('timeout_seconds'):
            task_config['execution_timeout'] = f"timedelta(seconds={upstream_policy_dict['timeout_seconds']})"
        
        # Build validation warnings
        validation_warnings = []
        if executor_type == 'docker' and not executor_config_dict.get('command'):
            validation_warnings.append(
                "Uses image default command - ensure Dockerfile has ENTRYPOINT/CMD"
            )

        return OrchestratorTask(
            task_id=node.component_type_id or component.id,
            task_name=component.name,
            operator_class=executor_mapping['operator_class'],
            operator_module=executor_mapping['operator_module'],
            component_ref=component.id,
            config=task_config,
            upstream_task_ids=[],
            trigger_rule=None,
            retries=retry_policy_dict.get('max_attempts', 0),
            validation_warnings=validation_warnings, 
            retry_delay_seconds=retry_policy_dict.get('delay_seconds', 0)
        )

    def map_upstream_policy(self, policy_type: str) -> str:
        """Map upstream policy to Airflow trigger rule."""
        mappings = self.mapping_config['upstream_policy_mappings']
        return mappings.get(policy_type, 'all_success')
    
    def map_connection(self, connection: Any) -> OrchestratorConnection:
        """Map connection to Airflow connection."""
        conn_dict = connection.model_dump() if hasattr(connection, 'model_dump') else connection
    
        # Try to infer from protocol first
        protocol = conn_dict.get('config', {}).get('protocol', '')
        if protocol == 'mongodb':
            conn_type = 'mongo'
        elif protocol in ['s3', 'gcs', 'azure_blob']:
            conn_type = protocol
        else:
            # Fallback to mapping
            conn_type_mapping = self.mapping_config['connection_mappings']
            conn_type = conn_type_mapping.get(conn_dict.get('type'), {}).get('conn_type', 'generic')

        
        # Build config based on connection type
        config = dict(conn_dict.get('config', {}))
        
        # Handle authentication
        auth = conn_dict.get('authentication', {})
        if auth.get('type') == 'token':
            config['extra'] = {
                'token_env_var': auth.get('token_env_var')
            }
        elif auth.get('type') == 'basic':
            config['login'] = auth.get('username_env_var')
            config['password'] = auth.get('password_env_var')
        
        return OrchestratorConnection(
            conn_id=conn_dict.get('id'),
            conn_type=conn_type,
            description=conn_dict.get('name'),
            config=config
        )
    
    def map_schedule(self) -> OrchestratorSchedule:
        """Map schedule to Airflow format."""
        schedule_params = self.step1_data.parameters.schedule
        
        # Extract schedule expression
        schedule_expression = self._extract_param_value(schedule_params, 'cron_expression')
        
        if not schedule_expression:
            self.logger.info("No schedule defined - pipeline will be manually triggered")
    
        # Extract start date
        start_date_str = self._extract_param_value(schedule_params, 'start_date')
        
        # Extract enabled flag
        is_enabled = self._extract_param_value(schedule_params, 'enabled', False)
        
        # Extract catchup
        catchup_val = self._extract_param_value(schedule_params, 'catchup', False)
        
        # Extract timezone
        timezone = self._extract_param_value(schedule_params, 'timezone', 'UTC')
        
        return OrchestratorSchedule(
            enabled=is_enabled,
            schedule_expression=schedule_expression,
            start_date=start_date_str,
            catchup=catchup_val,
            timezone=timezone
        )