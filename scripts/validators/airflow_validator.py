"""
Airflow-specific validator for intermediate YAML.
"""

from validators.base_validator import BaseValidator

class AirflowValidator(BaseValidator):
    """Validates intermediate YAML for Airflow compatibility."""
    
    def get_orchestrator_name(self) -> str:
        return "airflow"
    
    def validate_orchestrator_specific(self):
        """Airflow-specific validations."""
        self._validate_operators()
        self._validate_trigger_rules()
        self._validate_connections_airflow()
        self._validate_schedule()
    
    def _validate_operators(self):
        """Validate Airflow operators."""
        valid_operators = {
            'DockerOperator': 'airflow.providers.docker.operators.docker',
            'PythonOperator': 'airflow.operators.python',
            'BashOperator': 'airflow.operators.bash',
            'SQLExecuteQueryOperator': 'airflow.providers.common.sql.operators.sql',
        }
        
        for task in self.yaml_data.tasks:
            op_class = task.operator_class
            op_module = task.operator_module
            
            if op_class in valid_operators:
                expected_module = valid_operators[op_class]
                if op_module != expected_module:
                    self.result.add_warning(
                        f"Task '{task.task_id}': Module '{op_module}' doesn't match "
                        f"expected '{expected_module}' for {op_class}"
                    )
            else:
                self.result.add_info(
                    f"Task '{task.task_id}': Using non-standard operator '{op_class}'"
                )
            
            # Check Docker operator config
            if op_class == 'DockerOperator':
                self._validate_docker_operator(task)
    
    def _validate_docker_operator(self, task):
        """Validate Docker operator configuration."""
        config = task.config
        
        if 'image' not in config:
            self.result.add_error(f"DockerOperator task '{task.task_id}' missing 'image'")
        
        if 'docker_url' not in config:
            self.result.add_info(
                f"Task '{task.task_id}': No docker_url specified, will use Airflow default"
            )
        
        # Check for command
        if 'command' not in config:
            if not task.validation_warnings:
                self.result.add_warning(
                    f"Task '{task.task_id}': No command specified, ensure image has ENTRYPOINT/CMD"
                )
    
    def _validate_trigger_rules(self):
        """Validate trigger rules."""
        valid_rules = {
            'all_success', 'all_failed', 'all_done',
            'one_success', 'one_failed', 'none_failed',
            'none_failed_min_one_success', 'none_skipped', 'always'
        }
        
        for task in self.yaml_data.tasks:
            rule = task.trigger_rule
            if rule and rule not in valid_rules:
                self.result.add_error(
                    f"Task '{task.task_id}': Invalid trigger_rule '{rule}'"
                )
    
    def _validate_connections_airflow(self):
        """Validate Airflow connections."""
        valid_conn_types = {
            'fs', 'http', 'postgres', 'mysql', 'mongo', 's3', 'gcs',
            'azure_blob', 'generic'
        }
        
        for conn in self.yaml_data.connections:
            if conn.conn_type not in valid_conn_types:
                self.result.add_warning(
                    f"Connection '{conn.conn_id}': Unusual conn_type '{conn.conn_type}' "
                    f"(not in standard Airflow types)"
                )
    
    def _validate_schedule(self):
        """Validate schedule configuration."""
        schedule = self.yaml_data.schedule
        
        if schedule.enabled and not schedule.schedule_expression:
            self.result.add_error("Schedule enabled but no schedule_expression provided")
        
        if schedule.schedule_expression:
            # Basic cron validation (could be more sophisticated)
            expr = schedule.schedule_expression
            if not (expr.startswith('@') or len(expr.split()) == 5):
                self.result.add_warning(
                    f"Schedule expression '{expr}' may not be valid cron format"
                )