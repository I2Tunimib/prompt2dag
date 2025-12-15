"""
Dagster-specific validator for intermediate YAML.
"""

from validators.base_validator import BaseValidator

class DagsterValidator(BaseValidator):
    """Validates intermediate YAML for Dagster compatibility."""
    
    def get_orchestrator_name(self) -> str:
        return "dagster"
    
    def validate_orchestrator_specific(self):
        """Dagster-specific validations."""
        self._validate_ops()
        self._validate_resources()
        self._validate_ins_outs()
        self._validate_dagster_metadata()
    
    def _validate_ops(self):
        """Validate Dagster op configuration."""
        for task in self.yaml_data.tasks:
            if task.operator_class != 'Op':
                self.result.add_warning(
                    f"Task '{task.task_id}': Expected operator_class='Op', got '{task.operator_class}'"
                )
            
            config = task.config
            
            # Check executor
            executor = config.get('executor', {})
            if not executor.get('type'):
                self.result.add_error(f"Op '{task.task_id}' missing executor type")
            
            # Check retry policy format
            retry_policy = config.get('retry_policy')
            if retry_policy:
                if 'max_retries' not in retry_policy:
                    self.result.add_warning(
                        f"Op '{task.task_id}': retry_policy missing 'max_retries'"
                    )
    
    def _validate_resources(self):
        """Validate Dagster resources."""
        # Get all referenced resources
        all_required = set()
        for task in self.yaml_data.tasks:
            required = task.config.get('required_resource_keys', [])
            all_required.update(required)
        
        # Get defined resources
        defined = {conn.config.get('resource_key') for conn in self.yaml_data.connections}
        
        # Check for missing definitions
        missing = all_required - defined
        if missing:
            self.result.add_warning(
                f"Ops require resources that aren't defined in connections: {missing}"
            )
        
        # Check for unused resources
        unused = defined - all_required
        if unused:
            self.result.add_info(f"Defined resources not used by any op: {unused}")
    
    def _validate_ins_outs(self):
        """Validate op inputs and outputs."""
        for task in self.yaml_data.tasks:
            ins = task.config.get('ins', [])
            outs = task.config.get('outs', [])
            
            # Check format
            for io_list, io_type in [(ins, 'input'), (outs, 'output')]:
                for io_spec in io_list:
                    if 'name' not in io_spec:
                        self.result.add_error(
                            f"Op '{task.task_id}': {io_type} missing 'name'"
                        )
                    if 'dagster_type' not in io_spec:
                        self.result.add_warning(
                            f"Op '{task.task_id}': {io_type} '{io_spec.get('name')}' "
                            f"missing 'dagster_type'"
                        )
    
    def _validate_dagster_metadata(self):
        """Validate Dagster-specific metadata."""
        metadata = self.yaml_data.metadata.orchestrator_specific
        
        required_fields = ['job_name', 'executor_type', 'io_manager']
        for field in required_fields:
            if field not in metadata:
                self.result.add_warning(f"Missing Dagster metadata field: '{field}'")
        
        # Validate executor type
        executor = metadata.get('executor_type')
        valid_executors = {
            'in_process_executor', 'multiprocess_executor',
            'docker_executor', 'k8s_job_executor'
        }
        if executor and executor not in valid_executors:
            self.result.add_warning(f"Unusual executor_type: '{executor}'")