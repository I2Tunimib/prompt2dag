"""
Prefect-specific validator for intermediate YAML.
"""

from validators.base_validator import BaseValidator

class PrefectValidator(BaseValidator):
    """Validates intermediate YAML for Prefect compatibility."""
    
    def get_orchestrator_name(self) -> str:
        return "prefect"
    
    def validate_orchestrator_specific(self):
        """Prefect-specific validations."""
        self._validate_infrastructure()
        self._validate_blocks()
        self._validate_prefect_metadata()
    
    def _validate_infrastructure(self):
        """Validate Prefect infrastructure configuration."""
        valid_infrastructure = {
            'DockerContainer': 'prefect.infrastructure.docker',
            'Process': 'prefect.infrastructure.process',
            'KubernetesJob': 'prefect_kubernetes.jobs',
        }
        
        for task in self.yaml_data.tasks:
            infra_class = task.operator_class
            infra_module = task.operator_module
            
            if infra_class in valid_infrastructure:
                expected = valid_infrastructure[infra_class]
                if infra_module != expected:
                    self.result.add_warning(
                        f"Task '{task.task_id}': Infrastructure module '{infra_module}' "
                        f"doesn't match expected '{expected}'"
                    )
            
            # Check Docker infrastructure
            if infra_class == 'DockerContainer':
                self._validate_docker_container(task)
    
    def _validate_docker_container(self, task):
        """Validate Prefect DockerContainer infrastructure."""
        config = task.config
        infra_config = config.get('infrastructure', {}).get('config', {})
        
        if 'image' not in infra_config:
            self.result.add_error(
                f"DockerContainer task '{task.task_id}' missing 'image' in infrastructure config"
            )
        
        # Check networks format (should be list)
        networks = infra_config.get('networks')
        if networks and not isinstance(networks, list):
            self.result.add_error(
                f"Task '{task.task_id}': 'networks' must be a list, got {type(networks)}"
            )
    
    def _validate_blocks(self):
        """Validate Prefect blocks."""
        valid_blocks = {
            'LocalFileSystem', 'Secret', 'S3Bucket', 'GCSBucket',
            'DatabaseCredentials', 'AzureBlobStorageCredentials'
        }
        
        for conn in self.yaml_data.connections:
            block_type = conn.config.get('block_type')
            if block_type and block_type not in valid_blocks:
                self.result.add_info(
                    f"Connection '{conn.conn_id}': Using non-standard block type '{block_type}'"
                )
    
    def _validate_prefect_metadata(self):
        """Validate Prefect-specific metadata."""
        metadata = self.yaml_data.metadata.orchestrator_specific
        
        required_fields = ['flow_name', 'deployment_name', 'task_runner']
        for field in required_fields:
            if field not in metadata:
                self.result.add_warning(f"Missing Prefect metadata field: '{field}'")
        
        # Validate task runner
        task_runner = metadata.get('task_runner')
        valid_runners = {'SequentialTaskRunner', 'ConcurrentTaskRunner', 'DaskTaskRunner'}
        if task_runner and task_runner not in valid_runners:
            self.result.add_warning(f"Unusual task_runner: '{task_runner}'")