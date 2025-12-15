from prefect import flow, task, SequentialTaskRunner
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

# Pre-generated task definitions
@task
def task_a():
    pass

@task
def task_b():
    pass

@task
def task_c():
    pass

# Main pipeline structure
@flow(name="unnamed_pipeline", task_runner=SequentialTaskRunner)
def unnamed_pipeline():
    # Call tasks in the correct order with proper dependencies
    a = task_a()
    b = task_b()
    c = task_c()

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=unnamed_pipeline,
    name="unnamed_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=None,  # Schedule is disabled
)

if __name__ == "__main__":
    deployment.apply()