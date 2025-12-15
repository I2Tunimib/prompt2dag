import prefect
from prefect import flow
from prefect.task_runners import SequentialTaskRunner


@flow(name="unnamed_pipeline", task_runner=SequentialTaskRunner())
def unnamed_pipeline():
    """Empty pipeline flow."""
    # No tasks to execute for the 'empty' pattern
    pass


if __name__ == "__main__":
    unnamed_pipeline()