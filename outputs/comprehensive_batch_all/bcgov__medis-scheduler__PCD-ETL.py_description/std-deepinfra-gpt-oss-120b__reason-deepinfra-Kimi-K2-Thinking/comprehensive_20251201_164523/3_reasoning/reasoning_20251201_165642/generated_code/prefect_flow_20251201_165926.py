from prefect import flow, task
from prefect.logging import get_run_logger
import time
from typing import Any


@task
def check_pcd_sftp_folder() -> str:
    """Simulates Kubernetes job checking PCD SFTP folder."""
    logger = get_run_logger()
    logger.info("Checking