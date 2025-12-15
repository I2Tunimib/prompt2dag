import os
import json
import requests
from pathlib import Path
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATASET_ID = 2
API_BASE_URL = "http://localhost:3003"