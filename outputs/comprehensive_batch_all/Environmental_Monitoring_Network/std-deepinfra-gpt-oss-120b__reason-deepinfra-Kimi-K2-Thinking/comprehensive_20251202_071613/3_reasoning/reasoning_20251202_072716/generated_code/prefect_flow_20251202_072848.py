import os
import json
import requests
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

# Configuration
DATA_DIR = os.getenv("DATA_DIR", "./data")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:3003")
HERE_API_TOKEN = os.getenv("HERE_API_TOKEN", "your_here_api_token")
GEOAPIFY_API_KEY = os.getenv("GEOAPIFY_API_KEY", "your_geoapify_api_key")