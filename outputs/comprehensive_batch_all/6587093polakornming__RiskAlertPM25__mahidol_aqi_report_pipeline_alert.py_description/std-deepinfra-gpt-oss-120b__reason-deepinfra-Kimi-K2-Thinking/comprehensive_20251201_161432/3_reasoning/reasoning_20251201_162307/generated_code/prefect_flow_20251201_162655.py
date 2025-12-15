import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import psycopg2
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

# Configuration
MAHIDOL_URL = os.getenv("MAHIDOL_AQI_URL", "https://example-mahidol-aqi.com")
HTML_OUTPUT_DIR = Path(os.getenv("HTML_OUTPUT_DIR", "./data/html"))
JSON_OUTPUT_DIR = Path(os.getenv("JSON_OUTPUT_DIR", "./data/json"))
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "aqi_db"),
    "user": os.getenv("DB_USER", "aqi_user"),
    "password": os.getenv("DB_PASSWORD", "aqi_password")
}
EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "sender_email": os.getenv("SENDER_EMAIL", "alerts@example.com"),
    "sender_password": os.getenv("SENDER_PASSWORD", "app_password"),
    "recipients_file": Path(os.getenv("RECIPIENTS_FILE", "./config/recipients.json"))
}
POLLUTION_MAPPING_FILE = Path(os.getenv("POLLUTION_MAPPING_FILE", "./config/pollution_mapping.json"))
AQI_THRESHOLD = int(os.getenv("AQI_THRESHOLD", "100"))

# Ensure directories exist
HTML_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)