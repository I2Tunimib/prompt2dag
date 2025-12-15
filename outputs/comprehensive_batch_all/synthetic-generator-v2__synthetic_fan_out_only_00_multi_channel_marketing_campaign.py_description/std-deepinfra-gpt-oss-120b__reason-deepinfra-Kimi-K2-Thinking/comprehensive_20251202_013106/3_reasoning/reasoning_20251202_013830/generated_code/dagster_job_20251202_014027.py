from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    OpExecutionContext,
    Config,
    get_dagster_logger,
)
import pandas as pd
import time
import random
import os

logger = get_dagster_logger()


class CSVConfig(Config):
    file_path: str = "customer_segments.csv"


class EmailConfig(Config):
    smtp_server: str = "smtp.example.com"
    from_address: str = "marketing@example.com"


class SMSConfig(Config):
    sms_gateway: str = "sms.provider.com"
    api_key: str = "dummy-api-key"


class PushConfig(Config):
    push_service: str = "push.provider.com"
    api_key: str