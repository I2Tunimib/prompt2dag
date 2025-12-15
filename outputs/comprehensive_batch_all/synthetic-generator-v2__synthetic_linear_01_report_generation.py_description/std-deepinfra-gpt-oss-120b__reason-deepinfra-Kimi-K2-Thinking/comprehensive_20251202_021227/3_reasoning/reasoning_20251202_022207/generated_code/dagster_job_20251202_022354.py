from dagster import (
    op,
    job,
    schedule,
    ScheduleDefinition,
    RetryPolicy,
    DefaultScheduleStatus,
    In,
    Out,
    Nothing,
    AssetMaterialization,
    MetadataValue,
    get_dagster_logger,
    build_schedule_from_partitioned_job,
    daily_partitioned_config,
    Definitions,
)
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime