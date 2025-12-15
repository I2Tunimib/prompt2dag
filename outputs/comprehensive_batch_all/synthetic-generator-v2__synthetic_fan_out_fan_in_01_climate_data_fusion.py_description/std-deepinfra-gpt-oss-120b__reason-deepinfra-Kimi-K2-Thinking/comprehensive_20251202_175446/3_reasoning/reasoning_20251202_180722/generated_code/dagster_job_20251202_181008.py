import os
import tempfile
from datetime import datetime
from typing import List

import pandas as pd
import requests
from dagster import (
    job,
    op,
    ScheduleDefinition,
    DefaultScheduleStatus,
    RetryPolicy,
    Out,
    In,
    Nothing,
    graph,
    AssetMaterialization,
    ExpectationResult,
    Failure,
    MetadataValue,
    OpExecutionContext,
)