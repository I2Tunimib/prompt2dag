from __future__ import annotations

import json
import os
import smtplib
import ssl
from pathlib import Path
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup
from dagster import (
    Config,
    Field,
    In,
    Out,
    ResourceDefinition,
    String,
    Bool,
    List,
    Int,
    job,
    op,
    resource,
)


class FetchHtmlConfig(Config):
    url: str = Field(..., description="URL of the Mahidol AQI report page.")
    output_path: str = Field(
        default="mahidol_aqi.html",
        description="Local file path where the fetched HTML will be saved.",
    )


class ExtractValidateConfig(Config):
    duplicate_check: bool = Field(
        default=True,
        description="Whether to perform a duplicate check against the database.",
    )
    json_output_path: str = Field(
        default="mahidol_aqi.json",
        description="Local file path where the extracted JSON will be saved.",
    )


class LoadPostgresConfig(Config):
    # In a real deployment this would include table names, schema, etc.
    pass


class EmailAlertConfig(Config):
    aqi_threshold: int = Field(
        default=100,
        description="AQI value above which an email alert should be sent.",
    )
    subject: str = Field(
        default="Air Quality Alert",
        description="Subject line for the alert email.",
    )
    body_template: str = Field(
        default="Current AQI is {aqi}, which exceeds the safe threshold of {threshold}.",
        description="Template for the email body. Use {aqi} and {threshold} placeholders.",
    )


@resource(config_schema={"conn_str": str})
def postgres_resource(init_context) -> str:
    """Simple stub returning a PostgreSQL connection string."""
    return init_context.resource_config["conn_str"]


@resource(
    config_schema={
        "smtp_server": str,
        "smtp_port": int,
        "username": str,
        "password": str,
        "recipients": List[str],
    }
)
def email_resource(init_context) -> Dict[str, Any]:
    """Provides SMTP configuration for sending emails."""
    return init_context.resource_config


@op(
    config_schema=FetchHtmlConfig.to_dict(),
    out=Out(String),
    description="Downloads the Mahidol AQI HTML page and saves it locally.",
)
def fetch_mahidol_aqi_html(context) -> str:
    cfg: FetchHtmlConfig = FetchHtmlConfig(**context.op_config)  # type: ignore[arg-type]
    response = requests.get(cfg.url, timeout=30)
    response.raise_for_status()
    output_path = Path(cfg.output_path)
    output_path.write_text(response.text, encoding="utf-8")
    context.log.info(f"Saved HTML to {output_path.resolve()}")
    return str(output_path)


@op(
    config_schema=ExtractValidateConfig.to_dict(),
    ins={"html_path": In(String)},
    out=Out(Dict[str, Any]),
    description="Parses the HTML, validates freshness, checks duplicates, and outputs JSON.",
)
def extract_and_validate_data(context, html_path: str) -> Dict[str, Any]:
    cfg: ExtractValidateConfig = ExtractValidateConfig(**context.op_config)  # type: ignore[arg-type]
    html_content = Path(html_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(html_content, "html.parser")

    # Example extraction logic – adapt to actual page structure.
    aqi_tag = soup.find(class_="aqi-value")
    if not aqi_tag:
        raise ValueError("AQI value not found in the HTML.")
    aqi_value = int(aqi_tag.text.strip())

    data = {
        "aqi": aqi_value,
        "source_url": context.op_config["url"] if "url" in context.op_config else "",
    }

    # Duplicate check stub – in real code query the DB.
    if cfg.duplicate_check:
        # Placeholder: assume no duplicate.
        context.log.info("Duplicate check passed (stub).")

    json_path = Path(cfg.json_output_path)
    json_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    context.log.info(f"Extracted data saved to {json_path.resolve()}")
    return data


@op(
    config_schema=LoadPostgresConfig.to_dict(),
    ins={"data": In(Dict[str, Any])},
    required_resource_keys={"postgres"},
    description="Loads the JSON data into PostgreSQL tables.",
)
def load_data_to_postgres(context, data: Dict[str, Any]) -> bool:
    conn_str: str = context.resources.postgres
    # Stub implementation – replace with actual DB logic.
    context.log.info(f"Connecting to PostgreSQL with conn_str: {conn_str}")
    context.log.info(f"Inserting data: {data}")
    # Assume success.
    return True


@op(
    config_schema=EmailAlertConfig.to_dict(),
    ins={"data": In(Dict[str, Any])},
    required_resource_keys={"email"},
    description="Sends an email alert if AQI exceeds the configured threshold.",
)
def conditional_email_alert(context, data: Dict[str, Any]) -> bool:
    cfg: EmailAlertConfig = EmailAlertConfig(**context.op_config)  # type: ignore[arg-type]
    aqi = data.get("aqi")
    if aqi is None:
        context.log.error("AQI value missing from data; cannot evaluate alert condition.")
        return False

    if aqi <= cfg.aqi_threshold:
        context.log.info(f"AQI {aqi} is within safe limits (threshold {cfg.aqi_threshold}). No email sent.")
        return False

    email_cfg = context.resources.email
    message = f"""Subject: {cfg.subject}
To: {", ".join(email_cfg["recipients"])}
{cfg.body_template.format(aqi=aqi, threshold=cfg.aqi_threshold)}"""

    context.log.info("Preparing to send email alert.")
    try:
        context.log.info(
            f"Connecting to SMTP server {email_cfg['smtp_server']}:{email_cfg['smtp_port']}"
        )
        context.log.info(f"Authenticating as {email_cfg['username']}")
        context.log.info(f"Sending alert to {email_cfg['recipients']}")
        # Using SSL context for secure connection.
        ssl_context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            email_cfg["smtp_server"], email_cfg["smtp_port"], context=ssl_context
        ) as server:
            server.login(email_cfg["username"], email_cfg["password"])
            server.sendmail(
                email_cfg["username"],
                email_cfg["recipients"],
                message.encode("utf-8"),
            )
        context.log.info("Email alert sent successfully.")
        return True
    except Exception as exc:  # pylint: disable=broad-except
        context.log.error(f"Failed to send email alert: {exc}")
        return False


@job(
    resource_defs={"postgres": postgres_resource, "email": email_resource},
    description="ETL pipeline that fetches Mahidol AQI data, loads it into PostgreSQL, and sends alerts.",
)
def mahidol_aqi_etl_job():
    html_path = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_path)
    load_success = load_data_to_postgres(extracted_data)
    # The email alert runs regardless of load_success; adjust if needed.
    conditional_email_alert(extracted_data)


if __name__ == "__main__":
    result = mahidol_aqi_etl_job.execute_in_process(
        run_config={
            "ops": {
                "fetch_mahidol_aqi_html": {
                    "config": {
                        "url": "https://example.com/mahidol_aqi",
                        "output_path": "mahidol_aqi.html",
                    }
                },
                "extract_and_validate_data": {
                    "config": {
                        "duplicate_check": True,
                        "json_output_path": "mahidol_aqi.json",
                    }
                },
                "load_data_to_postgres": {"config": {}},
                "conditional_email_alert": {
                    "config": {
                        "aqi_threshold": 100,
                        "subject": "Air Quality Alert",
                        "body_template": "Current AQI is {aqi}, which exceeds the safe threshold of {threshold}.",
                    }
                },
            },
            "resources": {
                "postgres": {"config": {"conn_str": "postgresql://user:pass@localhost/dbname"}},
                "email": {
                    "config": {
                        "smtp_server": "smtp.gmail.com",
                        "smtp_port": 465,
                        "username": "your_email@gmail.com",
                        "password": "your_app_password",
                        "recipients": ["recipient1@example.com", "recipient2@example.com"],
                    }
                },
            },
        }
    )
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")