from dagster import op, job, Config, OpExecutionContext, get_dagster_logger
import requests
from bs4 import BeautifulSoup
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, Any, Optional

# Config classes
class MahidolAQIConfig(Config):
    url: str = "https://example-mahidol-aqi.com"  # Replace with actual URL
    html_output_path: str = "/tmp/mahidol_aqi.html"
    json_output_path: str = "/tmp/mahidol_aqi.json"
    pollution_mapping_path: str = "/tmp/pollution_mapping.json"
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "air_quality"
    db_user: str = "postgres"
    db_password: str = "password"
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = "your-email@gmail.com"
    smtp_password: str = "your-app-password"
    email_recipients_path: str = "/tmp/email_recipients.json"
    aqi_threshold: int = 100

# Resources (minimal stubs)
# In a real implementation, you'd create proper Dagster resources
# For this example, we'll use config directly in ops

@op
def fetch_mahidol_aqi_html(context: OpExecutionContext, config: MahidolAQIConfig) -> str:
    """
    Downloads the latest AQI report webpage from Mahidol University
    and saves it as an HTML file.
    """
    logger = get_dagster_logger()
    logger.info(f"Fetching AQI data from {config.url}")
    
    try:
        response = requests.get(config.url, timeout=30)
        response.raise_for_status()
        
        # Save HTML to file
        os.makedirs(os.path.dirname(config.html_output_path), exist_ok=True)
        with open(config.html_output_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        logger.info(f"HTML saved to {config.html_output_path}")
        return config.html_output_path
    except Exception as e:
        logger.error(f"Failed to fetch HTML: {e}")
        raise

@op
def extract_and_validate_data(
    context: OpExecutionContext, 
    config: MahidolAQIConfig,
    html_path: str
) -> Optional[str]:
    """
    Parses HTML to extract air quality metrics, validates data freshness,
    checks for duplicates against the database, and creates structured JSON.
    Returns JSON file path if data is new and valid, None if duplicate.
    """
    logger = get_dagster_logger()
    logger.info(f"Extracting data from {html_path}")
    
    try:
        # Read HTML
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract data (placeholder logic - adapt to actual HTML structure)
        # This is a minimal example that creates a plausible data structure
        data = {
            "timestamp": datetime.now().isoformat(),
            "location": "Mahidol University",
            "aqi": 85,  # Placeholder - would extract from HTML
            "pollutants": {
                "pm25": 25.5,
                "pm10": 35.2,
                "o3": 45.1,
                "no2": 22.3,
                "so2": 10.5,
                "co": 0.8
            }
        }
        
        # Validate freshness (example: check if timestamp is recent)
        # In real implementation, compare with current time
        logger.info(f"Data timestamp: {data['timestamp']}")
        
        # Check for duplicates against database
        # Minimal stub: simulate duplicate check
        # In real implementation, query PostgreSQL
        is_duplicate = False  # Placeholder
        
        if is_duplicate:
            logger.info("Data is duplicate, skipping further processing")
            return None
        
        # Save JSON
        os.makedirs(os.path.dirname(config.json_output_path), exist_ok=True)
        with open(config.json_output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"JSON saved to {config.json_output_path}")
        return config.json_output_path
        
    except Exception as e:
        logger.error(f"Failed to extract and validate data: {e}")
        raise

@op
def load_data_to_postgresql(
    context: OpExecutionContext,
    config: MahidolAQIConfig,
    json_path: Optional[str]
) -> Optional[int]:
    """
    Transforms and loads JSON data into PostgreSQL tables.
    Returns AQI value if data was loaded, None if no data to load.
    """
    logger = get_dagster_logger()
    
    if json_path is None:
        logger.info("No JSON data to load, skipping")
        return None
    
    logger.info(f"Loading data from {json_path} to PostgreSQL")
    
    try:
        # Read JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Connect to PostgreSQL
        import psycopg2
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            dbname=config.db_name,
            user=config.db_user,
            password=config.db_password
        )
        cursor = conn.cursor()
        
        # Load data into tables (minimal example)
        # In real implementation, use proper SQL and handle transactions
        
        # Insert datetime dimension
        cursor.execute(
            "INSERT INTO datetime_dim (timestamp) VALUES (%s) ON CONFLICT DO NOTHING",
            (data["timestamp"],)
        )
        
        # Insert location dimension
        cursor.execute(
            "INSERT INTO location_dim (location_name) VALUES (%s) ON CONFLICT DO NOTHING",
            (data["location"],)
        )
        
        # Insert pollution facts
        cursor.execute(
            """
            INSERT INTO aqi_facts 
            (timestamp, location, aqi, pm25, pm10, o3, no2, so2, co)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["timestamp"],
                data["location"],
                data["aqi"],
                data["pollutants"]["pm25"],
                data["pollutants"]["pm10"],
                data["pollutants"]["o3"],
                data["pollutants"]["no2"],
                data["pollutants"]["so2"],
                data["pollutants"]["co"]
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Data loaded to PostgreSQL successfully")
        return data["aqi"]
        
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {e}")
        raise

@op
def conditional_email_alert(
    context: OpExecutionContext,
    config: MahidolAQIConfig,
    aqi_value: Optional[int]
) -> None:
    """
    Evaluates AQI value against thresholds and sends email alerts
    if air quality exceeds safe levels.
    """
    logger = get_dagster_logger()
    
    if aqi_value is None:
        logger.info("No AQI value to evaluate, skipping email alert")
        return
    
    logger.info(f"Evaluating AQI value: {aqi_value}")
    
    if aqi_value <= config.aqi_threshold:
        logger.info(f"AQI {aqi_value} is within safe threshold {config.aqi_threshold}, no alert needed")
        return
    
    # AQI exceeds threshold, send alert
    logger.warning(f"AQI {aqi_value} exceeds threshold {config.aqi_threshold}, sending alert")
    
    try:
        # Load email recipients
        with open(config.email_recipients_path, 'r', encoding='utf-8') as f:
            recipients = json.load(f)
        
        if not recipients:
            logger.warning("No email recipients configured")
            return
        
        # Compose email
        subject = f"Air Quality Alert: AQI {aqi_value} Exceeds Safe Threshold"
        body = f"""
        Air Quality Alert
        
        Current AQI: {aqi_value}
        Threshold: {config.aqi_threshold}
        Location: Mahidol University
        Time: {datetime.now().isoformat()}
        
        Please take necessary precautions.
        """
        
        msg = MIMEMultipart()
        msg['From'] = config.smtp_user
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email via SMTP
        server = smtplib.SMTP(config.smtp_host, config.smtp_port)
        server.starttls()
        server.login(config.smtp_user, config.smtp_password)
        text = msg.as_string()
        server.sendmail(config.smtp_user, recipients, text)
        server.quit()
        
        logger.info(f"Alert email sent to {len(recipients)} recipients")
        
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        # Don't raise - alert failure shouldn't fail the entire pipeline

@job
def mahidol_aqi_etl_job():
    """
    ETL pipeline for Mahidol University air quality data.
    Linear execution: fetch -> extract/validate -> load -> alert
    """
    # Define the dependency graph
    html_path = fetch_mahidol_aqi_html()
    json_path = extract_and_validate_data(html_path)
    aqi_value = load_data_to_postgresql(json_path)
    conditional_email_alert(aqi_value)

if __name__ == '__main__':
    # Minimal launch pattern
    result = mahidol_aqi_etl_job.execute_in_process(
        run_config={
            "ops": {
                "fetch_mahidol_aqi_html": {
                    "config": {
                        "url": "https://example-mahidol-aqi.com",
                        "html_output_path": "/tmp/mahidol_aqi.html",
                        "json_output_path": "/tmp/mahidol_aqi.json",
                        "pollution_mapping_path": "/tmp/pollution_mapping.json",
                        "db_host": "localhost",
                        "db_port": 5432,
                        "db_name": "air_quality",
                        "db_user": "postgres",
                        "db_password": "password",
                        "smtp_host": "smtp.gmail.com",
                        "smtp_port": 587,
                        "smtp_user": "your-email@gmail.com",
                        "smtp_password": "your-app-password",
                        "email_recipients_path": "/tmp/email_recipients.json",
                        "aqi_threshold": 100
                    }
                }
            }
        }
    )