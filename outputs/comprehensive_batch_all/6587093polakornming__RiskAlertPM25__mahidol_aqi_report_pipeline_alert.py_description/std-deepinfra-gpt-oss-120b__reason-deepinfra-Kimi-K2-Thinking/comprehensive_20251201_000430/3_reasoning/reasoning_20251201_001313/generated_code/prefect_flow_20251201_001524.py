import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
import psycopg2
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner  # For explicit sequential execution

# Configuration (could be loaded from files as mentioned)
CONFIG = {
    "mahidol_url": "https://example-mahidol-aqi.com",  # Replace with actual URL
    "html_output_path": "/tmp/mahidol_aqi.html",
    "json_output_path": "/tmp/mahidol_aqi.json",
    "db_config": {
        "host": "localhost",
        "database": "aqi_db",
        "user": "aqi_user",
        "password": "aqi_password",
        "port": 5432
    },
    "email_config": {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "alerts@example.com",
        "sender_password": "app_password",
        "recipients": ["recipient@example.com"]
    },
    "aqi_threshold": 100  # Alert if AQI > 100
}

@task
def fetch_mahidol_aqi_html(url: str, output_path: str) -> Optional[str]:
    """Fetch AQI HTML from Mahidol University and save to file."""
    logger = get_run_logger()
    try:
        logger.info(f"Fetching AQI data from {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        Path(output_path).write_text(response.text, encoding='utf-8')
        logger.info(f"Saved HTML to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to fetch HTML: {e}")
        raise

@task
def extract_and_validate_data(
    html_path: str, 
    db_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Extract and validate AQI data from HTML, checking for duplicates."""
    logger = get_run_logger()
    try:
        # Check if file exists and has content
        html_content = Path(html_path).read_text(encoding='utf-8')
        if not html_content:
            logger.warning("HTML file is empty")
            return None
        
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract data (this is a placeholder - actual parsing depends on HTML structure)
        # Assuming the page has a table with AQI data
        data = {
            "timestamp": "2024-01-01T12:00:00",  # Extract from HTML
            "location": "Mahidol University",
            "aqi": 85,  # Extract from HTML
            "pm25": 25.5,
            "pm10": 45.2,
            "o3": 60.1,
            "no2": 30.5,
            "so2": 10.2,
            "co": 0.8
        }
        
        # Validate data freshness (example: check if timestamp is recent)
        # This would involve parsing the timestamp and comparing to current time
        logger.info(f"Extracted data: {data}")
        
        # Check for duplicates in database
        if check_duplicate_in_db(data, db_config):
            logger.info("Data already exists in database, skipping")
            return None
        
        # Save to JSON file
        json_path = CONFIG["json_output_path"]
        Path(json_path).write_text(json.dumps(data, indent=2), encoding='utf-8')
        logger.info(f"Saved validated data to {json_path}")
        
        return data
    except Exception as e:
        logger.error(f"Failed to extract and validate data: {e}")
        raise

def check_duplicate_in_db(data: Dict[str, Any], db_config: Dict[str, Any]) -> bool:
    """Helper function to check if data already exists in database."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Check if record with same timestamp and location exists
        query = """
            SELECT 1 FROM aqi_facts 
            JOIN datetime_dim ON aqi_facts.datetime_id = datetime_dim.id
            JOIN location_dim ON aqi_facts.location_id = location_dim.id
            WHERE datetime_dim.timestamp = %s AND location_dim.name = %s
        """
        cursor.execute(query, (data["timestamp"], data["location"]))
        exists = cursor.fetchone() is not None
        
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        # If check fails, assume not duplicate to avoid skipping valid data
        get_run_logger().warning(f"Duplicate check failed: {e}")
        return False

@task
def load_data_to_postgresql(
    data: Optional[Dict[str, Any]], 
    db_config: Dict[str, Any]
) -> bool:
    """Load validated AQI data into PostgreSQL database."""
    logger = get_run_logger()
    
    if data is None:
        logger.info("No data to load, skipping")
        return False
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Insert into datetime_dim
        cursor.execute(
            "INSERT INTO datetime_dim (timestamp) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id",
            (data["timestamp"],)
        )
        datetime_result = cursor.fetchone()
        if datetime_result:
            datetime_id = datetime_result[0]
        else:
            cursor.execute("SELECT id FROM datetime_dim WHERE timestamp = %s", (data["timestamp"],))
            datetime_id = cursor.fetchone()[0]
        
        # Insert into location_dim
        cursor.execute(
            "INSERT INTO location_dim (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id",
            (data["location"],)
        )
        location_result = cursor.fetchone()
        if location_result:
            location_id = location_result[0]
        else:
            cursor.execute("SELECT id FROM location_dim WHERE name = %s", (data["location"],))
            location_id = cursor.fetchone()[0]
        
        # Insert into pollution_dim
        cursor.execute(
            """
            INSERT INTO pollution_dim (pm25, pm10, o3, no2, so2, co)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING RETURNING id
            """,
            (data["pm25"], data["pm10"], data["o3"], data["no2"], data["so2"], data["co"])
        )
        pollution_result = cursor.fetchone()
        if pollution_result:
            pollution_id = pollution_result[0]
        else:
            # For simplicity, get the latest pollution record with matching values
            cursor.execute(
                "SELECT id FROM pollution_dim WHERE pm25 = %s AND pm10 = %s LIMIT 1",
                (data["pm25"], data["pm10"])
            )
            pollution_id = cursor.fetchone()[0]
        
        # Insert into aqi_facts
        cursor.execute(
            """
            INSERT INTO aqi_facts (datetime_id, location_id, pollution_id, aqi_value)
            VALUES (%s, %s, %s, %s)
            """,
            (datetime_id, location_id, pollution_id, data["aqi"])
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully loaded AQI data: {data['aqi']}")
        return True
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {e}")
        raise

@task
def conditional_email_alert(
    data: Optional[Dict[str, Any]], 
    threshold: int, 
    email_config: Dict[str, Any]
) -> bool:
    """Send email alert if AQI exceeds threshold."""
    logger = get_run_logger()
    
    if data is None:
        logger.info("No data to evaluate, skipping alert")
        return False
    
    aqi_value = data.get("aqi", 0)
    if aqi_value <= threshold:
        logger.info(f"AQI {aqi_value} is within safe threshold ({threshold})")
        return False
    
    try:
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = email_config["sender_email"]
        msg["To"] = ", ".join(email_config["recipients"])
        msg["Subject"] = f"Air Quality Alert: AQI {aqi_value} Exceeds Safe Level"
        
        body = f"""
        Air Quality Alert
        
        Location: {data['location']}
        Timestamp: {data['timestamp']}
        Current AQI: {aqi_value}
        Threshold: {threshold}
        
        Pollutant Levels:
        - PM2.5: {data['pm25']} µg/m³
        - PM10: {data['pm10']} µg/m³
        - O3: {data['o3']} ppb
        - NO2: {data['no2']} ppb
        - SO2: {data['so2']} ppb
        - CO: {data['co']} ppm
        """
        
        msg.attach(MIMEText(body, "plain"))
        
        # Send email via SMTP
        server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
        server.starttls()
        server.login(email_config["sender_email"], email_config["sender_password"])
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Alert email sent for AQI {aqi_value}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        raise

@flow(name="mahidol-aqi-etl", task_runner=SequentialTaskRunner())
def mahidol_aqi_etl_flow(config: Dict[str, Any] = CONFIG):
    """Main ETL flow for Mahidol University AQI data."""
    logger = get_run_logger()
    logger.info("Starting Mahidol AQI ETL pipeline")
    
    # Step 1: Fetch HTML
    html_path = fetch_mahidol_aqi_html(
        config["mahidol_url"], 
        config["html_output_path"]
    )
    
    # Step 2: Extract and validate (returns None if duplicate or invalid)
    validated_data = extract_and_validate_data(html_path, config["db_config"])
    
    # Step 3: Load to PostgreSQL (skips if data is None)
    load_success = load_data_to_postgresql(validated_data, config["db_config"])
    
    # Step 4: Conditional email alert (skips if data is None or AQI safe)
    email_sent = conditional_email_alert(
        validated_data, 
        config["aqi_threshold"], 
        config["email_config"]
    )
    
    logger.info(
        f"Pipeline completed - Load success: {load_success}, Email sent: {email_sent}"
    )
    
    return {
        "html_path": html_path,
        "data": validated_data,
        "load_success": load_success,
        "email_sent": email_sent
    }

if __name__ == "__main__":
    # For local execution
    mahidol_aqi_etl_flow()
    
    # For deployment with schedule (commented example)
    # from prefect.deployments import Deployment
    # from prefect.server.schemas.schedules import CronSchedule
    # 
    # Deployment.build_from_flow(
    #     flow=mahidol_aqi_etl_flow,
    #     name="mahidol-aqi-daily",
    #     schedule=CronSchedule(cron="0 6 * * *"),  # Daily at 6 AM
    #     work_pool_name="default"
    # )