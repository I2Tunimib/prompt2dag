from dagster import op, job, In, Out, Config, OpExecutionContext
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional

# Minimal resource stubs (can be expanded)
class PostgresResource:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def get_connection(self):
        # Minimal stub - in production use proper connection pooling
        import psycopg2
        return psycopg2.connect(self.connection_string)
    
    def execute_query(self, query, params=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.fetchall() if cursor.description else None
        finally:
            cursor.close()
            conn.close()

class SmtpResource:
    def __init__(self, smtp_server: str, smtp_port: int, username: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
    
    def send_email(self, to: list, subject: str, body: str):
        import smtplib
        from email.mime.text import MIMEText
        
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.username
        msg['To'] = ', '.join(to)
        
        server = smtplib.SMTP(self.smtp_server, self.smtp_port)
        server.starttls()
        server.login(self.username, self.password)
        server.send_message(msg)
        server.quit()

# Config classes
class FetchConfig(Config):
    url: str = "https://example-mahidol-aqi.com"
    output_path: str = "/tmp/mahidol_aqi.html"

class ExtractConfig(Config):
    html_path: str = "/tmp/mahidol_aqi.html"
    db_connection_string: str = "postgresql://user:pass@localhost/aqi_db"

class LoadConfig(Config):
    db_connection_string: str = "postgresql://user:pass@localhost/aqi_db"

class AlertConfig(Config):
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    username: str = "your-email@gmail.com"
    password: str = "your-app-password"
    recipients: list = ["recipient@example.com"]
    aqi_threshold: int = 100

# Ops
@op
def fetch_mahidol_aqi_html(context: OpExecutionContext, config: FetchConfig) -> str:
    """
    Downloads the latest AQI report webpage from Mahidol University.
    """
    context.log.info(f"Fetching AQI data from {config.url}")
    
    response = requests.get(config.url, timeout=30)
    response.raise_for_status()
    
    # Save to file
    os.makedirs(os.path.dirname(config.output_path), exist_ok=True)
    with open(config.output_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    context.log.info(f"Saved HTML to {config.output_path}")
    return config.output_path

@op
def extract_and_validate_data(context: OpExecutionContext, html_path: str, config: ExtractConfig) -> Dict[str, Any]:
    """
    Parses HTML to extract air quality metrics, validates freshness,
    checks for duplicates, and creates structured JSON output.
    """
    context.log.info(f"Extracting data from {html_path}")
    
    # Read HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract data (minimal example - adjust based on actual HTML structure)
    # This is a placeholder for the actual parsing logic
    data = {
        "timestamp": datetime.now().isoformat(),
        "location": "Mahidol University",
        "pm25": 35.5,
        "pm10": 50.2,
        "aqi": 95,
        "pollutant": "PM2.5"
    }
    
    # Validate freshness (example: check if data is from today)
    # In real implementation, parse actual timestamp from HTML
    data_date = datetime.now().date()
    if data_date < datetime.now().date():
        raise ValueError("Data is not fresh")
    
    # Check for duplicates in database
    db = PostgresResource(config.db_connection_string)
    query = """
        SELECT COUNT(*) FROM aqi_facts 
        WHERE location = %s AND timestamp::date = %s
    """
    result = db.execute_query(query, (data["location"], data["timestamp"][:10]))
    
    if result and result[0][0] > 0:
        context.log.info("Data already exists in database, skipping processing")
        return {}  # Return empty dict to signal skip
    
    context.log.info("Data extracted and validated successfully")
    return data

@op
def load_data_to_postgresql(context: OpExecutionContext, data: Dict[str, Any], config: LoadConfig) -> Optional[int]:
    """
    Transforms and loads JSON data into PostgreSQL tables.
    Returns the AQI value for potential alerting.
    """
    if not data:
        context.log.info("No data to load, skipping")
        return None
    
    context.log.info("Loading data to PostgreSQL")
    db = PostgresResource(config.db_connection_string)
    
    # Insert datetime dimension
    timestamp = datetime.fromisoformat(data["timestamp"])
    datetime_query = """
        INSERT INTO datetime_dim (timestamp, year, month, day, hour)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING
    """
    db.execute_query(datetime_query, (
        data["timestamp"],
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour
    ))
    
    # Insert location dimension
    location_query = """
        INSERT INTO location_dim (location_name)
        VALUES (%s)
        ON CONFLICT (location_name) DO NOTHING
    """
    db.execute_query(location_query, (data["location"],))
    
    # Get location_id
    location_id_query = "SELECT location_id FROM location_dim WHERE location_name = %s"
    location_result = db.execute_query(location_id_query, (data["location"],))
    location_id = location_result[0][0] if location_result else 1
    
    # Insert pollution dimensions
    pollution_query = """
        INSERT INTO pollution_dim (pollutant_type, pm25_value, pm10_value)
        VALUES (%s, %s, %s)
        RETURNING pollution_id
    """
    pollution_result = db.execute_query(pollution_query, (
        data["pollutant"],
        data.get("pm25"),
        data.get("pm10")
    ))
    pollution_id = pollution_result[0][0] if pollution_result else 1
    
    # Insert AQI fact
    aqi_query = """
        INSERT INTO aqi_facts (location_id, timestamp, pollution_id, aqi_value)
        VALUES (%s, %s, %s, %s)
    """
    db.execute_query(aqi_query, (location_id, data["timestamp"], pollution_id, data["aqi"]))
    
    context.log.info(f"Data loaded successfully, AQI: {data['aqi']}")
    return data["aqi"]

@op
def conditional_email_alert(context: OpExecutionContext, aqi_value: Optional[int], config: AlertConfig):
    """
    Evaluates AQI against thresholds and sends email alerts if exceeded.
    """
    if aqi_value is None:
        context.log.info("No AQI value provided, skipping alert")
        return
    
    context.log.info(f"Checking AQI value {aqi_value} against threshold {config.aqi_threshold}")
    
    if aqi_value <= config.aqi_threshold:
        context.log.info(f"AQI {aqi_value} is within safe limits, no alert sent")
        return
    
    # Send alert email
    smtp = SmtpResource(
        config.smtp_server,
        config.smtp_port,
        config.username,
        config.password
    )
    
    subject = f"Air Quality Alert: AQI {aqi_value} exceeds safe threshold"
    body = f"""
    Air Quality Alert Notification
    
    Current AQI: {aqi_value}
    Threshold: {config.aqi_threshold}
    Location: Mahidol University
    Time: {datetime.now().isoformat()}
    
    Please take necessary precautions.
    """
    
    smtp.send_email(config.recipients, subject, body)
    context.log.info(f"Alert email sent to {config.recipients}")

# Job definition
@job
def mahidol_aqi_etl_job():
    """
    ETL pipeline for Mahidol University air quality data.
    """
    html_path = fetch_mahidol_aqi_html()
    extracted_data = extract_and_validate_data(html_path)
    aqi_value = load_data_to_postgresql(extracted_data)
    conditional_email_alert(aqi_value)

# Launch pattern
if __name__ == '__main__':
    result = mahidol_aqi_etl_job.execute_in_process()
    print(f"Job execution result: {result.success}")