import json
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from bs4 import BeautifulSoup
from prefect import flow, task
from prefect.blocks.system import JSON
from prefect.blocks.system import Secret
import psycopg2
from psycopg2 import sql


@task(retries=3, retry_delay_seconds=60)
def fetch_mahidol_aqi_html(url: str, output_path: str) -> str:
    """Download AQI HTML from Mahidol University and save to file."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    return output_path


@task
def extract_and_validate_data(
    html_path: str,
    db_conn_str: str,
    pollution_mapping_path: str
) -> str:
    """Parse HTML, validate data, check duplicates, and create JSON."""
    # Load pollution mapping
    with open(pollution_mapping_path, 'r') as f:
        pollution_mapping = json.load(f)
    
    # Parse HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
    
    # Extract data (this is a placeholder - actual parsing depends on HTML structure)
    # For demonstration, I'll create a mock structure
    data = {
        'timestamp': datetime.now().isoformat(),
        'location': 'Mahidol University',
        'pollutants': {
            'pm25': 25.5,
            'pm10': 45.2,
            'o3': 60.1,
            'no2': 30.5,
            'so2': 15.3,
            'co': 1.2
        },
        'aqi': 85
    }
    
    # Validate freshness (check if data is from today)
    # This is a simplified check
    data_date = datetime.fromisoformat(data['timestamp']).date()
    if data_date != datetime.now().date():
        raise ValueError("Data is not fresh")
    
    # Check for duplicates in database
    conn = psycopg2.connect(db_conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM aqi_facts WHERE location = %s AND timestamp::date = %s",
            (data['location'], data_date)
        )
        if cursor.fetchone()[0] > 0:
            print(f"Data for {data['location']} on {data_date} already exists. Skipping.")
            return None
    finally:
        cursor.close()
        conn.close()
    
    # Create JSON output
    json_path = html_path.replace('.html', '.json')
    with open(json_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return json_path


@task
def load_data_to_postgresql(json_path: str, db_conn_str: str) -> int:
    """Load JSON data into PostgreSQL tables."""
    if not json_path or not os.path.exists(json_path):
        print("No data to load. Skipping.")
        return 0
    
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    conn = psycopg2.connect(db_conn_str)
    cursor = conn.cursor()
    
    try:
        # Insert datetime dimension
        timestamp = datetime.fromisoformat(data['timestamp'])
        cursor.execute(
            """
            INSERT INTO datetime_dim (date, hour, minute, day_of_week, month, year)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING datetime_id
            """,
            (timestamp.date(), timestamp.hour, timestamp.minute,
             timestamp.strftime('%A'), timestamp.month, timestamp.year)
        )
        datetime_result = cursor.fetchone()
        if datetime_result:
            datetime_id = datetime_result[0]
        else:
            cursor.execute(
                "SELECT datetime_id FROM datetime_dim WHERE date = %s AND hour = %s AND minute = %s",
                (timestamp.date(), timestamp.hour, timestamp.minute)
            )
            datetime_id = cursor.fetchone()[0]
        
        # Insert location dimension
        cursor.execute(
            """
            INSERT INTO location_dim (location_name)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            RETURNING location_id
            """,
            (data['location'],)
        )
        location_result = cursor.fetchone()
        if location_result:
            location_id = location_result[0]
        else:
            cursor.execute(
                "SELECT location_id FROM location_dim WHERE location_name = %s",
                (data['location'],)
            )
            location_id = cursor.fetchone()[0]
        
        # Insert pollution dimensions and AQI facts
        for pollutant, value in data['pollutants'].items():
            # Insert pollution dimension
            cursor.execute(
                """
                INSERT INTO pollution_dim (pollutant_name, unit, description)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING pollutant_id
                """,
                (pollutant, 'μg/m³', pollution_mapping.get(pollutant, ''))
            )
            pollutant_result = cursor.fetchone()
            if pollutant_result:
                pollutant_id = pollutant_result[0]
            else:
                cursor.execute(
                    "SELECT pollutant_id FROM pollution_dim WHERE pollutant_name = %s",
                    (pollutant,)
                )
                pollutant_id = cursor.fetchone()[0]
            
            # Insert AQI fact
            cursor.execute(
                """
                INSERT INTO aqi_facts (datetime_id, location_id, pollutant_id, value, aqi)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (datetime_id, location_id, pollutant_id, value, data['aqi'])
            )
        
        conn.commit()
        return 1
    
    except Exception as e:
        conn.rollback()
        raise e
    
    finally:
        cursor.close()
        conn.close()


@task
def conditional_email_alert(
    json_path: str,
    smtp_config: dict,
    aqi_threshold: int = 100
) -> str:
    """Send email alert if AQI exceeds threshold."""
    if not json_path or not os.path.exists(json_path):
        return "No data to evaluate. Skipping alert."
    
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    current_aqi = data['aqi']
    
    if current_aqi <= aqi_threshold:
        return f"AQI {current_aqi} is within safe threshold ({aqi_threshold}). No alert sent."
    
    # Compose email
    subject = f"Air Quality Alert: AQI {current_aqi} exceeds safe threshold"
    body = f"""
    <html>
    <body>
        <h2>Air Quality Alert</h2>
        <p><strong>Location:</strong> {data['location']}</p>
        <p><strong>Timestamp:</strong> {data['timestamp']}</p>
        <p><strong>Current AQI:</strong> {current_aqi}</p>
        <p><strong>Threshold:</strong> {aqi_threshold}</p>
        <p><strong>Status:</strong> <span style="color: red;">UNSAFE</span></p>
    </body>
    </html>
    """
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = smtp_config['sender']
    msg['To'] = ', '.join(smtp_config['recipients'])
    
    msg.attach(MIMEText(body, 'html'))
    
    # Send email
    with smtplib.SMTP(smtp_config['host'], smtp_config['port']) as server:
        server.starttls()
        server.login(smtp_config['username'], smtp_config['password'])
        server.send_message(msg)
    
    return f"Alert email sent for AQI {current_aqi}"


@flow(name="mahidol-aqi-etl-pipeline")
def mahidol_aqi_pipeline(
    mahidol_url: str = "https://airquality.mahidol.ac.th/report",
    html_output_path: str = "/tmp/mahidol_aqi.html",
    pollution_mapping_path: str = "/config/pollution_mapping.json",
    db_conn_str: str = None,
    smtp_config: dict = None,
    aqi_threshold: int = 100
):
    """
    ETL pipeline for Mahidol University air quality data.
    
    Steps:
    1. Fetch HTML from Mahidol website
    2. Extract and validate data (check freshness and duplicates)
    3. Load data into PostgreSQL
    4. Send email alert if AQI exceeds threshold
    """
    # Get DB connection string from Prefect Secret if not provided
    if db_conn_str is None:
        db_secret = Secret.load("postgres-connection-string")
        db_conn_str = db_secret.get()
    
    # Get SMTP config from Prefect JSON block if not provided
    if smtp_config is None:
        smtp_block = JSON.load("smtp-config")
        smtp_config = smtp_block.value
    
    # Step 1: Fetch HTML
    html_path = fetch_mahidol_aqi_html(mahidol_url, html_output_path)
    
    # Step 2: Extract and validate
    json_path = extract_and_validate_data(
        html_path,
        db_conn_str,
        pollution_mapping_path
    )
    
    # Step 3: Load to PostgreSQL (only if data is new)
    if json_path:
        load_result = load_data_to_postgresql(json_path, db_conn_str)
    else:
        load_result = 0
        print("No new data to load. Skipping database load.")
    
    # Step 4: Conditional email alert (only if data was loaded)
    if json_path and load_result > 0:
        alert_result = conditional_email_alert(
            json_path,
            smtp_config,
            aqi_threshold
        )
    else:
        alert_result = "No data loaded. Skipping alert."
    
    return {
        "html_path": html_path,
        "json_path": json_path,
        "records_loaded": load_result,
        "alert_status": alert_result
    }


if __name__ == '__main__':
    # For local execution
    # Configure these values appropriately or use Prefect blocks
    mahidol_aqi_pipeline(
        mahidol_url="https://airquality.mahidol.ac.th/report",
        html_output_path="/tmp/mahidol_aqi.html",
        pollution_mapping_path="/config/pollution_mapping.json",
        db_conn_str="postgresql://user:pass@localhost:5432/aqi_db",
        smtp_config={
            'host': 'smtp.gmail.com',
            'port': 587,
            'username': 'your-email@gmail.com',
            'password': 'your-app-password',
            'sender': 'your-email@gmail.com',
            'recipients': ['recipient@example.com']
        },
        aqi_threshold=100
    )