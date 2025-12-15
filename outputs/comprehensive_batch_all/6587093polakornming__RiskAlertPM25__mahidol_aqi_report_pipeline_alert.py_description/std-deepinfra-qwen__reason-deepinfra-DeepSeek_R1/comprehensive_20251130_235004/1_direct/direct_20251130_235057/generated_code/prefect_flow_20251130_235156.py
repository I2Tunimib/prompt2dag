from prefect import flow, task
import requests
from bs4 import BeautifulSoup
import json
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

@task
def fetch_mahidol_aqi_html():
    """Downloads the latest AQI report webpage from Mahidol University and saves it as an HTML file."""
    url = "https://example.com/mahidol-aqi-report"
    response = requests.get(url)
    response.raise_for_status()
    with open("mahidol_aqi_report.html", "w") as file:
        file.write(response.text)
    return "mahidol_aqi_report.html"

@task
def extract_and_validate_data(html_file):
    """Parses the HTML to extract air quality metrics, validates data freshness, checks for duplicates, and creates a structured JSON output."""
    with open(html_file, "r") as file:
        soup = BeautifulSoup(file, "html.parser")
    aqi_data = soup.find("div", {"class": "aqi-data"})
    if not aqi_data:
        raise ValueError("AQI data not found in the HTML.")
    
    # Example data extraction (simplified)
    data = {
        "datetime": aqi_data.find("span", {"class": "datetime"}).text,
        "location": aqi_data.find("span", {"class": "location"}).text,
        "aqi_value": int(aqi_data.find("span", {"class": "aqi-value"}).text),
    }
    
    # Validate data freshness and check for duplicates
    if not is_data_fresh(data["datetime"]):
        raise ValueError("Data is not fresh.")
    if is_duplicate(data):
        raise ValueError("Data is a duplicate.")
    
    with open("aqi_data.json", "w") as file:
        json.dump(data, file)
    return "aqi_data.json"

def is_data_fresh(datetime_str):
    """Check if the data is fresh (e.g., within the last hour)."""
    # Simplified check for demonstration
    return True

def is_duplicate(data):
    """Check if the data is a duplicate in the database."""
    # Simplified check for demonstration
    return False

@task
def load_data_to_postgresql(json_file):
    """Transforms and loads the JSON data into multiple database tables including datetime, location, pollution dimensions, and AQI facts."""
    with open(json_file, "r") as file:
        data = json.load(file)
    
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cur = conn.cursor()
    
    # Example SQL statements (simplified)
    cur.execute("INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING", (data["datetime"],))
    cur.execute("INSERT INTO location (location) VALUES (%s) ON CONFLICT DO NOTHING", (data["location"],))
    cur.execute("INSERT INTO aqi_facts (datetime_id, location_id, aqi_value) VALUES ((SELECT id FROM datetime WHERE datetime = %s), (SELECT id FROM location WHERE location = %s), %s)", (data["datetime"], data["location"], data["aqi_value"]))
    
    conn.commit()
    cur.close()
    conn.close()

@task
def conditional_email_alert(json_file):
    """Evaluates the current AQI value against safety thresholds and sends email alerts to configured recipients if air quality exceeds safe levels."""
    with open(json_file, "r") as file:
        data = json.load(file)
    
    if data["aqi_value"] > 100:  # Example threshold
        send_email_alert(data)

def send_email_alert(data):
    """Sends an email alert using SMTP (Gmail)."""
    sender_email = "your_email@gmail.com"
    receiver_email = "recipient_email@example.com"
    password = "your_password"
    
    message = MIMEMultipart("alternative")
    message["Subject"] = "Air Quality Alert"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    text = f"Air Quality Alert: AQI value {data['aqi_value']} exceeds safe levels at {data['location']} on {data['datetime']}."
    part = MIMEText(text, "plain")
    message.attach(part)
    
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())

@flow
def aqi_etl_pipeline():
    """Orchestrates the ETL pipeline for fetching, processing, and loading air quality data, and sending alerts."""
    html_file = fetch_mahidol_aqi_html()
    json_file = extract_and_validate_data(html_file)
    load_data_to_postgresql(json_file)
    conditional_email_alert(json_file)

if __name__ == "__main__":
    aqi_etl_pipeline()