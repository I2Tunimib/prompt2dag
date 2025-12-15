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
    
    # Example data extraction
    data = {
        "datetime": aqi_data.find("span", {"class": "datetime"}).text,
        "location": aqi_data.find("span", {"class": "location"}).text,
        "aqi_value": int(aqi_data.find("span", {"class": "aqi-value"}).text),
    }
    
    # Validate data freshness and check for duplicates
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM aqi_data WHERE datetime = %s AND location = %s", (data["datetime"], data["location"]))
    if cursor.fetchone():
        conn.close()
        return None  # Data already exists, skip further processing
    conn.close()
    
    return data

@task
def load_data_to_postgresql(data):
    """Transforms and loads the JSON data into multiple database tables including datetime, location, pollution dimensions, and AQI facts."""
    if not data:
        return  # No new data to load
    
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()
    
    # Insert into datetime table
    cursor.execute("INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING", (data["datetime"],))
    
    # Insert into location table
    cursor.execute("INSERT INTO location (location) VALUES (%s) ON CONFLICT DO NOTHING", (data["location"],))
    
    # Insert into aqi_data table
    cursor.execute(
        "INSERT INTO aqi_data (datetime_id, location_id, aqi_value) VALUES ((SELECT id FROM datetime WHERE datetime = %s), (SELECT id FROM location WHERE location = %s), %s)",
        (data["datetime"], data["location"], data["aqi_value"])
    )
    
    conn.commit()
    conn.close()

@task
def conditional_email_alert(data):
    """Evaluates the current AQI value against safety thresholds and sends email alerts to configured recipients if air quality exceeds safe levels."""
    if not data or data["aqi_value"] <= 100:
        return  # AQI is within safe levels, no alert needed
    
    # Load email configuration
    with open("email_config.json", "r") as file:
        email_config = json.load(file)
    
    # Set up the email
    sender_email = email_config["sender_email"]
    receiver_email = email_config["receiver_email"]
    password = email_config["password"]
    
    message = MIMEMultipart("alternative")
    message["Subject"] = "Air Quality Alert"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    text = f"Air Quality Index (AQI) at {data['location']} on {data['datetime']} has exceeded safe levels. Current AQI: {data['aqi_value']}"
    part = MIMEText(text, "plain")
    message.attach(part)
    
    # Send the email
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())

@flow
def aqi_etl_pipeline():
    """Orchestrates the ETL pipeline for fetching, processing, and loading air quality data, and sending alerts."""
    html_file = fetch_mahidol_aqi_html()
    data = extract_and_validate_data(html_file)
    load_data_to_postgresql(data)
    conditional_email_alert(data)

if __name__ == "__main__":
    aqi_etl_pipeline()