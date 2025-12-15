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
    """
    Downloads the latest AQI report webpage from Mahidol University and saves it as an HTML file.
    """
    url = "https://example.com/mahidol-aqi-report"
    response = requests.get(url)
    response.raise_for_status()
    with open("mahidol_aqi_report.html", "w") as file:
        file.write(response.text)
    return "mahidol_aqi_report.html"

@task
def extract_and_validate_data(html_file):
    """
    Parses the HTML to extract air quality metrics, validates data freshness, checks for duplicates,
    and creates a structured JSON output.
    """
    with open(html_file, "r") as file:
        soup = BeautifulSoup(file, "html.parser")
    
    aqi_data = {
        "datetime": soup.find("div", {"class": "datetime"}).text,
        "location": soup.find("div", {"class": "location"}).text,
        "aqi_value": int(soup.find("div", {"class": "aqi-value"}).text),
        "pollutants": [
            {"name": pollutant.find("span", {"class": "name"}).text, "value": float(pollutant.find("span", {"class": "value"}).text)}
            for pollutant in soup.find_all("div", {"class": "pollutant"})
        ]
    }

    # Validate data freshness and check for duplicates
    if not is_data_fresh(aqi_data["datetime"]) or is_data_duplicate(aqi_data):
        return None

    return aqi_data

def is_data_fresh(datetime_str):
    """
    Checks if the data is fresh (e.g., within the last hour).
    """
    # Placeholder for actual freshness check
    return True

def is_data_duplicate(aqi_data):
    """
    Checks if the data is a duplicate in the database.
    """
    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM aqi_facts WHERE datetime = %s AND location = %s",
        (aqi_data["datetime"], aqi_data["location"])
    )
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result is not None

@task
def load_data_to_postgresql(aqi_data):
    """
    Transforms and loads the JSON data into multiple database tables.
    """
    if not aqi_data:
        return

    conn = psycopg2.connect(
        dbname="your_dbname",
        user="your_user",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    cursor = conn.cursor()

    # Insert datetime
    cursor.execute(
        "INSERT INTO datetime (datetime) VALUES (%s) ON CONFLICT DO NOTHING",
        (aqi_data["datetime"],)
    )

    # Insert location
    cursor.execute(
        "INSERT INTO location (location) VALUES (%s) ON CONFLICT DO NOTHING",
        (aqi_data["location"],)
    )

    # Insert pollutants
    for pollutant in aqi_data["pollutants"]:
        cursor.execute(
            "INSERT INTO pollutants (name, value) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (pollutant["name"], pollutant["value"])
        )

    # Insert AQI facts
    cursor.execute(
        "INSERT INTO aqi_facts (datetime, location, aqi_value) VALUES (%s, %s, %s)",
        (aqi_data["datetime"], aqi_data["location"], aqi_data["aqi_value"])
    )

    conn.commit()
    cursor.close()
    conn.close()

@task
def conditional_email_alert(aqi_data):
    """
    Evaluates the current AQI value against safety thresholds and sends email alerts if necessary.
    """
    if not aqi_data or aqi_data["aqi_value"] <= 100:
        return

    sender_email = "your_email@example.com"
    receiver_email = "recipient_email@example.com"
    password = "your_password"

    message = MIMEMultipart("alternative")
    message["Subject"] = "Air Quality Alert"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"""\
    Air Quality Alert:
    Location: {aqi_data["location"]}
    AQI Value: {aqi_data["aqi_value"]}
    """

    part = MIMEText(text, "plain")
    message.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())

@flow
def aqi_etl_pipeline():
    """
    Orchestrates the ETL pipeline for Mahidol University's air quality data.
    """
    html_file = fetch_mahidol_aqi_html()
    aqi_data = extract_and_validate_data(html_file)
    load_data_to_postgresql(aqi_data)
    conditional_email_alert(aqi_data)

if __name__ == "__main__":
    aqi_etl_pipeline()