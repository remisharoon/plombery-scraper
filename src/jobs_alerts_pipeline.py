from asyncio import sleep
from datetime import datetime
import enum
from typing import Optional
from dateutil import tz
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from pydantic import BaseModel, Field
from plombery import register_pipeline, task, Trigger, get_logger
import random
import requests
import time
import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import urllib.request
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from sqlalchemy import text
from config import read_config
import re

# Set up the API request
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"

# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']
# API_KEY = gemini_config['API_KEY']
API_KEY = random.choice([gemini_config['API_KEY_RH'], gemini_config['API_KEY_RHA']])


params = {'key': API_KEY}  # Use the actual API key provided
headers = {'Content-Type': 'application/json'}

neondb_config = read_config()['PostgresDB']
connection_string = neondb_config['connection_string']

sendgrid_config = read_config()['Sendgrid']

def send_alert_email(connection, server, email, alerts):

    sender_email = "jobs-agent@gmail.com"
    receiver_email = email

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = "Daily Jobs Update"

    # Construct an HTML table for the email body
    html = """<html>
        <head>
            <style>
                table { width: 100%; border-collapse: collapse; }
                th, td { border: 1px solid #dddddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h2>Here is your daily Jobs update!</h2>
            <table>
                <tr>
                    <th>Date Posted</th>
                    <th>Title</th>
                    <th>Company</th>
                    <th>Location</th>
                    <th>Country</th>
                    <th>Link</th>
                </tr>
    """
    for row in alerts:
        html += f"""
                <tr>
                    <td>{row.date_posted}</td>
                    <td>{row.title}</td>
                    <td>{row.company}</td>
                    <td>{row.location}</td>
                    <td>{row.country_inferred}</td>
                    <td><a href="{row.job_url}">View Job</a></td>
                </tr>
        """
    html += """
            </table>
        </body>
    </html>
    """

    # Attach the HTML content
    message.attach(MIMEText(html, "html"))

    msgtext = message.as_string()  # Converts the message to a string
    server.sendmail(sender_email, receiver_email, msgtext)  # Sends the email

@task
async def send_alert_emails():
    # Database connection string
    engine = create_engine(connection_string)
    connection = engine.connect()

    # Connect to Gmail's SMTP server
    Username = sendgrid_config['Username']
    Password = sendgrid_config['Password']
    server = smtplib.SMTP('pro.turbo-smtp.com', 587)
    server.starttls()  # Encrypts the email
    server.login(Username, Password)  # Log in to server

    # Fetch all subscriptions and group by email
    query = text("""
        SELECT email, country, job_title
        FROM ja_job_alerts_subscriptions
    """)
    subscriptions = connection.execute(query).fetchall()

    # Create a dictionary to store the grouped subscriptions
    subscriptions_dict = {}
    for sub in subscriptions:
        if sub.email not in subscriptions_dict:
            subscriptions_dict[sub.email] = []
        subscriptions_dict[sub.email].append(sub)

    # Fetch job alerts for each subscriber and send email
    # for email, subs in subscriptions_dict.items():
    #     conditions = " OR ".join([
    #         f"(job_title_inferred = '{sub.job_title}' AND country_inferred = '{sub.country}')"
    #         for sub in subs
    #     ])
    #     query = text(f"""
    #         SELECT distinct date_posted, title, company, location, job_url, country_inferred
    #         FROM ja_jobs_norm_vw
    #         WHERE ({conditions})
    #         AND date_posted >= CURRENT_DATE - INTERVAL '7 DAY'
    #         ORDER BY date_posted DESC
    #     """)
    #     alerts = connection.execute(query).fetchall()
    #     if alerts:
    #         send_alert_email(connection, server, email, alerts)

    # for email, subs in subscriptions_dict.items():
    #     clauses = []
    #     params = {}
    #     for i, sub in enumerate(subs):
    #         # Build a regex: \bdata\s+engineer(ing)?\b (case-insensitive)
    #         kw = sub.job_title.strip().lower()
    #         if kw == "data engineer":
    #             pattern = r"\bdata\s+engineer(ing)?\b"
    #         else:
    #             # generic fallback: escape user text and do word-boundary match
    #             pattern = rf"\b{re.escape(kw)}\b"
    #
    #         clauses.append(f"(job_title_inferred ~* :p{i} AND country_inferred = :c{i})")
    #         params[f"p{i}"] = pattern
    #         params[f"c{i}"] = sub.country
    #
    #     conditions = " OR ".join(clauses)
    #     sql = f"""
    #         SELECT DISTINCT date_posted, title, company, location, job_url, country_inferred
    #         FROM ja_jobs_norm_vw
    #         WHERE ({conditions})
    #           AND date_posted >= CURRENT_DATE - INTERVAL '7 DAY'
    #         ORDER BY date_posted DESC
    #     """
    #     alerts = connection.execute(text(sql), params).fetchall()
    #     if alerts:
    #         send_alert_email(connection, server, email, alerts)

    # Match titles by regex only (ignore country). Uses Postgres \m \M word boundaries.
    for email, subs in subscriptions_dict.items():
        clauses = []
        params = {}

        for i, sub in enumerate(subs):
            kw = (sub.job_title or "").strip().lower()
            if kw == "data engineer":
                # matches "data engineer", "data engineering", "senior data engineer", etc.
                pattern = r"\mdata\s+engineer(ing)?\M"
            else:
                # generic fallback: word-boundary match of the full term
                # (e.g., "data scientist", "data analyst")
                pattern = rf"\m{re.escape(kw)}\M"

            clauses.append(f"(job_title_inferred ~* :p{i})")
            params[f"p{i}"] = pattern

        # If no subs for this email, skip
        if not clauses:
            continue

        conditions = " OR ".join(clauses)
        sql = f"""
            SELECT DISTINCT date_posted, title, company, location, job_url, country_inferred
            FROM ja_jobs_norm_vw
            WHERE ({conditions})
              AND date_posted >= CURRENT_DATE - INTERVAL '7 DAY'
            ORDER BY date_posted DESC
        """
        alerts = connection.execute(text(sql), params).fetchall()
        if alerts:
            send_alert_email(connection, server, email, alerts)

    server.quit()  # Terminates the server session
    connection.close()
    engine.dispose()

class InputParams(BaseModel):
    """Showcase all the available input types in Plombery"""

register_pipeline(
    id="jobs_alerts_pipeline",
    description="""This is a very alertty pipeline""",
    tasks=[send_alert_emails],
    triggers=[
        Trigger(
            id="daily1",
            name="Daily1",
            description="Run the pipeline 1 times daily",
            params=InputParams(),
            schedule=CronTrigger(
                hour="7", minute="45", timezone="Asia/Dubai"
            ),
        )
    ],
    params=InputParams,
)
