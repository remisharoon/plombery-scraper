from asyncio import sleep
from datetime import datetime
import enum
from typing import Optional
from dateutil import tz
from apscheduler.triggers.interval import IntervalTrigger
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
from sqlalchemy.exc import SQLAlchemyError
from config import read_config


def _get_engine():
    neondb_config = read_config()['PostgresDB']
    connection_string = neondb_config['connection_string']
    return create_engine(connection_string)



def execute_query(query):
    engine = _get_engine()
    connection = engine.connect()
    transaction = connection.begin()  # Start a transaction

    try:
        results = connection.execute(query)
        transaction.commit()  # Commit the transaction if no errors occur
        if results.returns_rows:
            return results.fetchall()  # Fetch results if there are any
        else:
            return None
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        transaction.rollback()  # Roll back the transaction in case of an error
        return None
    except Exception as e:
        print(f"General error: {e}")
        return None
    finally:
        connection.close()
        engine.dispose()

# Example usage
query = "INSERT INTO ja_country_names_std_mapping (source_value, target_value) VALUES ('AZ, AE', 'Dubai')"
execute_query(query)


