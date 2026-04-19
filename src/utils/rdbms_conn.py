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
import logging

logger = logging.getLogger(__name__)


def _get_engine():
    neondb_config = read_config()['PostgresDB']
    connection_string = neondb_config['connection_string']
    return create_engine(connection_string, connect_args={"connect_timeout": 10})



def execute_query(query):
    engine = _get_engine()
    connection = engine.connect()
    transaction = connection.begin()

    try:
        if isinstance(query, str):
            query = text(query)
        results = connection.execute(query)
        transaction.commit()
        if results.returns_rows:
            return results.fetchall()
        else:
            return None
    except SQLAlchemyError as e:
        logger.warning("Database error: %s", e)
        transaction.rollback()
        return None
    except Exception as e:
        logger.warning("General error: %s", e)
        return None
    finally:
        connection.close()
        engine.dispose()


