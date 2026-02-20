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
from utils.rdbms_conn import execute_query
from utils.ai_infer import infer
from sqlalchemy import text

def get_dimensions_to_standardize():
    dims = []

    # ja_country_names_std_mapping
    dim_country_names = {}
    dim_country_names['table'] = 'ja_country_names_std_mapping'
    dim_country_names['query_source_values_to_std'] = text("""select distinct country_inferred source_values_to_std  from ja_jobs_raw jr left outer join  ja_country_names_std_mapping cm
                                on trim(jr.country_inferred) = cm.source_value 
                                where cm.target_value is null 
                                and jr.country_inferred is not null
                                and trim(jr.country_inferred) != ''
                            """)
    dim_country_names['query_std_values'] = text("""select distinct target_value from ja_country_names_std_mapping where target_value is not null""")

    # dims.append(dim_country_names)

    # ja_job_titles_std_mapping
    dim_job_titles_names = {}
    dim_job_titles_names['table'] = 'ja_job_titles_std_mapping'
    dim_job_titles_names['query_source_values_to_std'] = text("""select distinct job_title_inferred  source_values_to_std  from ja_jobs_raw jr left outer join  ja_job_titles_std_mapping jm
                                on trim(jr.job_title_inferred) = jm.source_value 
                                where jm.target_value is null 
                                and jr.job_title_inferred is not null
                                and trim(jr.country_inferred) != ''
                            """)
    dim_job_titles_names['query_std_values'] = text("""select distinct target_value from ja_job_titles_std_mapping where target_value is not null""")

    dims.append(dim_job_titles_names)

    return dims


def results_to_csv(results):
    """Convert query results to a CSV string."""
    # Assuming each result is a tuple
    res = '","'.join(str(item[0]) for item in results) if results else ''
    res = '"' + res + '"' if res else ''
    return res

def insert_mapping_to_db(mappings, mapping_table):
    # Assuming you have a database connection and a table named 'mapping_table'
    for source_value, target_value in mappings.items():
        print("inserting ", source_value, target_value)
        query = text(f"INSERT INTO {mapping_table} (source_value, target_value) VALUES ('{source_value}', '{target_value}')")
        print("insert query = ", query)
        execute_query(query)
        print("done inserting ", source_value, target_value)

@task
async def standardize_dims():
    dims = get_dimensions_to_standardize()

    for dim in dims:
        query = dim['query_source_values_to_std']
        source_values_to_std = execute_query(query)
        source_values_to_std_csv = results_to_csv(source_values_to_std)

        query = dim['query_std_values']
        mapping_table = dim['table']
        target_values = execute_query(query)
        target_values_csv = results_to_csv(target_values)

        if source_values_to_std_csv != '' and target_values_csv != '':
            payload = {
                "contents": [{"parts": [{"text": f"""  create a standardization mapping for these source fields into the provided target fields as a JSON string of values that can be loaded using json.loads(), if there is no appropriate standardization mapping provided in the target fields, use the value Other :
                        ; source values are as follows - {source_values_to_std_csv} ;
                        ; target values are as follows - {target_values_csv}  ;
                        Give the output as a proper JSON string format  """}]}],
            }
            print(payload)
            new_mappings = infer(payload)

            print(new_mappings)
            insert_mapping_to_db(new_mappings, mapping_table)

            print("done")


class InputParams(BaseModel):
    """Showcase all the available input types in Plombery"""

register_pipeline(
    id="dims_standardization_pipeline",
    description="""This is a very standardddyy pipeline""",
    tasks=[standardize_dims],
    triggers=[
        Trigger(
            id="daily1",
            name="Daily1",
            description="Run the pipeline 1 times daily",
            params=InputParams(),
            schedule=IntervalTrigger(
                # days=1,
                hours=24,
                # start_date=datetime(
                #     2023, 1, 1, 22, 30, tzinfo=tz.gettz("Europe/Brussels")
                # ),
            ),
        )
    ],
    params=InputParams,
)