#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import sqlalchemy as sa
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
# from selenium.webdriver.common.action_chains import ActionChains

#from src.connections.cockroach_client import get_cr_engine
from sqlalchemy import text

# Configure Chrome options to download the file automatically
chrome_options = Options()
chrome_options.add_argument("--headless") # <-- This line will make Chrome headless

# Create the "csv_downloads" folder in the script's directory if it doesn't exist
current_directory = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(current_directory, "csv_downloads")
os.makedirs(download_folder, exist_ok=True)

prefs = {
    "download.default_directory": download_folder,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
}

chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)

# Configure Firefox options to download the file automatically
firefox_options = Options()
# firefox_options.add_argument("--headless")  # <-- This line will make Firefox headless

# Create the "csv_downloads" folder in the script's directory if it doesn't exist
current_directory = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(current_directory, "../csv_downloads")
os.makedirs(download_folder, exist_ok=True)

# Firefox preferences for file download
fp = webdriver.FirefoxProfile()
fp.set_preference("browser.download.folderList", 2)
fp.set_preference("browser.download.manager.showWhenStarting", False)
fp.set_preference("browser.download.dir", download_folder)
fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
firefox_options.profile = fp

# Use WebDriver Manager to install the GeckoDriver dynamically
# driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=firefox_options)
# driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)
# driver = webdriver.Firefox(options=firefox_options)

time.sleep(3)

def site_load():
    driver.get('https://dubailand.gov.ae/en/open-data/real-estate-data/#/')
    time.sleep(5)
    from_date_field = driver.find_element(By.ID, 'transaction_pFromDate')  # Replace 'fromDate' with the actual id or change the method based on the actual attributes of the field
    # Clear the field
    from_date_field.clear()
    # Set the value
    from_date_field.send_keys('01/01/2022')  # Replace with the desired date

    submit_button = driver.find_element("xpath", "//button[@type='submit']")
    submit_button.location_once_scrolled_into_view
    # Use ActionChains to move to the element
    # actions = ActionChains(driver)
    # actions.move_to_element(submit_button).perform()
    driver.execute_script("arguments[0].click();", submit_button)
    time.sleep(50)

def click_download_button():
    download_button = driver.find_element(By.CLASS_NAME, "js-ExportCsv")
    # Scroll down until the button is visible
    download_button.location_once_scrolled_into_view
    # Use ActionChains to move to the element
    # actions = ActionChains(driver)
    # actions.move_to_element(download_button).perform()
    driver.execute_script("arguments[0].click();", download_button)
    time.sleep(15)

def get_latest_downloaded_file():
    files = os.listdir(download_folder)
    csv_files = [file for file in files if file.endswith(".csv")]
    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_folder, x)))
    return os.path.join(download_folder, csv_files[-1])

def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def get_latest_record_date(engine, table_name):
    conn = engine.connect()
    # Retrieve data from the pa_projects table
    # query = text('SELECT title, description, latitude, longitude, min_bedrooms, max_bedrooms, price_per_area, starting_price FROM pa_projects')
    # rows = conn.execute(query).fetchall()
    query = sa.text(f'SELECT MAX("Transaction Date") FROM {table_name}')
    result = conn.execute(query).scalar()
    return result

site_load()
click_download_button()
time.sleep(20)
driver.quit()

# Get the latest downloaded CSV file
csv_file_path = get_latest_downloaded_file()

# Read the downloaded CSV file into a pandas DataFrame
dld_trans_df = pd.read_csv(csv_file_path)
# print(dld_trans_df.head())

# Delete the downloaded CSV file
# delete_file(csv_file_path)

# cr_engine = get_cr_engine()

table_name = "pa_dld_trans"
# latest_date_string = get_latest_record_date(cr_engine, table_name)
latest_date = 0
# latest_date = pd.to_datetime(latest_date_string)
# Filter the DataFrame to keep only records with a date greater than the latest date in the database
# Replace 'date_column_name' with the appropriate column name in your DataFrame
dld_trans_df['Transaction Date'] = pd.to_datetime(dld_trans_df['Transaction Date'])
# filtered_df = dld_trans_df[dld_trans_df['Transaction Date'] > latest_date]
print(latest_date,len(dld_trans_df.index),len(dld_trans_df.index))
# filtered_df.to_sql(table_name, cr_engine, if_exists="append", index=False)
print(dld_trans_df.head(1))
# Close the database connection
# cr_engine.dispose()
