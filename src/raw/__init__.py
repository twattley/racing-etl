from dataclasses import dataclass
import hashlib
import os
import random
import re
from datetime import datetime
import traceback
import time

from src.raw.webdriver_base import WebDriverBuilder, get_driver, get_headless_driver, is_driver_session_valid
from src.storage.sql_db import fetch_data, store_data
from src.raw.syncronizer import sync

import numpy as np
import pandas as pd
import pytz
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from src.utils.logging_config import I, E


BASE_RP_URL = "https://www.racingpost.com"
RP_RESULTS_URL = f"{BASE_RP_URL}/results/"

BASE_TF_URL = "https://www.timeform.com"
TF_RESULTS_URL = f"{BASE_TF_URL}/horse-racing/results/"


@dataclass
class DataScrapingTask:
    driver: WebDriverBuilder
    filepath: str
    schema: str
    table: str
    job_name: str
    scraping_function: callable
    link_filter_function: callable

@dataclass
class LinkScrapingTask:
    driver: WebDriverBuilder
    schema: str
    source_table: str
    destination_table: str
    link_filter_function: callable


def shuffle_dates(dates):
    dates_list = dates["date"].tolist()
    random.shuffle(dates_list)
    return [i.strftime("%Y-%m-%d") for i in dates_list]


def get_driver(task):    
    if 'rp' in task.job_name.lower():
        driver = task.driver()
    else:
        driver = task.driver(timeform=True)
    return driver
    

def process_scraping_data(task: DataScrapingTask):
    driver = get_driver(task)
    df = pd.DataFrame()
    processed_dates = pd.read_csv(task.filepath)

    for i, v in enumerate(range(1000000000)):
        try:
            I(f'Current size of the dataframe: {len(df)}')
            if processed_dates.empty:
                I("No missing links found. Ending the script.")
                break
            filtered_links_df = task.link_filter_function(processed_dates)
            sampled_link = filtered_links_df.sample(frac=1)
            I(f"Number of missing links: {len(filtered_links_df)}")
            link = sampled_link.link.iloc[0]
            I(f"Scraping link: {link}")
            if not is_driver_session_valid(driver):
                driver.quit()
                driver = get_driver(task)
                time.sleep(random.randint(360, 600))
            driver.get(link)
            time.sleep(4)
            performance_data = task.scraping_function(driver, link)
            df = pd.concat([df, performance_data])

            if i % 10 == 0:
                store_data(df, task.table, task.schema)
                sync(task.job_name)
                df = pd.DataFrame()
                processed_dates = pd.read_csv(task.filepath)

        except KeyboardInterrupt:
            I("Keyboard interrupt detected. Exiting the script.")
            break
        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            time.sleep(random.randint(10, 20))
            continue


def process_scraping_result_links(task):
    I("Scrape_links.py execution started.")
    driver = task.driver
    dates = fetch_data(f"SELECT * FROM {task.schema}.{task.source_table}")
    if dates.empty:
        I("No missing dates found. Ending the script.")
        return
    
    dates = shuffle_dates(dates)
    for date in dates:
        try:
            url = f"{TF_RESULTS_URL}{str(date)}"
            I(f"Generated URL for scraping: {url}")
            driver.get(url)
            time.sleep(5)
            I("Page load complete. Proceeding to scrape links.")
            days_results_links = task.link_filter_function(driver)
            I(f"Found {len(days_results_links)} valid links for date {date}.")
            days_results_links_df = pd.DataFrame(
                {"date": [date] * len(days_results_links), "link": days_results_links}
            )
            I(f"Inserting {len(days_results_links)} links into the database.")
            store_data(days_results_links_df, task.destination_table, task.schema)
        except KeyboardInterrupt:
            I("Keyboard interrupt detected. Exiting the script.")
            break
        except Exception as e:
            I(f"An error occurred: {e}. Continuing to next date.")
            continue

def run_scraping_task(task):
    if isinstance(task, DataScrapingTask):
        process_scraping_data(task)
    elif isinstance(task, LinkScrapingTask):
        process_scraping_result_links(task)
