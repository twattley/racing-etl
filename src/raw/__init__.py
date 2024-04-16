import random
import time
import traceback
from dataclasses import dataclass

import pandas as pd
from selenium import webdriver

from src.data_models.base.base_model import BaseDataModel, convert_and_validate_data
from src.raw.webdriver_base import select_source_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import E, I


@dataclass
class DataScrapingTask:
    driver: webdriver.Chrome
    schema: str
    table: str
    job_name: str
    scraper_func: callable
    data_model: BaseDataModel
    string_fields: dict
    unique_id: str


@dataclass
class LinkScrapingTask:
    driver: webdriver.Chrome
    base_url: str
    schema: str
    source_table: str
    destination_table: str
    filter_func: callable


def process_scraping_data(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    dataframes_list = []
    filtered_links_df = fetch_data(f"SELECT * FROM {task.schema}.missing_links")
    I(f"Number of missing links: {len(filtered_links_df)}")
    for link in filtered_links_df.link:
        try:
            I(f"Scraping link: {link}")
            driver.get(link)
            dataframes_list.append(task.scraper_func(driver, link))
        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            continue

    data = pd.concat(dataframes_list)
    data = data.pipe(
        convert_and_validate_data, task.data_model, task.string_fields, task.unique_id
    )
    store_data(data, task.table, task.schema)
    driver.quit()


def process_scraping_result_links(task: LinkScrapingTask) -> None:
    I("Scrape_links.py execution started.")
    driver = task.driver
    dates = fetch_data(f"SELECT * FROM {task.schema}.{task.source_table}")
    if dates.empty:
        I("No missing dates found. Ending the script.")
        return
    dates = [i.strftime("%Y-%m-%d") for i in dates["date"]]
    for date in dates:
        try:
            url = f"{task.base_url}{str(date)}"
            I(f"Generated URL for scraping: {url}")
            driver.get(url)
            time.sleep(5)
            I("Page load complete. Proceeding to scrape links.")
            days_results_links = task.filter_func(driver)
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

    driver.quit()
    I("Scrape_links.py execution completed.")


def run_scraping_task(task):
    if isinstance(task, DataScrapingTask):
        process_scraping_data(task)
    elif isinstance(task, LinkScrapingTask):
        process_scraping_result_links(task)
