import random
import time
import traceback
from dataclasses import dataclass

import pandas as pd

from src.raw.webdriver_base import (
    WebDriverBuilder,
    is_driver_session_valid,
    select_source_driver,
)
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import E, I


@dataclass
class DataScrapingTask:
    driver: WebDriverBuilder
    schema: str
    table: str
    job_name: str
    scraper_func: callable


@dataclass
class LinkScrapingTask:
    driver: WebDriverBuilder
    base_url: str
    schema: str
    source_table: str
    destination_table: str
    filter_func: callable


def shuffle_dates(dates):
    dates_list = dates["date"].tolist()
    random.shuffle(dates_list)
    return [i.strftime("%Y-%m-%d") for i in dates_list]


def process_batch_and_refresh_data(dataframes_list, task):
    store_data(pd.concat(dataframes_list), task.table, task.schema)
    filtered_links_df = fetch_data(f"SELECT * FROM {task.schema}.missing_links")
    I(f"Number of missing links: {len(filtered_links_df)}")
    return filtered_links_df


def process_scraping_data(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    dataframes_list = []
    filtered_links_df = fetch_data(f"SELECT * FROM {task.schema}.missing_links")
    I(f"Number of missing links: {len(filtered_links_df)}")
    for i, _ in enumerate(range(1000000000)):
        try:
            if filtered_links_df.empty:
                I("No missing links found. Ending the script.")
                break

            link = filtered_links_df.sample().link.iloc[0]

            I(f"Scraping link: {link}")
            if not is_driver_session_valid(driver):
                driver.quit()
                driver = select_source_driver(task)
                time.sleep(5)
            driver.get(link)
            performance_data = task.scraper_func(driver, link)
            if performance_data is None:
                continue
            dataframes_list.append(performance_data)

            if (i + 1) % 1 == 0:
                filtered_links_df = process_batch_and_refresh_data(
                    dataframes_list, task
                )
                dataframes_list = []
                if filtered_links_df.empty:
                    I("No missing links found. Ending the script.")
                    break

        except KeyboardInterrupt:
            I("Keyboard interrupt detected. Exiting the script.")
            break
        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            continue


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


def run_scraping_task(task):
    if isinstance(task, DataScrapingTask):
        process_scraping_data(task)
    elif isinstance(task, LinkScrapingTask):
        process_scraping_result_links(task)
