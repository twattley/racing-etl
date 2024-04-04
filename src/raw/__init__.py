from datetime import date, datetime
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
from src.storage.s3_bucket import DigitalOceanSpacesHandler
from src.utils.logging_config import E, I
import os


@dataclass
class DataScrapingTask:
    driver: WebDriverBuilder
    schema: str
    table: str
    job_name: str
    scraper_func: callable
    year: int


@dataclass
class LinkScrapingTask:
    driver: WebDriverBuilder
    base_url: str
    schema: str
    source_table: str
    destination_table: str
    filter_func: callable


def process_batch_and_refresh_data(dataframes_list, task):
    handler = DigitalOceanSpacesHandler(
        access_key_id=os.environ.get("DIGITAL_OCEAN_SPACES_ACCESS_KEY_ID"),
        secret_access_key=os.environ.get("DIGITAL_OCEAN_SPACES_SECRET_ACCESS_KEY"),
    )
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    folder = f"data/subsets/{task.year}"
    file_name = f"datachunk_{timestamp}.parquet"
    object_path = f"{folder}/{file_name}"
    handler.upload_df_as_parquet(pd.concat(dataframes_list), object_path)

    prefix = f"{folder}/"
    upload_path = f"{folder}/consolidated_data_{task.year}.parquet"
    handler.process_folder(prefix, upload_path)
    s3_data_df = handler.download_folder(prefix)
    filtered_links_df = handler.download_df_from_parquet(
        "data/missing_links/missing_links.parquet"
    )
    filtered_links_df = filtered_links_df[
        filtered_links_df["date"].between(
            date(int(task.year), 1, 1), date(int(task.year), 12, 31)
        )
    ]
    processed_links = s3_data_df["debug_link"].unique()
    filtered_links_df = filtered_links_df[~filtered_links_df.link.isin(processed_links)]
    I(f"Number of missing links: {len(filtered_links_df)}")
    return filtered_links_df


def process_scraping_data(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    dataframes_list = []
    handler = DigitalOceanSpacesHandler(
        access_key_id=os.environ.get("DIGITAL_OCEAN_SPACES_ACCESS_KEY_ID"),
        secret_access_key=os.environ.get("DIGITAL_OCEAN_SPACES_SECRET_ACCESS_KEY"),
    )
    filtered_links_df = handler.download_df_from_parquet(
        "data/missing_links/missing_links.parquet"
    )
    filtered_links_df = filtered_links_df[
        filtered_links_df["date"].between(
            date(int(task.year), 1, 1), date(int(task.year), 12, 31)
        )
    ]

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

            if (i + 1) % 20 == 0:
                filtered_links_df = process_batch_and_refresh_data(
                    dataframes_list, task
                )
                dataframes_list = []
                if filtered_links_df.empty:
                    I("No missing links found. Ending the script.")
                    break

        except KeyboardInterrupt:
            I("Keyboard interrupt detected. Exiting the script.")
            process_batch_and_refresh_data(dataframes_list, task)
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
