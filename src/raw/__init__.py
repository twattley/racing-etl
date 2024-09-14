import time
import traceback
from dataclasses import dataclass

import pandas as pd
from selenium import webdriver

from src.data_models.base.base_model import BaseDataModel, convert_and_validate_data
from src.raw.webdriver_base import select_source_driver
from src.storage.psql_db import get_db
from src.utils.logging_config import E, I
from src.utils.processing_utils import register_job_completion

db = get_db()


@dataclass
class DataScrapingTask:
    driver: webdriver.Chrome
    schema: str
    source_view: str
    dest_table: str
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
    # dataframes_list = []

    # for link in filtered_links_df.link_url.unique():
    for _ in range(1000000):
        filtered_links_df = db.fetch_data(
            f"SELECT * FROM {task.schema}.{task.source_view}"
        )
        if filtered_links_df.empty:
            I("No missing links found. Ending the script.")
            return
        I(f"Number of missing links: {len(filtered_links_df)}")
        sample_link = filtered_links_df.sample(1).link_url.values[0]
        try:
            I(f"Scraping link: {sample_link}")
            driver.get(sample_link)
            data = task.scraper_func(driver, sample_link)
            if data["race_title"][0] == "NOT A CLASS 1 RACE OR SPECIAL COURSE":
                db.store_data(data[["debug_link"]], "not_class_1_races", task.schema)
                continue
            db.store_data(data, task.dest_table, task.schema)
            # dataframes_list.append(data)
        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            continue

    # if not dataframes_list:
    #     I("No data scraped. Ending the script.")
    #     return
    # data = pd.concat(dataframes_list)
    # data = data.pipe(
    #     convert_and_validate_data, task.data_model, task.string_fields, task.unique_id
    # )
    # db.store_data(data, task.dest_table, task.schema)
    # register_job_completion(task.job_name)
    # driver.quit()


def process_scraping_result_links(task: LinkScrapingTask) -> None:
    I("Scrape_links.py execution started.")
    driver = task.driver
    dates = db.fetch_data(f"SELECT * FROM {task.schema}.{task.source_table}")
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
                {
                    "date": [date] * len(days_results_links),
                    "link_url": days_results_links,
                }
            )
            I(f"Inserting {len(days_results_links)} links into the database.")
            db.store_data(days_results_links_df, task.destination_table, task.schema)
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


def check_already_processed(job_name: str) -> bool:
    return db.fetch_data(
        f"""
        SELECT * 
        FROM metrics.processing_times 
        WHERE job_name = '{job_name}' 
        AND processed_at:: date = CURRENT_DATE
        """
    ).empty
