import random
import time
from src.raw import RP_RESULTS_URL, LinkScrapingTask, run_scraping_task
import pandas as pd
from selenium.webdriver.common.by import By
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I


def get_results_links(driver):
    links = driver.find_elements(By.CSS_SELECTOR, "a[href*='results/']")
    hrefs = [link.get_attribute("href") for link in links]
    return list(
        {
            i
            for i in hrefs
            if "fullReplay" not in i
            and len(i.split("/")) == 8
            and "winning-times" not in i
        }
    )

def process_rp_scrape_links():
    task = LinkScrapingTask(
        driver=get_headless_driver(),
        schema="rp_raw",
        source_table="missing_dates",
        destination_table="days_results_links",
        link_filter_function=get_results_links,
    )
    run_scraping_task(task)


if __name__ == "__main__":
    process_rp_scrape_links()
