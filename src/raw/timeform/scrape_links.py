import os
import random
import time
from src.raw import TF_RESULTS_URL, LinkScrapingTask, run_scraping_task
import pandas as pd
from selenium.webdriver.common.by import By
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I, E
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_pages_results_links(driver):
    elements = driver.find_elements(
        By.CSS_SELECTOR, 'a.results-title[href*="/horse-racing/result/"]'
    )
    return [element.get_attribute("href") for element in elements]


def get_results_links(driver):
    pages_links = get_pages_results_links(driver)
    buttons = driver.find_elements(
        By.CSS_SELECTOR, "button.w-course-region-tabs-button"
    )
    for button in buttons:
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
        button.click()
        time.sleep(10)
        pages_links.extend(get_pages_results_links(driver))

    return list(set(pages_links))


def process_tf_scrape_links():
    task = LinkScrapingTask(
        driver=get_headless_driver(),
        schema="tf_raw",
        source_table="missing_dates",
        destination_table="days_results_links",
        link_filter_function=get_results_links,
    )
    run_scraping_task(task)


if __name__ == "__main__":
    process_tf_scrape_links()
