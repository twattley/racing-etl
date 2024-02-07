import random
import time
from src.raw import TF_RESULTS_URL
import pandas as pd
from selenium.webdriver.common.by import By
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



def get_pages_results_links(driver):
    elements = driver.find_elements(By.CSS_SELECTOR, 'a.results-title[href*="/horse-racing/result/"]')
    return [element.get_attribute('href') for element in elements]

def get_results_links(driver):
    pages_links = get_pages_results_links(driver)
    buttons = driver.find_elements(By.CSS_SELECTOR, 'button.w-course-region-tabs-button')
    for button in buttons:
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
        button.click()
        time.sleep(10) 
        pages_links.extend(get_pages_results_links(driver))

    return list(set(pages_links))


if __name__ == "__main__":
    I("Scrape_links.py execution started.")
    driver = get_headless_driver(timeform=True)
    while True:
        try:
            dates = fetch_data("SELECT * FROM tf_raw.missing_dates")
            if dates.empty:
                I("No missing dates found. Ending the script.")
                break
            dates_list = dates["date"].tolist()
            random.shuffle(dates_list)
            date = dates_list[0].strftime("%Y-%m-%d")
            url = f"{TF_RESULTS_URL}{str(date)}"
            I(f"Generated URL for scraping: {url}")
            driver.get(url)
            time.sleep(4)
            I("Page load complete. Proceeding to scrape links.")
            days_results_links = get_results_links(driver)
            I(f"Found {len(days_results_links)} valid links for date {date}.")
            days_results_links_df = pd.DataFrame(
                {"date": [date] * len(days_results_links), "link": days_results_links}
            )
            time.sleep(random.randint(2, 4))
            I(f"Inserting {len(days_results_links)} links into the database.")
            store_data(days_results_links_df, 'days_results_links', 'tf_raw')
        except Exception as e:
            I(f"An error occurred: {e}. Continuing to next date.")
            continue
