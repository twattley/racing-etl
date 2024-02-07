import random
import time
from src.raw import RP_RESULTS_URL
import pandas as pd
from selenium.webdriver.common.by import By
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I

if __name__ == "__main__":
    I("Scrape_links.py execution started.")
    driver = get_headless_driver()
    while True:
        try:
            dates = fetch_data("SELECT * FROM rp_raw.missing_dates")
            if dates.empty:
                I("No missing dates found. Ending the script.")
                break
            dates_list = dates["date"].tolist()
            random.shuffle(dates_list)
            date = dates_list[0].strftime("%Y-%m-%d")
            url = f"{RP_RESULTS_URL}{str(date)}"
            I(f"Generated URL for scraping: {url}")
            driver.get(url)
            time.sleep(4)
            I("Page load complete. Proceeding to scrape links.")
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='results/']")
            hrefs = [link.get_attribute("href") for link in links]
            days_results_links = list(
                {
                    i
                    for i in hrefs
                    if "fullReplay" not in i
                    and len(i.split("/")) == 8
                    and "winning-times" not in i
                }
            )
            I(f"Found {len(days_results_links)} valid links for date {date}.")
            days_results_links_df = pd.DataFrame(
                {"date": [date] * len(days_results_links), "link": days_results_links}
            )
            time.sleep(random.randint(2, 4))
            I(f"Inserting {len(days_results_links)} links into the database.")
            store_data(days_results_links_df, 'days_results_links', 'rp_raw')
        except Exception as e:
            I(f"An error occurred: {e}. Continuing to next date.")
            continue
