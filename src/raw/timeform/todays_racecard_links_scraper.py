import re
import time

import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.timeform.course_ref_data import TF_UKE_IRE_COURSE_IDS


class TFRacecardsLinkScraper(ILinkScraper):
    BASE_URL = "https://www.timeform.com/horse-racing/racecards"

    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                I(f"Scraping Timeform links for {date} (Attempt {attempt + 1})")
                driver.get(self.BASE_URL)
                time.sleep(15)
                self._click_for_racecards(driver, date)
                links = self._get_racecard_links(driver, date)
                return pd.DataFrame(
                    {
                        "link_url": links,
                        "date": [date] * len(links),
                    }
                )
            except Exception as e:
                I(f"An error occurred on attempt {attempt + 1}: {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                time.sleep(5)  # Wait a bit longer before retrying the entire process

        raise ValueError(f"Failed to scrape links after {max_attempts} attempts")

    def _click_for_racecards(self, driver: webdriver.Chrome, date: str):
        button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    f"button.w-racecard-grid-nav-button[data-meeting-date='{date}']",
                )
            )
        )
        driver.execute_script("arguments[0].click();", button)
        I(f"Clicked on button for date: {date}")
        time.sleep(10)

    def _get_racecard_links(self, driver: webdriver.Chrome, date: str) -> list[str]:
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Wait for the links to be present
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//a"))
                )

                hrefs = []
                elements = driver.find_elements(By.XPATH, "//a")
                for element in elements:
                    try:
                        href = element.get_attribute("href")
                        if href:
                            hrefs.append(href)
                    except StaleElementReferenceException:
                        continue  # Skip this element if it's stale

                trimmed_hrefs = [
                    href[:-1] if href.endswith("/") else href for href in hrefs
                ]

                patterns = [
                    rf"{self.BASE_URL}/{course_name}/{date}/([01]\d|2[0-3])[0-5]\d\/{course_id}/(10|[1-9])/(.*)"
                    for course_name, course_id in TF_UKE_IRE_COURSE_IDS.items()
                ]

                if not patterns:
                    raise ValueError(f"No patterns found on date: {date}")

                I(f"Found {len(hrefs)} links for {date}")

                filtered_urls = {
                    url
                    for url in trimmed_hrefs
                    for pattern in patterns
                    if re.search(pattern, url)
                }

                if filtered_urls:
                    return sorted(filtered_urls)
                else:
                    I(f"No matching URLs found on attempt {attempt + 1}. Retrying...")
                    time.sleep(2)  # Wait a bit before retrying
            except Exception as e:
                I(f"An error occurred on attempt {attempt + 1}: {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                time.sleep(2)  # Wait a bit before retrying

        raise ValueError(f"Failed to get racecard links after {max_attempts} attempts")
