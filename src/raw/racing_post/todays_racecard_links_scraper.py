import re
import time

import pandas as pd
from api_helpers.helpers.logging_config import I, W
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.racing_post.course_ref_data import RP_UKE_IRE_COURSE_IDS


class RPRacecardsLinkScraper(ILinkScraper):
    BASE_URL = "https://www.racingpost.com/racecards"

    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                I(f"Scraping Racing Post links for {date} (Attempt {attempt + 1})")
                driver.get(self.BASE_URL)
                time.sleep(15)
                links = self._get_racecard_links(driver, date)
                return pd.DataFrame(
                    {
                        "link_url": links,
                        "date": [date] * len(links),
                    }
                )
            except Exception as e:
                W(f"An error occurred on attempt {attempt + 1}: {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                time.sleep(5)  # Wait before retrying

        raise ValueError(f"Failed to scrape links after {max_attempts} attempts")

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

                filtered_hrefs = [
                    i for i in hrefs if i is not None and "racecards" in i
                ]
                trimmed_hrefs = [
                    href[:-1] if href.endswith("/") else href for href in filtered_hrefs
                ]
                patterns = [
                    rf"https://www.racingpost.com/racecards/{course_id}/{course_name}/{date}/\d{{1,10}}$"
                    for course_id, course_name in RP_UKE_IRE_COURSE_IDS.items()
                ]
                if not patterns:
                    raise ValueError(f"No patterns found on date: {date}")

                I(f"Found {len(filtered_hrefs)} links for {date}")

                filtered_urls = {
                    url
                    for url in trimmed_hrefs
                    for pattern in patterns
                    if re.search(pattern, url)
                }

                if filtered_urls:
                    return sorted(filtered_urls)
                else:
                    W(f"No matching URLs found on attempt {attempt + 1}. Retrying...")
                    time.sleep(2)  # Wait before retrying
            except Exception as e:
                W(
                    f"An error occurred while getting racecard links on attempt {attempt + 1}: {str(e)}"
                )
                if attempt == max_attempts - 1:
                    raise
