import re
import time

import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.timeform.course_ref_data import TF_UKE_IRE_COURSE_IDS


class TFRacecardsLinkScraper(ILinkScraper):
    BASE_URL = "https://www.timeform.com/horse-racing/racecards"

    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        I(f"Scraping Timeform links for {date}")
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
        hrefs = [
            element.get_attribute("href")
            for element in driver.find_elements(By.XPATH, "//a")
        ]
        trimmed_hrefs = []
        for href in hrefs:
            if href.endswith("/"):
                href = href[:-1]
            trimmed_hrefs.append(href)

        patterns = []
        for course_name, course_id in TF_UKE_IRE_COURSE_IDS.items():
            pattern = rf"{self.BASE_URL}/{course_name}/{date}/([01]\d|2[0-3])[0-5]\d\/{course_id}/(10|[1-9])/(.*)"
            patterns.append(pattern)

        if not patterns:
            raise ValueError(f"No patterns found on date: {date}")

        I(f"Found {len(hrefs)} links for {date}")

        return sorted(
            {url for url in hrefs for pattern in patterns if re.search(pattern, url)}
        )
