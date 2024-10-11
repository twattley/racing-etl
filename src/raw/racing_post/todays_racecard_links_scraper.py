import re
import time

import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.racing_post.course_ref_data import RP_UKE_IRE_COURSE_IDS


class RPRacecardsLinkScraper(ILinkScraper):
    BASE_URL = "https://www.racingpost.com/racecards"

    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        I(f"Scraping Racing Post links for {date}")
        driver.get(self.BASE_URL)
        time.sleep(15)
        links = self._get_racecard_links(driver, date)
        return pd.DataFrame(
            {
                "link_url": links,
                "date": [date] * len(links),
            }
        )

    def _get_racecard_links(self, driver: webdriver.Chrome, date: str) -> list[str]:
        hrefs = [
            element.get_attribute("href")
            for element in driver.find_elements(By.XPATH, "//a")
        ]
        filtered_hrefs = [i for i in hrefs if i is not None]

        racecards = [i for i in filtered_hrefs if "racecards" in i]
        trimmed_hrefs = []
        for href in racecards:
            if href.endswith("/"):
                href = href[:-1]
            trimmed_hrefs.append(href)

        patterns = []
        for course_name, course_id in RP_UKE_IRE_COURSE_IDS.items():
            pattern = rf"https://www.racingpost.com/racecards/{course_id}/{course_name}/{date}/\d{{1,10}}$"
            patterns.append(pattern)

        if not patterns:
            raise ValueError(f"No patterns found on date: {date}")

        I(f"Found {len(filtered_hrefs)} links for {date}")

        return sorted(
            {
                url
                for url in filtered_hrefs
                for pattern in patterns
                if re.search(pattern, url)
            }
        )
