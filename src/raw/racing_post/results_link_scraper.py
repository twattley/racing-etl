import time

import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.raw.interfaces.link_scraper_interface import ILinkScraper


class RPResultsLinkScraper(ILinkScraper):
    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        driver.get(f"https://www.racingpost.com/results/{date}")
        time.sleep(5)
        days_results_links = self._get_results_links(driver)
        I(f"Found {len(days_results_links)} valid links for date {date}.")
        return pd.DataFrame(
            {
                "date": date,
                "link_url": days_results_links,
            }
        )

    def _get_results_links(self, driver: webdriver.Chrome) -> list[str]:
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
