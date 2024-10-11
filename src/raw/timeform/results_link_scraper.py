import time

import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.raw.interfaces.link_scraper_interface import ILinkScraper


class TFResultsLinkScraper(ILinkScraper):
    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        driver.get(f"https://www.timeform.com/horse-racing/results/{str(date)}")
        time.sleep(5)
        days_results_links = self._get_results_links(driver)
        I(f"Found {len(days_results_links)} valid links for date {date}.")
        return pd.DataFrame(
            {
                "date": date,
                "link_url": days_results_links,
            }
        )

    def _get_pages_results_links(self, driver: webdriver.Chrome) -> list[str]:
        elements = driver.find_elements(
            By.CSS_SELECTOR, 'a.results-title[href*="/horse-racing/result/"]'
        )
        return [element.get_attribute("href") for element in elements]

    def _get_results_links(self, driver: webdriver.Chrome) -> list[str]:
        pages_links = self._get_pages_results_links(driver)
        buttons = driver.find_elements(
            By.CSS_SELECTOR, "button.w-course-region-tabs-button"
        )
        for button in buttons:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
            button.click()
            time.sleep(10)
            pages_links.extend(self._get_pages_results_links(driver))

        return list(set(pages_links))
