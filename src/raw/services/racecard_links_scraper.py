from datetime import datetime

import pandas as pd
from api_helpers.helpers.logging_config import I

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.interfaces.webriver_interface import IWebDriver
from api_helpers.interfaces.storage_client_interface import IStorageClient


class RacecardsLinksScraperService:
    TODAY = datetime.now().strftime("%Y-%m-%d")

    def __init__(
        self,
        scraper: ILinkScraper,
        storage_client: IStorageClient,
        driver: IWebDriver,
        schema: str,
        table_name: str,
        view_name: str,
    ):
        self.scraper = scraper
        self.storage_client = storage_client
        self.driver = driver
        self.schema = schema
        self.table_name = table_name
        self.view_name = view_name

    def process_date(self) -> pd.DataFrame:
        driver = self.driver.create_session()
        data: pd.DataFrame = self.scraper.scrape_links(driver, self.TODAY)
        I(f"Scraped {len(data)} links for {self.TODAY}")
        return data

    def _store_racecard_data(self, data: pd.DataFrame) -> None:
        self.storage_client.store_data(
            self.schema, self.table_name, data, truncate=True
        )

    def run_racecard_links_scraper(self):
        data = self.process_date()
        self._store_racecard_data(data)
