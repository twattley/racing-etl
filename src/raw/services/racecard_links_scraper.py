from datetime import datetime

import pandas as pd
from api_helpers.helpers.logging_config import I

from src.raw.interfaces.link_scraper_interface import ILinkScraper
from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.raw.interfaces.webriver_interface import IWebDriver


class RacecardsLinksScraperService:
    TODAY = datetime.now().strftime("%Y-%m-%d")

    def __init__(
        self,
        scraper: ILinkScraper,
        data_dao: IRawDataDao,
        driver: IWebDriver,
        schema: str,
        table_name: str,
        view_name: str,
    ):
        self.scraper = scraper
        self.data_dao = data_dao
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
        self.data_dao.store_data(self.schema, self.table_name, data)

    def run_racecard_links_scraper(self):
        data = self.process_date()
        self._store_racecard_data(data)
