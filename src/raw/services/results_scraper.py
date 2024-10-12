import pandas as pd
from api_helpers.helpers.logging_config import E, I

from src.raw.interfaces.data_scraper_interface import IDataScraper
from src.raw.interfaces.link_identifier_interface import ILinkIdentifier
from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.raw.interfaces.webriver_interface import IWebDriver


class ResultsDataScraperService:
    def __init__(
        self,
        scraper: IDataScraper,
        data_dao: IRawDataDao,
        driver: IWebDriver,
        link_identifier: ILinkIdentifier,
        schema: str,
        table_name: str,
        view_name: str,
        login: bool = False,
    ):
        self.scraper = scraper
        self.data_dao = data_dao
        self.driver = driver
        self.link_identifier = link_identifier
        self.schema = schema
        self.table_name = table_name
        self.view_name = view_name
        self.login = login

    def _get_missing_links(self) -> list[str]:
        links = self.data_dao.fetch_links(self.schema, self.view_name)
        return links.to_dict(orient="records")

    def process_links(self, links: list[str]) -> pd.DataFrame:
        driver = self.driver.create_session(self.login)
        dataframes_list = []

        for link in links:
            try:
                if self._filter_link(link["link_url"]):
                    I(f"Scraping link: {link['link_url']}")
                    driver.get(link["link_url"])
                    data = self.scraper.scrape_data(driver, link["link_url"])
                    I(f"Scraped {len(data)} rows")
                    dataframes_list.append(data)
                else:
                    I(f"Link {link['link_url']} Not of interest to this scraper")
            except Exception as e:
                E(
                    f"Encountered an error: {e}. Attempting to continue with the next link."
                )
                continue

        if not dataframes_list:
            I("No data scraped. Ending the script.")
            return

        combined_data = pd.concat(dataframes_list)

        return combined_data

    def _filter_link(self, link: str) -> bool:
        return self.link_identifier.identify_link(link)

    def _stores_results_data(self, data: pd.DataFrame) -> None:
        self.data_dao.store_data(self.schema, self.table_name, data)

    def run_results_scraper(self):
        links = self._get_missing_links()
        if not links:
            I("No links to scrape. Ending the script.")
            return
        data = self.process_links(links)
        self._stores_results_data(data)
