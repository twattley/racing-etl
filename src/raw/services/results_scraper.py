import pandas as pd
from api_helpers.helpers.logging_config import E, I
from api_helpers.interfaces.storage_client_interface import IStorageClient
from src.raw.interfaces.data_scraper_interface import IDataScraper
from src.raw.interfaces.webriver_interface import IWebDriver


class ResultsDataScraperService:
    def __init__(
        self,
        scraper: IDataScraper,
        storage_client: IStorageClient,
        driver: IWebDriver,
        schema: str,
        table_name: str,
        view_name: str,
        upsert_procedure: str,
        login: bool = False,
    ):
        self.scraper = scraper
        self.storage_client = storage_client
        self.driver = driver
        self.schema = schema
        self.table_name = table_name
        self.view_name = view_name
        self.upsert_procedure = upsert_procedure
        self.login = login

    def _get_missing_links(self) -> list[str]:
        links: pd.DataFrame = self.storage_client.fetch_data(
            f"SELECT link_url FROM {self.schema}.{self.view_name}"
        )
        return links.to_dict(orient="records")

    def process_links(self, links: list[str]) -> pd.DataFrame:
        driver = self.driver.create_session(self.login)
        dataframes_list = []

        for index, link in enumerate(links):
            I(f"Processing link {index} of {len(links)}")
            try:
                I(f"Scraping link: {link['link_url']}")
                driver.get(link["link_url"])
                data = self.scraper.scrape_data(driver, link["link_url"])
                I(f"Scraped {len(data)} rows")
                dataframes_list.append(data)
            except Exception as e:
                E(
                    f"Encountered an error: {e}. Attempting to continue with the next link."
                )
                continue

        if not dataframes_list:
            I("No data scraped. Ending the script.")
            return pd.DataFrame()

        combined_data = pd.concat(dataframes_list)

        return combined_data

    def _stores_results_data(self, data: pd.DataFrame) -> None:
        self.storage_client.upsert_data(
            data=data,
            schema=self.schema,
            table_name=self.table_name,
            unique_columns=["unique_id"],
            use_base_table=True,
            upsert_procedure=self.upsert_procedure,
        )

    def run_results_scraper(self):
        links = self._get_missing_links()
        if not links:
            I("No links to scrape. Ending the script.")
            return
        data = self.process_links(links)
        if data.empty:
            I("No data processed. Ending the script.")
            return
        self._stores_results_data(data)
