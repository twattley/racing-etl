from api_helpers.interfaces.storage_client_interface import IStorageClient

from src.config import Config
from src.raw.services.racecard_links_scraper import RacecardsLinksScraperService
from src.raw.services.racecard_scraper import RacecardsDataScraperService
from src.raw.services.result_links_scraper import ResultLinksScraperService
from src.raw.services.results_scraper import ResultsDataScraperService
from src.raw.timeform.results_data_scraper import TFResultsDataScraper
from src.raw.timeform.results_link_scraper import TFResultsLinkScraper
from src.raw.timeform.todays_racecard_data_scraper import TFRacecardsDataScraper
from src.raw.timeform.todays_racecard_links_scraper import TFRacecardsLinkScraper
from src.raw.webdriver.web_driver import WebDriver


class TFIngestor:
    def __init__(
        self,
        config: Config,
        storage_client: IStorageClient,
    ):
        self.config = config
        self.storage_client = storage_client

    SCHEMA = "tf_raw"

    def ingest_todays_links(self):
        service = RacecardsLinksScraperService(
            scraper=TFRacecardsLinkScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.todays_data.links_view,
            table_name=self.config.db.raw.todays_data.links_table,
        )
        service.run_racecard_links_scraper()

    def ingest_todays_data(self):
        service = RacecardsDataScraperService(
            scraper=TFRacecardsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.todays_data.links_table,
            table_name=self.config.db.raw.todays_data.data_table,
            login=True,
        )
        service.run_racecards_scraper()

    def ingest_results_links(self):
        service = ResultLinksScraperService(
            scraper=TFResultsLinkScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.results_data.links_view,
            table_name=self.config.db.raw.results_data.links_table,
        )
        service.run_results_links_scraper()

    def ingest_results_data(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.results_data.data_view,
            table_name=self.config.db.raw.results_data.data_table,
            login=True,
        )
        service.run_results_scraper()

    def ingest_results_data_world(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.db.raw.results_data.data_world_table,
            view_name=self.config.db.raw.results_data.data_world_view,
            login=True,
        )
        service.run_results_scraper()
