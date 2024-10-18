from api_helpers.interfaces.storage_client_interface import IStorageClient

from src.config import Config
from src.raw.racing_post.results_data_scraper import RPResultsDataScraper
from src.raw.racing_post.results_link_scraper import RPResultsLinkScraper
from src.raw.racing_post.todays_racecard_data_scraper import RPRacecardsDataScraper
from src.raw.racing_post.todays_racecard_links_scraper import RPRacecardsLinkScraper
from src.raw.services.racecard_links_scraper import RacecardsLinksScraperService
from src.raw.services.racecard_scraper import RacecardsDataScraperService
from src.raw.services.result_links_scraper import ResultLinksScraperService
from src.raw.services.results_scraper import ResultsDataScraperService
from src.raw.webdriver.web_driver import WebDriver


class RPIngestor:
    def __init__(
        self,
        config: Config,
        storage_client: IStorageClient,
    ):
        self.config = config
        self.storage_client = storage_client

    SCHEMA = "rp_raw"

    def ingest_todays_links(self):
        service = RacecardsLinksScraperService(
            scraper=RPRacecardsLinkScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.todays_data.links_view,
            table_name=self.config.db.raw.todays_data.links_table,
        )
        service.run_racecard_links_scraper()

    def ingest_todays_data(self):
        service = RacecardsDataScraperService(
            scraper=RPRacecardsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.todays_data.links_table,
            table_name=self.config.db.raw.todays_data.data_table,
        )
        service.run_racecards_scraper()

    def ingest_results_links(self):
        service = ResultLinksScraperService(
            scraper=RPResultsLinkScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.results_data.links_view,
            table_name=self.config.db.raw.results_data.links_table,
        )
        service.run_results_links_scraper()

    def ingest_results_data(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            view_name=self.config.db.raw.results_data.data_view,
            table_name=self.config.db.raw.results_data.data_table,
        )
        service.run_results_scraper()

    def ingest_results_data_world(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            storage_client=self.storage_client,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.db.raw.results_data.data_world_table,
            view_name=self.config.db.raw.results_data.data_world_view,
        )
        service.run_results_scraper()
