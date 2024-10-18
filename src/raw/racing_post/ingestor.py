from dataclasses import dataclass
from typing import Literal

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials
from api_helpers.clients.postgres_client import PostgresClient
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.logging_config import I
from api_helpers.helpers.processing_utils import pt

from src.config import Config
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from src.raw.betfair.fetch_todays_data import TodaysBetfairDataService
from src.raw.daos.postgres_dao import PostgresDao
from src.raw.daos.s3_dao import S3Dao
from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.raw.racing_post.results_data_scraper import RPResultsDataScraper
from src.raw.racing_post.results_link_scraper import RPResultsLinkScraper
from src.raw.racing_post.todays_racecard_data_scraper import RPRacecardsDataScraper
from src.raw.racing_post.todays_racecard_links_scraper import RPRacecardsLinkScraper
from src.raw.services.historical_betfair_data import HistoricalBetfairDataService
from src.raw.services.racecard_links_scraper import RacecardsLinksScraperService
from src.raw.services.racecard_scraper import RacecardsDataScraperService
from src.raw.services.result_links_scraper import ResultLinksScraperService
from src.raw.services.results_scraper import ResultsDataScraperService
from src.raw.timeform.results_data_scraper import TFResultsDataScraper
from src.raw.timeform.results_link_scraper import TFResultsLinkScraper
from src.raw.timeform.todays_racecard_data_scraper import TFRacecardsDataScraper
from src.raw.timeform.todays_racecard_links_scraper import TFRacecardsLinkScraper
from src.raw.webdriver.web_driver import WebDriver
from src.storage.storage_client import get_storage_client


class RPIngestor:
    def __init__(
        self,
        config: Config,
        dao: IRawDataDao,
    ):
        self.config = config
        self.dao = dao

    SCHEMA = "rp_raw"

    def ingest_results_links(self):
        service = ResultLinksScraperService(
            scraper=RPResultsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.results_links_table_name,
            view_name=self.config.results_links_view_name,
        )
        service.run_results_links_scraper()

    def ingest_todays_links(self):
        service = RacecardsLinksScraperService(
            scraper=RPRacecardsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.racecards_links_table_name,
            view_name=self.config.racecards_links_view_name,
        )
        service.run_racecard_links_scraper()

    def ingest_results_data(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.results_data_table_name,
            view_name=self.config.results_data_view_name,
        )
        service.run_results_scraper()

    def ingest_todays_data(self):
        service = RacecardsDataScraperService(
            scraper=RPRacecardsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.racecards_data_table_name,
            view_name=self.config.racecards_data_view_name,
        )
        service.run_racecards_scraper()

    def ingest_results_data_world(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema=self.SCHEMA,
            table_name=self.config.ingestion_base_table_name,
            view_name=self.config.ingestion_base_view_name,
        )
        service.run_results_scraper()
