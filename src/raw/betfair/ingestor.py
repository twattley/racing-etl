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


class BFIngestor:
    def __init__(
        self,
        config: Config,
        dao: IRawDataDao,
        betfair_client: BetFairClient,
    ):
        self.config = config
        self.dao = dao
        self.betfair_client = betfair_client

    def ingest_results_data(self):
        service = HistoricalBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            betfair_data_processor=BetfairDataProcessor(self.config),
            s3_client=get_storage_client("s3"),
            data_dao=self.dao,
        )
        service.run_data_ingestion()

    def ingest_todays_data(self):
        service = TodaysBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            data_dao=self.dao,
        )
        service.run_data_ingestion()
