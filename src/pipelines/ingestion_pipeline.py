from dataclasses import dataclass
from typing import Literal

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials
from api_helpers.clients.postgres_client import PostgresClient
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.processing_utils import pt

from src.config import Config
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from src.raw.betfair.fetch_todays_data import TodaysBetfairDataService
from src.raw.daos.postgres_dao import PostgresDao
from src.raw.daos.s3_dao import S3Dao
from src.raw.helpers.link_identifier import LinkIdentifier
from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.raw.racing_post.course_ref_data import (
    RP_NON_UKE_IRE_COURSE_IDS,
    RP_UKE_IRE_COURSE_IDS,
)
from src.raw.racing_post.results_data_scraper import RPResultsDataScraper
from src.raw.racing_post.results_link_scraper import RPResultsLinkScraper
from src.raw.racing_post.todays_racecard_data_scraper import RPRacecardsDataScraper
from src.raw.racing_post.todays_racecard_links_scraper import RPRacecardsLinkScraper
from src.raw.services.historical_betfair_data import HistoricalBetfairDataService
from src.raw.services.racecard_links_scraper import RacecardsLinksScraperService
from src.raw.services.racecard_scraper import RacecardsDataScraperService
from src.raw.services.result_links_scraper import ResultLinksScraperService
from src.raw.services.results_scraper import ResultsDataScraperService
from src.raw.timeform.course_ref_data import (
    TF_NON_UKE_IRE_COURSE_IDS,
    TF_UKE_IRE_COURSE_IDS,
)
from src.raw.timeform.results_data_scraper import TFResultsDataScraper
from src.raw.timeform.results_link_scraper import TFResultsLinkScraper
from src.raw.timeform.todays_racecard_data_scraper import TFRacecardsDataScraper
from src.raw.timeform.todays_racecard_links_scraper import TFRacecardsLinkScraper
from src.raw.webdriver.web_driver import WebDriver
from src.storage.storage_client import get_storage_client


@dataclass
class Views:
    uk_ire_performance_data: str
    non_uk_ire_performance_data: str


def ingest_data_from_s3(schema: str, table: str):
    s3_client: S3Client = get_storage_client("s3")
    postgres_client: PostgresClient = get_storage_client("postgres")
    data = s3_client.fetch_data(
        f"{schema}.{table}.parquet",
    )
    postgres_client.store_data(data, table, schema)
    postgres_client.call_procedure("insert_cloud_data", schema)


def ingest_raw_performance_data_from_s3(environment: Literal["LOCAL", "CLOUD"]):
    if environment == "CLOUD":
        return
    pt(
        lambda: ingest_data_from_s3("rp_raw", "performance_data"),
        lambda: ingest_data_from_s3("rp_raw", "non_uk_ire_performance_data"),
        lambda: ingest_data_from_s3("tf_raw", "performance_data"),
    )
    pt(
        lambda: ingest_data_from_s3("tf_raw", "non_uk_ire_performance_data"),
        lambda: ingest_data_from_s3("bf_raw", "todays_price_data"),
        lambda: ingest_data_from_s3("bf_raw", "historical_price_data"),
    )


class IngestionPipeline:
    def __init__(
        self,
        config: Config,
        dao: IRawDataDao,
        betfair_client: BetFairClient,
        missing_data_views: Views,
    ):
        self.config = config
        self.dao = dao
        self.betfair_client = betfair_client
        self.missing_data_views = missing_data_views

    # Results Links
    def ingest_rp_results_links_data(self):
        service = ResultLinksScraperService(
            scraper=RPResultsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="days_results_links",
            view_name="missing_dates",
        )
        service.run_results_links_scraper()

    def ingest_tf_results_links_data(self):
        service = ResultLinksScraperService(
            scraper=TFResultsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="days_results_links",
            view_name="missing_dates",
        )
        service.run_results_links_scraper()

    # Racecards Links
    def ingest_rp_racecards_links_data(self):
        service = RacecardsLinksScraperService(
            scraper=RPRacecardsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="days_racecards_links",
            view_name="missing_dates",
        )
        service.run_racecard_links_scraper()

    def ingest_tf_racecards_links_data(self):
        service = RacecardsLinksScraperService(
            scraper=TFRacecardsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="days_racecards_links",
            view_name="missing_dates",
        )
        service.run_racecard_links_scraper()

    # UK/IRE Results Data
    def ingest_uk_ire_rp_results_data(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(source="rp", mapping=RP_UKE_IRE_COURSE_IDS),
            schema="rp_raw",
            table_name="performance_data",
            view_name=self.missing_data_views.uk_ire_performance_data,
        )
        service.run_results_scraper()

    def ingest_uk_ire_tf_results_data(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(source="tf", mapping=TF_UKE_IRE_COURSE_IDS),
            schema="tf_raw",
            table_name="performance_data",
            view_name=self.missing_data_views.uk_ire_performance_data,
            login=True,
        )
        service.run_results_scraper()

    # Non-UK/IRE Results Data
    def ingest_non_uk_ire_rp_results_data(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(
                source="rp", mapping=RP_NON_UKE_IRE_COURSE_IDS
            ),
            schema="rp_raw",
            table_name="non_uk_ire_performance_data",
            view_name=self.missing_data_views.non_uk_ire_performance_data,
        )
        service.run_results_scraper()

    def ingest_non_uk_ire_tf_results_data(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(
                source="tf", mapping=TF_NON_UKE_IRE_COURSE_IDS
            ),
            schema="tf_raw",
            table_name="non_uk_ire_performance_data",
            view_name=self.missing_data_views.non_uk_ire_performance_data,
            login=True,
        )
        service.run_results_scraper()

    # Racecards Data
    def ingest_rp_racecards_data(self):
        service = RacecardsDataScraperService(
            scraper=RPRacecardsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="todays_performance_data",
            view_name="days_racecards_links",
        )
        service.run_racecards_scraper()

    def ingest_tf_racecards_data(self):
        service = RacecardsDataScraperService(
            scraper=TFRacecardsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="todays_performance_data",
            view_name="days_racecards_links",
        )
        service.run_racecards_scraper()

    # Betfair Historical Data
    def ingest_historical_betfair_data(self):
        service = HistoricalBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            betfair_data_processor=BetfairDataProcessor(self.config),
            s3_client=get_storage_client("s3"),
            data_dao=self.dao,
        )
        service.run_data_ingestion()

    def ingest_todays_betfair_data(self):
        service = TodaysBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            data_dao=self.dao,
        )
        service.run_data_ingestion()


def run_ingestion_pipeline():
    config = Config()
    dao_map = {
        "LOCAL": PostgresDao(),
        "CLOUD": S3Dao(),
    }
    view_map = {
        "LOCAL": Views(
            uk_ire_performance_data="missing_uk_ire_links",
            non_uk_ire_performance_data="missing_non_uk_ire_links",
        ),
        "CLOUD": Views(
            uk_ire_performance_data="days_results_links",
            non_uk_ire_performance_data="days_results_links",
        ),
    }
    dao = dao_map[config.runtime_environment]
    missing_data_views = view_map[config.runtime_environment]
    betfair_client = BetFairClient(
        BetfairCredentials(
            username=config.bf.username,
            password=config.bf.password,
            app_key=config.bf.app_key,
            certs_path=config.bf.certs_path,
        )
    )
    ingestor = IngestionPipeline(
        config=config,
        dao=dao,
        missing_data_views=missing_data_views,
        betfair_client=betfair_client,
    )

    ingest_raw_performance_data_from_s3(environment=config.runtime_environment)

    # Results Links
    pt(
        ingestor.ingest_rp_results_links_data,
        ingestor.ingest_tf_results_links_data,
    )

    # Racecards Links
    pt(
        ingestor.ingest_rp_racecards_links_data,
        ingestor.ingest_tf_racecards_links_data,
    )

    # Racecards Data
    pt(
        ingestor.ingest_rp_racecards_data,
        ingestor.ingest_tf_racecards_data,
    )

    # UK/IRE Results Data
    pt(
        ingestor.ingest_uk_ire_rp_results_data,
        ingestor.ingest_uk_ire_tf_results_data,
    )

    # Non-UK/IRE Results Data
    pt(
        ingestor.ingest_non_uk_ire_rp_results_data,
        ingestor.ingest_non_uk_ire_tf_results_data,
    )

    # Betfair Data
    pt(
        ingestor.ingest_historical_betfair_data,
        ingestor.ingest_todays_betfair_data,
    )


if __name__ == "__main__":
    run_ingestion_pipeline()
