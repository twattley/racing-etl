from dataclasses import dataclass
from typing import Literal

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials
from api_helpers.clients.postgres_client import PostgresClient
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.processing_utils import pt
from api_helpers.helpers.logging_config import I

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
from src.data_quality.raw.todays_data_quality import TodaysDataQuality


@dataclass
class Views:
    results_data: str
    results_data_world: str


def ingest_data_from_s3(
    schema: str, table: str, storage_type: Literal["upsert", "insert"]
):
    s3_client: S3Client = get_storage_client("s3")
    postgres_client: PostgresClient = get_storage_client("postgres")

    data = s3_client.fetch_data(f"{schema}/{table}.parquet")
    I(f"Ingested {len(data)} rows from {schema}.{table}")

    if storage_type == "upsert":
        I(f"Upserting {len(data)} rows into {schema}.{table}")
        postgres_client.upsert_data(data, table, schema, unique_columns=["unique_id"])
    elif storage_type == "insert":
        I(f"Inserting {len(data)} rows into {schema}.{table}")
        postgres_client.store_data(data, table, schema, truncate=True)


def ingest_raw_performance_data_from_s3(environment: Literal["LOCAL", "CLOUD"]):
    if environment == "CLOUD":
        I("Skipping ingestion of raw performance data in cloud")
        return

    # RACING POST
    pt(
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="results_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="results_data_world",
            storage_type="upsert",
        ),
    )
    pt(
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="todays_data",
            storage_type="insert",
        ),
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="days_racecards_links",
            storage_type="insert",
        ),
    )

    # TIMEFORM
    pt(
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="performance_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="results_data_world",
            storage_type="upsert",
        ),
    )
    pt(
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="todays_data",
            storage_type="insert",
        ),
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="days_racecards_links",
            storage_type="insert",
        ),
    )
    # BETFAIR
    pt(
        lambda: ingest_data_from_s3(
            schema="bf_raw",
            table="historical_price_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="bf_raw",
            table="todays_data",
            storage_type="insert",
        ),
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
    def ingest_results_rp_links(self):
        service = ResultLinksScraperService(
            scraper=RPResultsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="results_links",
            view_name="missing_dates",
        )
        service.run_results_links_scraper()

    def ingest_results_tf_links(self):
        service = ResultLinksScraperService(
            scraper=TFResultsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="results_links",
            view_name="missing_dates",
        )
        service.run_results_links_scraper()

    # Racecards Links
    def ingest_todays_rp_links(self):
        service = RacecardsLinksScraperService(
            scraper=RPRacecardsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="todays_links",
            view_name="missing_dates",
        )
        service.run_racecard_links_scraper()

    def ingest_todays_tf_links(self):
        service = RacecardsLinksScraperService(
            scraper=TFRacecardsLinkScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="todays_links",
            view_name="missing_dates",
        )
        service.run_racecard_links_scraper()

    # UK/IRE Results Data
    def ingest_results_rp_data(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(source="rp", mapping=RP_UKE_IRE_COURSE_IDS),
            schema="rp_raw",
            table_name="results_data",
            view_name=self.missing_data_views.results_data,
        )
        service.run_results_scraper()

    def ingest_results_tf_data(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(source="tf", mapping=TF_UKE_IRE_COURSE_IDS),
            schema="tf_raw",
            table_name="results_data",
            view_name=self.missing_data_views.results_data,
            login=True,
        )
        service.run_results_scraper()

    # Results Data World
    def ingest_results_rp_data_world(self):
        service = ResultsDataScraperService(
            scraper=RPResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(
                source="rp", mapping=RP_NON_UKE_IRE_COURSE_IDS
            ),
            schema="rp_raw",
            table_name="results_data_world",
            view_name=self.missing_data_views.results_data_world,
        )
        service.run_results_scraper()

    def ingest_results_tf_data_world(self):
        service = ResultsDataScraperService(
            scraper=TFResultsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            link_identifier=LinkIdentifier(
                source="tf", mapping=TF_NON_UKE_IRE_COURSE_IDS
            ),
            schema="tf_raw",
            table_name="results_data_world",
            view_name=self.missing_data_views.results_data_world,
            login=True,
        )
        service.run_results_scraper()

    # Racecards Data
    def ingest_todays_rp_data(self):
        service = RacecardsDataScraperService(
            scraper=RPRacecardsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="rp_raw",
            table_name="todays_data",
            view_name="days_racecards_links",
        )
        service.run_racecards_scraper()

    def ingest_todays_tf_data(self):
        service = RacecardsDataScraperService(
            scraper=TFRacecardsDataScraper(),
            data_dao=self.dao,
            driver=WebDriver(self.config),
            schema="tf_raw",
            table_name="todays_data",
            view_name="days_racecards_links",
        )
        service.run_racecards_scraper()

    # Betfair Historical Data
    def ingest_results_bf_data(self):
        service = HistoricalBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            betfair_data_processor=BetfairDataProcessor(self.config),
            s3_client=get_storage_client("s3"),
            data_dao=self.dao,
        )
        service.run_data_ingestion()

    def ingest_todays_bf_data(self):
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
            results_data="missing_results_links",
            results_data_world="missing_world_results_links",
        ),
        "CLOUD": Views(
            results_data="days_results_links",
            results_data_world="days_results_links",
        ),
    }
    dao = dao_map[config.runtime_environment]
    missing_data_views = view_map[config.runtime_environment]
    betfair_client = BetFairClient(
        BetfairCredentials(
            username=config.bf_username,
            password=config.bf_password,
            app_key=config.bf_app_key,
            certs_path=config.bf_certs_path,
        )
    )
    ingestor = IngestionPipeline(
        config=config,
        dao=dao,
        missing_data_views=missing_data_views,
        betfair_client=betfair_client,
    )
    rp_data_quality = TodaysDataQuality(
        postgres_client=get_storage_client("postgres"),
        schema="rp_raw",
        runtime_environment=config.runtime_environment,
    )
    tf_data_quality = TodaysDataQuality(
        postgres_client=get_storage_client("postgres"),
        schema="tf_raw",
        runtime_environment=config.runtime_environment,
    )

    # ingest_raw_performance_data_from_s3(environment=config.runtime_environment)

    # ingestor.ingest_results_rp_links()
    ingestor.ingest_results_tf_links()

    # # Racecards Links
    # ingestor.ingest_todays_rp_links()
    # ingestor.ingest_todays_tf_links()

    # # Racecards Data
    # ingestor.ingest_todays_rp_data()
    # ingestor.ingest_todays_tf_data()

    # # UK/IRE Results Data
    # ingestor.ingest_results_rp_data()
    # ingestor.ingest_results_tf_data()

    # Non-UK/IRE Results Data
    # ingestor.ingest_results_rp_data_world()
    # must process after rp data
    ingestor.ingest_results_tf_data_world()

    # Betfair Data
    # ingestor.ingest_todays_bf_data()
    # ingestor.ingest_results_bf_data()

    # rp_data_quality.check_data_quality()
    # tf_data_quality.check_data_quality()


if __name__ == "__main__":
    run_ingestion_pipeline()
