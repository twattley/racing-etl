from dataclasses import dataclass

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials

from src.config import Config
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from src.raw.daos.s3_dao import S3Dao
from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.raw.services.historical_betfair_data import HistoricalBetfairDataService
from src.storage.storage_client import get_storage_client


@dataclass
class Views:
    uk_ire_performance_data: str
    non_uk_ire_performance_data: str


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

    def ingest_historical_betfair_data(self):
        # Betfair Historical Data
        for start_date, end_date in [
            # ("2016-01-01", "2016-12-31"),
            ("2017-01-01", "2017-12-31"),
            ("2018-01-01", "2018-12-31"),
            ("2019-01-01", "2019-12-31"),
            ("2020-01-01", "2020-12-31"),
            ("2021-01-01", "2021-12-31"),
            ("2022-01-01", "2022-12-31"),
            ("2023-01-01", "2023-12-31"),
            ("2024-01-01", "2024-10-12"),
        ]:
            service = HistoricalBetfairDataService(
                config=self.config,
                betfair_client=self.betfair_client,
                betfair_data_processor=BetfairDataProcessor(self.config),
                s3_client=get_storage_client("s3"),
                data_dao=self.dao,
                start_date=start_date,
                end_date=end_date,
            )
            service.run_data_ingestion()


def run_ingestion_pipeline():
    config = Config()

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
        dao=S3Dao(),
        missing_data_views=Views(
            uk_ire_performance_data="NA",
            non_uk_ire_performance_data="NA",
        ),
        betfair_client=betfair_client,
    )
    ingestor.ingest_historical_betfair_data()


if __name__ == "__main__":
    run_ingestion_pipeline()
