from api_helpers.clients.betfair_client import BetFairClient

from src.config import Config
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from src.raw.betfair.fetch_todays_data import TodaysBetfairDataService
from src.raw.services.historical_betfair_ingestor import HistoricalBetfairDataService
from src.raw.betfair.betfair_cache import BetfairCache
from api_helpers.interfaces.storage_client_interface import IStorageClient


class BFIngestor:
    def __init__(
        self,
        config: Config,
        betfair_client: BetFairClient,
        storage_client: IStorageClient,
    ):
        self.config = config
        self.betfair_client = betfair_client
        self.storage_client = storage_client

    def ingest_results_data(self):
        service = HistoricalBetfairDataService(
            config=self.config,
            betfair_cache=BetfairCache(),
            betfair_client=self.betfair_client,
            betfair_data_processor=BetfairDataProcessor(self.config),
            storage_client=self.storage_client,
        )
        service.run_data_ingestion()

    def ingest_todays_data(self):
        service = TodaysBetfairDataService(
            config=self.config,
            betfair_client=self.betfair_client,
            storage_client=self.storage_client,
        )
        service.run_data_ingestion()
