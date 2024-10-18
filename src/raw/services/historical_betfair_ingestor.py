import os
from calendar import monthrange
from datetime import date

import pandas as pd
from api_helpers.clients.betfair_client import (
    BetFairClient,
    BetfairHistoricalDataParams,
)
from api_helpers.helpers.logging_config import E, I

from src.config import Config
from src.raw.betfair.betfair_cache import BetfairCache
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from api_helpers.interfaces.storage_client_interface import IStorageClient


class HistoricalBetfairDataService:
    SCHEMA = "bf_raw"

    def __init__(
        self,
        config: Config,
        betfair_cache: BetfairCache,
        betfair_client: BetFairClient,
        betfair_data_processor: BetfairDataProcessor,
        storage_client: IStorageClient,
    ):
        self.config = config
        self.betfair_client = betfair_client
        self.betfair_data_processor = betfair_data_processor
        self.storage_client = storage_client
        self.betfair_cache = betfair_cache

    def run_data_ingestion(self) -> pd.DataFrame:
        self.betfair_cache.load_cache()
        params: BetfairHistoricalDataParams = self._get_params(
            self.betfair_cache.last_processed_date
        )
        file_list = self.betfair_client.get_files(params)
        file_list_set = set(file_list)
        unprocessed_files = list(file_list_set - self.betfair_cache.cached_files)

        if not unprocessed_files:
            I("No unprocessed files found, exiting!")
            return

        error_data = []
        market_data = []
        for index, file in enumerate(unprocessed_files):
            I(
                f"Processing file {index + 1} of {len(unprocessed_files)} for {params.from_year}"
            )
            try:
                raw_data = self.betfair_client.fetch_historical_data(file)
                data = self.betfair_data_processor.open_compressed_file(raw_data)
                if self.betfair_data_processor.check_abandoned(data):
                    I(f"Abandoned market {file}")
                    error_data.append(file)
                    self._remove_file(file)
                    continue
                processed_data = self.betfair_data_processor.process_data(data, file)
                I(f"Processed: {len(processed_data)} rows")
                market_data.append(processed_data)
                self._remove_file(file)
            except Exception as e:
                E(f"Error processing file {file}: {e}")
                error_data.append(file)
                self._remove_file(file)
                continue

        if error_data:
            self.betfair_cache.store_data(
                "error", pd.DataFrame({"filename": error_data})
            )

        if error_data and not market_data:
            raise ValueError(
                f"{len(error_data)} errors in this run, check logs for more details"
            )

        market_data = pd.concat(market_data)

        self.storage_client.store_data(
            market_data,
            self.config.db.raw.results_data.data_table,
            self.SCHEMA,
        )
        self.betfair_cache.store_data("processed", market_data)

    def _get_params(self, last_processed_date: date) -> BetfairHistoricalDataParams:
        constants = {
            "market_types_collection": ["WIN"],
            "countries_collection": ["GB", "IE"],
            "file_type_collection": ["M"],
        }
        calculated_params = self.calculate_date_params(last_processed_date)

        return BetfairHistoricalDataParams(
            **calculated_params,
            **constants,
        )

    def calculate_date_params(self, input_date: date) -> dict[str, int]:
        first_day = 1
        if input_date.month == 1:
            first_month = 12
            first_year = input_date.year - 1
        else:
            first_month = input_date.month - 1
            first_year = input_date.year

        _, last_day = monthrange(input_date.year, input_date.month)
        last_month = input_date.month
        last_year = input_date.year

        return {
            "from_day": first_day,
            "from_month": first_month,
            "from_year": first_year,
            "to_day": last_day,
            "to_month": last_month,
            "to_year": last_year,
        }

    def _remove_file(self, file: str):
        try:
            os.remove(file.split("/")[-1])
        except FileNotFoundError as e:
            E(f"File not found: {e}")
