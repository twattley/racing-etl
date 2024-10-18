import os
from calendar import monthrange
from datetime import date

import pandas as pd
from api_helpers.clients.betfair_client import (
    BetFairClient,
    BetfairHistoricalDataParams,
)
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.logging_config import E, I
from api_helpers.helpers.processing_utils import pt, ptr

from src.config import Config
from src.raw.betfair.fetch_historical_data import BetfairDataProcessor
from src.raw.interfaces.raw_data_dao import IRawDataDao


class HistoricalBetfairDataService:
    CACHE_PATH = "cache/betfair/historical"
    ERROR_FILES_PATH = "cache/betfair/historical/errors/error_files.parquet"
    PROCESSED_FILES_PATH = (
        f"cache/betfair/historical/{{year}}/processed_files.parquet"  # noqa: F541
    )

    def __init__(
        self,
        config: Config,
        betfair_client: BetFairClient,
        betfair_data_processor: BetfairDataProcessor,
        s3_client: S3Client,
        data_dao: IRawDataDao,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        self.config = config
        self.betfair_client = betfair_client
        self.betfair_data_processor = betfair_data_processor
        self.s3_client = s3_client
        self.data_dao = data_dao
        self.start_date = start_date
        self.end_date = end_date

    def run_data_ingestion(self) -> pd.DataFrame:
        params = self._get_params()
        if params.from_year == params.to_year:
            file_list, error_files_data, processed_files_data = ptr(
                lambda: self.betfair_client.get_files(params),
                lambda: self.s3_client.fetch_data(self.ERROR_FILES_PATH),
                lambda: self.s3_client.fetch_data(
                    self.PROCESSED_FILES_PATH.format(year=params.from_year)
                ),
            )
        else:
            (
                file_list,
                error_files_data,
                processed_files_data_1,
                processed_files_data_2,
            ) = ptr(
                lambda: self.betfair_client.get_files(params),
                lambda: self.s3_client.fetch_data(self.ERROR_FILES_PATH),
                lambda: self.s3_client.fetch_data(
                    self.PROCESSED_FILES_PATH.format(year=params.from_year)
                ),
                lambda: self.s3_client.fetch_data(
                    self.PROCESSED_FILES_PATH.format(year=params.to_year)
                ),
            )
            if processed_files_data_2.empty:
                processed_files_data = processed_files_data_1
            else:
                processed_files_data = pd.concat(
                    [processed_files_data_1, processed_files_data_2]
                )

        if error_files_data.empty and processed_files_data.empty:
            I("No error files found")
            unprocessed_files = file_list - processed_files_data
        else:
            processed_data = pd.concat([error_files_data, processed_files_data])[
                "filename"
            ].unique()
            file_list_set = set(file_list)
            processed_data_set = set(processed_data)
            unprocessed_files = list(file_list_set - processed_data_set)

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
                year = file.split("/")[4]
                raw_data = self.betfair_client.fetch_historical_data(file)
                data = self.betfair_data_processor.open_compressed_file(raw_data)
                if self.betfair_data_processor.check_abandoned(data):
                    I(f"Abandoned market {file}")
                    error_data.append(
                        pd.DataFrame({"filename": [file], "year": [year]})
                    )
                    self._remove_file(file)
                    continue
                processed_data = self.betfair_data_processor.process_data(
                    data, file, year
                )
                I(f"Processed: {len(processed_data)} rows")
                market_data.append(processed_data)
                self._remove_file(file)
            except Exception as e:
                E(f"Error processing file {file}: {e}")
                error_data.append(pd.DataFrame({"filename": [file], "year": [year]}))
                self._remove_file(file)
                continue

        if error_data:
            new_error_data = pd.concat(error_data)
            cached_error_data = pd.concat([error_files_data, new_error_data])
            (self.s3_client.store_data(cached_error_data, self.ERROR_FILES_PATH),)
        if not market_data:
            return

        new_market_data = pd.concat(market_data)
        cached_processed_data = pd.concat([processed_files_data, new_market_data])
        pt(
            lambda: self.data_dao.store_data(
                "bf_raw",
                "historical_price_data_cloud",
                new_market_data.drop(columns=["year"]),
            ),
            lambda: self.s3_client.store_data(
                cached_processed_data,
                self.PROCESSED_FILES_PATH.format(year=params.from_year),
            ),
        )

    def _get_params(self) -> BetfairHistoricalDataParams:
        constants = {
            "market_types_collection": ["WIN"],
            "countries_collection": ["GB", "IE"],
            "file_type_collection": ["M"],
        }
        if self.start_date and self.end_date:
            year, month, day = map(int, self.start_date.split("-"))
            start_date = date(year, month, day)
            year, month, day = map(int, self.end_date.split("-"))
            end_date = date(year, month, day)
            return BetfairHistoricalDataParams(
                from_year=start_date.year,
                from_month=start_date.month,
                from_day=start_date.day,
                to_year=end_date.year,
                to_month=end_date.month,
                to_day=end_date.day,
                **constants,
            )
        else:
            date_params = self.calculate_date_params(date.today())
            return BetfairHistoricalDataParams(
                from_year=date_params["first_year"],
                from_month=date_params["first_month"],
                from_day=date_params["first_day"],
                to_year=date_params["last_year"],
                to_month=date_params["last_month"],
                to_day=date_params["last_day"],
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
            "first_day": first_day,
            "first_month": first_month,
            "first_year": first_year,
            "last_day": last_day,
            "last_month": last_month,
            "last_year": last_year,
        }

    def _remove_file(self, file: str):
        try:
            os.remove(file.split("/")[-1])
        except FileNotFoundError as e:
            E(f"File not found: {e}")
