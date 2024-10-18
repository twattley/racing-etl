from typing import Literal

import pandas as pd


class BetfairCache:
    CACHE_PATH = "./data/betfair/cache"

    def __init__(self):
        self.processed_files = set()
        self.last_processed_date = pd.Timestamp.min

    def file_processed(self, filename: str) -> bool:
        return filename in self.processed_files

    def load_cache(self) -> pd.DataFrame:
        self.error_files = pd.read_parquet(f"{self.CACHE_PATH}/error_files.parquet")
        self.processed_files = pd.read_parquet(
            f"{self.CACHE_PATH}/processed_files.parquet"
        )

        self.cached_files_data = pd.concat(
            [self.error_files, self.processed_files[["filename"]]]
        )

        self.cached_files = set(self.cached_files_data["filename"].unique())
        self.last_processed_date = max(self.processed_files["race_date"])

    def store_data(self, file_type: Literal["error", "processed"], data: pd.DataFrame):
        if file_type == "error":
            self.error_files = pd.concat([self.error_files, data])
            self.error_files.to_parquet(f"{self.CACHE_PATH}/error_files.parquet")
        elif file_type == "processed":
            self.processed_files = pd.concat([self.processed_files, data])
            self.processed_files.to_parquet(
                f"{self.CACHE_PATH}/processed_files.parquet"
            )
