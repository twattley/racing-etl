from datetime import datetime, timedelta

import pandas as pd
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.logging_config import I

from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.storage.storage_client import get_storage_client


class S3Dao(IRawDataDao):
    def __init__(self) -> None:
        self.s3_client: S3Client = get_storage_client("s3")

    def fetch_dates(self, schema: str, view_name: str) -> pd.DataFrame:
        now = datetime.now()
        dates = []
        for i in range(2, 6):
            date = now - timedelta(days=i)
            dates.append(date.replace(hour=0, minute=0, second=0, microsecond=0))

        I(f"Dummy Results dates: {dates} for {schema}.{view_name}")
        return pd.DataFrame({"date": dates})

    def fetch_links(self, schema: str, view_name: str) -> pd.DataFrame:
        file_path = f"{schema}/{view_name}.parquet"
        I(f"Fetching results links from {file_path}")
        return self.s3_client.fetch_data(file_path)

    def store_links(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        self.s3_client.store_data(data, f"{schema}/{table_name}.parquet")

    def store_data(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        self.s3_client.store_data(data, f"{schema}/{table_name}.parquet")
