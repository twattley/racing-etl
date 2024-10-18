import pandas as pd
from api_helpers.clients.postgres_client import PostgresClient

from src.raw.interfaces.raw_data_dao import IRawDataDao
from src.storage.storage_client import get_storage_client


class PostgresDao(IRawDataDao):
    def __init__(self) -> None:
        self.postgres_client: PostgresClient = get_storage_client("postgres")

    def fetch_dates(self, schema: str, view_name: str) -> list[str]:
        return self.postgres_client.fetch_data(f"SELECT date FROM {schema}.{view_name}")

    def fetch_links(self, schema: str, table_name: str) -> pd.DataFrame:
        return self.postgres_client.fetch_data(
            f"SELECT link_url FROM {schema}.{table_name}"
        )

    def store_links(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        self.postgres_client.store_data(data, table_name, schema)

    def store_data(
        self, schema: str, table_name: str, data: pd.DataFrame, truncate: bool = False
    ) -> None:
        self.postgres_client.store_data(data, table_name, schema, truncate)

    def upsert_data(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        self.postgres_client.upsert_data(data, schema, table_name, ["unique_id"])
