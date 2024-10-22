from api_helpers.interfaces.storage_client_interface import IStorageClient
from src.load.generate_query import LoadSQLGenerator

from src.storage.storage_client import get_storage_client


class DataLoaderService:
    def __init__(self, storage_client: IStorageClient):
        self.storage_client = storage_client

    def load_unioned_results_data(self):
        sql = LoadSQLGenerator.get_unioned_results_data_upsert_sql()
        self.storage_client.execute_query(sql)


if __name__ == "__main__":
    data_loader = DataLoaderService(storage_client=get_storage_client("postgres"))
    data_loader.load_unioned_results_data()
