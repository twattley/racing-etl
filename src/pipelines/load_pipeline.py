from api_helpers.interfaces.storage_client_interface import IStorageClient
from src.load.data_loader_service import DataLoaderService
from src.storage.storage_client import get_storage_client


def run_load_pipeline(storage_client: IStorageClient):
    data_loader = DataLoaderService(storage_client=storage_client)
    data_loader.load_unioned_results_data()


if __name__ == "__main__":
    run_load_pipeline(storage_client=get_storage_client("postgres"))
