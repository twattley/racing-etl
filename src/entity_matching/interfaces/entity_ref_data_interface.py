from typing import Protocol
from api_helpers.interfaces.storage_client_interface import IStorageClient
import pandas as pd


class IEntityRefData(Protocol):
    def __init__(self, datastore: IStorageClient): ...

    def get_data(self, *args, **kwargs) -> pd.DataFrame: ...

    def add_new_enties(self, *args, **kwargs) -> pd.DataFrame: ...
