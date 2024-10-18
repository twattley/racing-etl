from typing import Protocol

import pandas as pd
from api_helpers.interfaces.storage_client_interface import IStorageClient


class IEntityRefData(Protocol):
    def __init__(self, datastore: IStorageClient): ...

    def get_data(self, *args, **kwargs) -> pd.DataFrame: ...

    def add_new_enties(self, *args, **kwargs) -> pd.DataFrame: ...
