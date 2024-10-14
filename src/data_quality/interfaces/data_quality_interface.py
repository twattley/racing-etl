from typing import Protocol
from src.storage.storage_client import PostgresClient


class IDataQualityInterface(Protocol):
    def __init__(self, postgres_client: PostgresClient): ...

    def check_data_quality(self) -> bool: ...
