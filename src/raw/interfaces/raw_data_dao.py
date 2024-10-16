from typing import Protocol

import pandas as pd


class IRawDataDao(Protocol):
    def fetch_dates(self, schema: str, view_name: str) -> pd.DataFrame:
        """
        Return a list of dates for urls.
        """

    def fetch_links(self, schema: str, view_name: str) -> pd.DataFrame:
        """
        Return a list of links to the results
        """

    def store_links(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        """
        Store the results links.
        """

    def store_data(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        """
        Store the results links.
        """

    def upsert_data(self, schema: str, table_name: str, data: pd.DataFrame) -> None:
        """
        Upsert the results links.
        """
