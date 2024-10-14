from typing import Literal
from api_helpers.clients.betfair_client import I
from src.data_quality.interfaces.data_quality_interface import IDataQualityInterface
from src.storage.storage_client import PostgresClient


class ResultsDataQuality(IDataQualityInterface):
    def __init__(
        self, postgres_client: PostgresClient, schema: Literal["rp_raw", "tf_raw"]
    ):
        self.postgres_client = postgres_client
        self.schema = schema

    def check_data_quality(self) -> bool:
        if not self._check_todays_racecard_links():
            I(
                f"Failed to check todays racecard links data quality check in {self.schema}"
            )
            return False
        if not self._check_todays_racecard_data():
            I(f"Failed to check todays racecard data quality check in {self.schema}")
            return False
        if not self._check_links_not_in_todays_data():
            I(
                f"Failed to check links not in todays data quality check in {self.schema}"
            )
            return False
        return True

    def _check_todays_racecard_links(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"SELECT * FROM {self.schema}.days_racecards_links WHERE date = CURRENT_DATE"
        )
        return len(todays_data) > 10

    def _check_todays_racecard_data(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"SELECT * FROM {self.schema}.todays_performance_data WHERE date = CURRENT_DATE"
        )
        return len(todays_data) > 50

    def _check_links_not_in_todays_data(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"""
             SELECT link_url
                FROM {self.schema}.days_racecards_links
                WHERE NOT (link_url IN (SELECT todays_performance_data.debug_link
                        FROM {self.schema}.todays_performance_data)
                    );
            """
        )
        return len(todays_data) > 0
