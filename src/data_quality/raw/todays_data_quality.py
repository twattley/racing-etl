from typing import Literal

import pandas as pd
from api_helpers.clients.betfair_client import I

from src.data_quality.interfaces.data_quality_interface import IDataQualityInterface
from src.storage.storage_client import PostgresClient


class TodaysDataQuality(IDataQualityInterface):
    def __init__(
        self,
        postgres_client: PostgresClient,
        schema: Literal["rp_raw", "tf_raw"],
        runtime_environment: Literal["LOCAL", "CLOUD"],
    ):
        self.postgres_client = postgres_client
        self.schema = schema
        self.runtime_environment = runtime_environment

    def check_data_quality(self) -> bool:
        if self.runtime_environment == "CLOUD":
            return True
        if not self._check_todays_racecard_links():
            I(
                f"Failed todays racecard links data quality check in {self.schema}.todays_links"
            )
            return False
        I("Racecard Links Check Passed")
        if not self._check_todays_racecard_data():
            I(
                f"Failed todays racecard data quality check in {self.schema}.todays_performance_data"
            )
            return False
        I("Racecard Data Check Passed")
        if not self._check_links_not_in_todays_data():
            I(f"Failed to check links in {self.schema}.performance_data")
            return False
        I("Links in Performance Data Check Passed")
        return True

    def _check_todays_racecard_links(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"""
            SELECT DISTINCT link_url 
            FROM {self.schema}.todays_links 
            WHERE date::date = CURRENT_DATE
            """
        )
        self._record_data_quality_counts(todays_data, "todays_links")
        return len(todays_data) > 10

    def _check_todays_racecard_data(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"""
            SELECT DISTINCT unique_id 
            FROM {self.schema}.todays_performance_data 
            WHERE race_date::date = CURRENT_DATE
            """
        )
        self._record_data_quality_counts(todays_data, "todays_performance_data")
        return len(todays_data) > 50

    def _check_links_not_in_todays_data(self) -> bool:
        I("Check todays data quality")
        todays_data = self.postgres_client.fetch_data(
            f"""
             SELECT link_url
                FROM {self.schema}.todays_links
                WHERE NOT (link_url IN (SELECT todays_performance_data.debug_link
                        FROM {self.schema}.todays_performance_data)
                    );
            """
        )
        return len(todays_data) == 0

    def _record_data_quality_counts(self, data: pd.DataFrame, table_name: str) -> None:
        self.postgres_client.execute_query(
            f"""
            UPDATE data_quality.days_racecards_counts
                SET record_count={len(data)}
                WHERE table_nm='{self.schema}.{table_name}';
            """
        )
