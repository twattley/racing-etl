import pandas as pd
from api_helpers.clients.betfair_client import BetFairClient
from api_helpers.helpers.logging_config import I

from src.config import Config
from src.raw.interfaces.raw_data_dao import IRawDataDao


class TodaysBetfairDataService:
    def __init__(
        self,
        config: Config,
        betfair_client: BetFairClient,
        data_dao: IRawDataDao,
    ):
        self.config = config
        self.betfair_client = betfair_client
        self.data_dao = data_dao

    def run_data_ingestion(self):
        I("Fetching todays market data")
        data = self.betfair_client.create_market_data()
        data = data.assign(
            created_at=pd.Timestamp.now(),
            race_date=data["race_time"].dt.date,
        )
        I(f"Found {data.shape[0]} markets")
        win_and_place = (
            pd.merge(
                data[data["market"] == "WIN"],
                data[data["market"] == "PLACE"],
                on=["race_time", "course", "todays_bf_unique_id"],
                suffixes=("_win", "_place"),
            )
            .rename(
                columns={
                    "horse_win": "horse_name",
                    "todays_bf_unique_id": "horse_id",
                    "last_traded_price_win": "betfair_win_sp",
                    "last_traded_price_place": "betfair_place_sp",
                    "status_win": "status",
                    "created_at_win": "created_at",
                    "race_date_win": "race_date",
                }
            )
            .filter(
                items=[
                    "race_time",
                    "race_date",
                    "horse_id",
                    "horse_name",
                    "course",
                    "betfair_win_sp",
                    "betfair_place_sp",
                    "created_at",
                    "status",
                    "market_id_win",
                    "market_id_place",
                ]
            )
            .sort_values(by="race_time", ascending=True)
        )
        self.data_dao.store_data("bf_raw", "todays_price_data", win_and_place)
