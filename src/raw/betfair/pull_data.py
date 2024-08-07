import pandas as pd

from src.betfair.betfair_client import BetfairClient, BetFairConnector
from src.storage.psql_db import get_db

db = get_db()


def process_bf_pull_data():
    trading_client = BetfairClient(client=BetFairConnector())
    data = trading_client.create_market_data()
    data = data.assign(created_at=pd.Timestamp.now())

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
            }
        )
        .filter(
            items=[
                "race_time",
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
    db.store_data(win_and_place, "todays_price_data", "bf_raw", truncate=True)


if __name__ == "__main__":
    process_bf_pull_data()
