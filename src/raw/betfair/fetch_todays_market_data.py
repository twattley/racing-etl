import pandas as pd
from api_helpers.betfair_client import (
    BetfairApiHelper,
    BetFairClient,
    BetfairCredentials,
)

from src.config import config
from src.storage.psql_db import get_db
from src.utils.logging_config import I

db = get_db()


def fetch_todays_market_data():
    I("Fetching todays market data")
    trading_client = BetFairClient(
        BetfairCredentials(
            username=config.bf_username,
            password=config.bf_password,
            app_key=config.bf_app_key,
            certs_path=config.bf_certs_path,
        )
    )
    trading_client.login()
    betfair_service = BetfairApiHelper(trading_client)
    data = betfair_service.create_market_data()
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
    I(f"Found {win_and_place.shape[0]} win and place markets")
    db.execute_query(
        """
        DELETE FROM bf_raw.todays_price_data 
        where race_date < now() - interval '6 days'
        """
    )

    db.store_data(win_and_place, "todays_price_data", "bf_raw")


if __name__ == "__main__":
    fetch_todays_market_data()
