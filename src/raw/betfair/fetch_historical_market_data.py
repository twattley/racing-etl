from calendar import monthrange
from datetime import date

from api_helpers.betfair_client import (
    BetfairApiHelper,
    BetFairClient,
    BetfairCredentials,
    BetfairHistoricalDataParams,
)

from src.config import config
from src.utils.logging_config import I
from src.storage.psql_db import get_db

db = get_db()


def get_last_day_in_month():
    today = date.today()
    _, last_day = monthrange(today.year, today.month)
    last_day = date(today.year, today.month, last_day)
    day = last_day.day
    month = last_day.month
    year = last_day.year
    return day, month, year


def get_max_processed_date_from_db():
    max_date = db.fetch_data(
        """SELECT max(race_time) AS max_processed 
        FROM bf_raw.historical_price_data;
        """
    )
    max_date = max_date["max_processed"].iloc[0].date()
    day = max_date.day + 1
    month = max_date.month
    year = max_date.year
    return day, month, year


def fetch_historical_market_data():
    todays_number = date.today().day
    if todays_number < 5:
        I(
            f"Skipping historical market data fetch for {date.today()} as it's before the 5th"
        )
        return
    last_day, last_month, last_year = get_last_day_in_month()
    first_day, first_month, first_year = get_max_processed_date_from_db()
    I(
        f"Fetching historical market data from {first_day}/{first_month}/{first_year} to {last_day}"
    )
    params = BetfairHistoricalDataParams(
        from_day=first_day,
        from_month=first_month,
        from_year=first_year,
        to_day=last_day,
        to_month=last_month,
        to_year=last_year,
        market_types_collection=["WIN"],
        countries_collection=["GB", "IE"],
        file_type_collection=["M"],
    )
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
    betfair_service.fetch_historical_market_data(params, config.bf_historical_data_path)


if __name__ == "__main__":
    fetch_historical_market_data()
