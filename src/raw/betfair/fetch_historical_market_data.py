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


def get_current_month_range():
    today = date.today()
    first_day = date(today.year, today.month, 1)
    _, last_day = monthrange(today.year, today.month)
    last_day = date(today.year, today.month, last_day)
    return first_day, last_day


def fetch_historical_market_data():
    first_day, last_day = get_current_month_range()
    I(f"Fetching historical market data from {first_day} to {last_day}")
    params = BetfairHistoricalDataParams(
        from_day=first_day.day,
        from_month=first_day.month,
        from_year=first_day.year,
        to_day=last_day.day,
        to_month=last_day.month,
        to_year=last_day.year,
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
