from calendar import monthrange
from datetime import date

from src.betfair.betfair_client import (
    BetfairClient,
    BetFairConnector,
    BetfairHistoricalDataParams,
)


def get_current_month_range():
    today = date.today()
    first_day = date(today.year, today.month, 1)
    _, last_day = monthrange(today.year, today.month)
    last_day = date(today.year, today.month, last_day)
    return first_day, last_day


def fetch_historical_market_data(params: BetfairHistoricalDataParams):
    trading_client = BetfairClient(client=BetFairConnector())
    trading_client.fetch_historical_market_data(params)


if __name__ == "__main__":
    # first_day, last_day = get_current_month_range()
    # params = BetfairHistoricalDataParams(
    #     from_day=first_day.day,
    #     from_month=first_day.month,
    #     from_year=first_day.year,
    #     to_day=last_day.day,
    #     to_month=last_day.month,
    #     to_year=last_day.year,
    #     market_types_collection=["WIN"],
    #     countries_collection=["GB"],
    #     file_type_collection=["M"],
    # )
    params = BetfairHistoricalDataParams(
        from_day=1,
        from_month=5,
        from_year=2023,
        to_day=31,
        to_month=8,
        to_year=2024,
        market_types_collection=["WIN"],
        countries_collection=["GB", "IE"],
        file_type_collection=["M"],
    )
    fetch_historical_market_data(params)
