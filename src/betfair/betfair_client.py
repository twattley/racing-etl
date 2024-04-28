import os
from datetime import datetime, timedelta

import betfairlightweight
import pandas as pd
import requests

from src.utils.logging_config import I
from src.utils.time_utils import get_uk_time_now, make_uk_time_aware


class BetFairConnector:
    def __init__(self):
        self.login_status = False
        self.client = None
        self.check_login_status()

    def login(self):
        self.client = betfairlightweight.APIClient(
            username=os.environ["BF_USERNAME"],
            password=os.environ["BF_PASSWORD"],
            app_key=os.environ["BF_APP_KEY"],
            certs=os.environ["BF_CERTS_PATH"],
        )
        self.client.login(session=requests)
        if not self.client.session_expired:
            I("Betfair login successful")
            self.login_status = True

    def check_login_status(self):
        if not self.client:
            return self._login_status()
        if self.client.session_expired:
            return self._login_status()
        I("Betfair login still valid")

    def _login_status(self):
        self.login()
        self.login_status = True

    def logout(self):
        self.client.logout()
        self.login_status = False


class BetfairClient:
    """
    Wrapper around betfair client
    """

    UK_NOW = get_uk_time_now()

    MARKET_FILTER = betfairlightweight.filters.market_filter(
        event_type_ids=["7"],
        market_countries=["GB", "IRE"],
        market_type_codes=["WIN", "PLACE"],
        market_start_time={
            "to": (datetime.now() + timedelta(days=1))
            .replace(hour=23, minute=59, second=0, microsecond=0)
            .strftime("%Y-%m-%dT%TZ")
        },
    )

    MARKET_PROJECTION = [
        "COMPETITION",
        "EVENT",
        "EVENT_TYPE",
        "MARKET_START_TIME",
        "MARKET_DESCRIPTION",
        "RUNNER_DESCRIPTION",
        "RUNNER_METADATA",
    ]

    PRICE_PROJECTION = betfairlightweight.filters.price_projection(
        price_data=betfairlightweight.filters.price_data(ex_all_offers=True)
    )

    def __init__(self, client: betfairlightweight.APIClient):
        self.trading_client = client

    def create_market_data(self) -> pd.DataFrame:
        markets, runners = self._create_markets_and_runners()
        return self._process_combined_market_data(markets, runners)

    def _create_markets_and_runners(self):

        markets = self.trading_client.client.betting.list_market_catalogue(
            filter=self.MARKET_FILTER,
            market_projection=self.MARKET_PROJECTION,
            max_results=1000,
        )
        I(f"Found {len(markets)} markets")
        runners = {
            runner.selection_id: runner.runner_name
            for market in markets
            for runner in market.runners
        }

        return markets, runners

    def _process_combined_market_data(self, markets, runners) -> pd.DataFrame:
        combined_data = []

        for market in markets:
            if make_uk_time_aware(market.market_start_time) <= self.UK_NOW:
                I(f"Skipping market {market.market_id} already started")
                continue

            market_book = self.trading_client.client.betting.list_market_book(
                market_ids=[market.market_id],
                price_projection=self.PRICE_PROJECTION,
            )

            for book in market_book:
                for runner in book.runners:
                    runner_data = {
                        "race_time": make_uk_time_aware(market.market_start_time),
                        "market": market.description.market_type,
                        "race": market.market_name,
                        "course": market.event.venue,
                        "horse": runners[runner.selection_id],
                        "status": runner.status,
                        "market_id": market.market_id,
                        "todays_bf_unique_id": runner.selection_id,
                        "last_traded_price": runner.last_price_traded,
                        "total_matched": runner.total_matched,
                    }
                    combined_data.append(runner_data)

        return pd.DataFrame(combined_data)
