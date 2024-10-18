import bz2
import hashlib
import json
from calendar import monthrange
from datetime import date, datetime, timezone
from io import StringIO

import numpy as np
import pandas as pd
import pytz
from api_helpers.helpers.logging_config import D, I

from src.config import Config


class BetfairDataProcessor:
    def __init__(
        self,
        config: Config,
    ):
        self.config = config

    def process_data(self, market_data: list[dict], filename: str) -> pd.DataFrame:
        opening_data = BetfairDataProcessor.get_market_data(market_data)
        sp_dict = BetfairDataProcessor.get_sp_data(market_data, opening_data)
        market_def = market_data[0]["mc"][0]["marketDefinition"]
        market_time = pd.to_datetime(market_def["marketTime"], utc=True)
        start_time = datetime(
            market_time.year,
            market_time.month,
            market_time.day,
            10,
            00,
            tzinfo=pytz.utc,
        )
        market = BetfairDataProcessor.create_market_dataset(
            market_data, opening_data, sp_dict
        )
        df = BetfairDataProcessor.remove_early_nr(market, start_time)
        if "REMOVED" not in df.status.unique():
            df = BetfairDataProcessor.create_percentage_moves(df)
            df = BetfairDataProcessor.create_price_change_dataset(df)
        else:
            df = BetfairDataProcessor.create_price_change_dataset_nrs(df)
        df = df.assign(
            filename=filename,
            unique_id=lambda x: x.apply(
                lambda y: hashlib.sha512(
                    f"{y['runner_id']}{y['filename']}".encode()
                ).hexdigest(),
                axis=1,
            ),
            race_date=df["race_time"].dt.date,
        )

        return df

    @staticmethod
    def check_abandoned(market_updates: list[dict]) -> bool:
        return {
            runner["status"]
            for runner in market_updates[-1]["mc"][0]["marketDefinition"]["runners"]
        } == {"REMOVED"}

    @staticmethod
    def get_sp_data(
        market_updates: list[dict], opening_prices: pd.DataFrame
    ) -> pd.DataFrame:
        sp_data = pd.DataFrame(
            market_updates[-1]["mc"][0]["marketDefinition"]["runners"]
        )[["bsp", "id", "name", "status"]]
        sp_data["course"] = opening_prices["course"]
        sp_data["race_time"] = opening_prices["race_time"]
        sp_data["race_type"] = opening_prices["race_type"]
        sp_data["market_change_time"] = sp_data["race_time"]
        sp_data["removal_update"] = False
        sp_data.rename(
            columns={
                "id": "runner_id",
                "name": "runner_name",
                "bsp": "price",
            },
            inplace=True,
        )

        return sp_data

    @staticmethod
    def get_market_data(market_updates: list[dict]) -> pd.DataFrame:
        market_def = market_updates[0]["mc"][0]["marketDefinition"]
        race_time = pd.to_datetime(market_def["marketTime"], utc=True)
        start_time = datetime.fromtimestamp(market_updates[0]["pt"] / 1000.0).replace(
            tzinfo=pytz.utc
        )

        return pd.DataFrame(
            [
                {
                    "course": market_def["venue"],
                    "race_time": race_time,
                    "race_type": market_def["name"],
                    "runner_id": runner["id"],
                    "runner_name": runner["name"],
                    "price": np.nan,
                    "market_change_time": start_time,
                    "status": runner["status"],
                    "removal_update": False,
                }
                for runner in market_def["runners"]
            ]
        )

    @staticmethod
    def remove_early_nr(market: pd.DataFrame, start_time: pd.Timestamp) -> pd.DataFrame:
        early_nr = list(
            market[
                (market["status"] == "REMOVED")
                & (market["market_change_time"] <= start_time)
            ]["runner_name"].unique()
        )
        return market[
            ~(market["runner_name"].isin(early_nr))
            & (market["market_change_time"] >= start_time)
        ]

    @staticmethod
    def create_market_dataset(
        market_updates: list[dict], opening_data: pd.DataFrame, sp_dict: dict
    ) -> pd.DataFrame:
        race_time = opening_data["race_time"].iloc[0]
        runner_map = dict(zip(opening_data["runner_id"], opening_data["runner_name"]))
        market = []
        removals = []
        for change in market_updates[1:]:
            market_change_time = datetime.fromtimestamp(
                change["pt"] / 1000.0, timezone.utc
            )
            if market_change_time >= race_time:
                break
            for runner in change["mc"]:
                if "marketDefinition" not in runner.keys():
                    changes = runner["rc"]
                    for change in changes:
                        runner_id = change["id"]
                        runner_name = runner_map[runner_id]
                        price = change["ltp"]
                        market.append(
                            {
                                "course": np.nan,
                                "race_time": np.nan,
                                "race_type": np.nan,
                                "runner_id": runner_id,
                                "runner_name": runner_name,
                                "price": price,
                                "market_change_time": market_change_time,
                                "removal_update": False,
                            }
                        )
                elif "marketDefinition" in runner.keys():
                    market_def = runner["marketDefinition"]
                    for runner in market_def["runners"]:
                        removal_update_indicator = (
                            runner["status"] == "REMOVED"
                            and runner["name"] not in removals
                        )
                        runner_dict = {
                            "course": market_def["venue"],
                            "race_time": race_time,
                            "race_type": market_def["name"],
                            "runner_id": runner["id"],
                            "runner_name": runner["name"],
                            "status": runner["status"],
                            "price": np.nan,
                            "market_change_time": market_change_time,
                            "removal_update": removal_update_indicator,
                        }
                        market.append(runner_dict)
        market = (
            pd.concat([pd.DataFrame(market), opening_data, sp_dict])
            .sort_values("market_change_time")
            .drop_duplicates()
        )

        market["course"] = market["course"].ffill()
        market["course"] = market["course"].bfill()
        market["race_time"] = market["race_time"].ffill()
        market["race_time"] = market["race_time"].bfill()
        market["race_type"] = market["race_type"].ffill()
        market["race_type"] = market["race_type"].bfill()
        market["market_change_time"] = market["market_change_time"].ffill()
        market["market_change_time"] = market["market_change_time"].bfill()

        market["price"] = market.groupby("runner_id")["price"].bfill()
        market["price"] = market.groupby("runner_id")["price"].ffill()
        market["status"] = market.groupby("runner_id")["status"].ffill()
        market = market.reset_index(drop=True)
        return market

    @staticmethod
    def create_percentage_moves(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(by="market_change_time")
        df = df.assign(
            min_price=df.groupby(["runner_id"])["price"].transform("min"),
            max_price=df.groupby(["runner_id"])["price"].transform("max"),
            latest_price=df.groupby(["runner_id"])["price"].transform("last"),
            earliest_price=df.groupby(["runner_id"])["price"].transform("first"),
        )

        df = df.assign(
            price_change=round(
                ((100 / df["earliest_price"]) - (100 / df["latest_price"])), 2
            )
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0)
        )
        return df.drop_duplicates(subset=["runner_id"])

    @staticmethod
    def create_unique_ids(df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            bf_race_key=df["course"] + df["race_time"].astype(str).str[:-6],
            bf_race_horse_key=(
                df["course"] + df["race_time"].astype(str).str[:-6] + df["runner_name"]
            ),
        )
        df = df.assign(
            race_key=df["bf_race_key"].apply(
                lambda x: hashlib.sha512(x.encode("utf-8")).hexdigest()
            ),
            bf_unique_id=df["bf_race_horse_key"].apply(
                lambda x: hashlib.sha512(x.encode("utf-8")).hexdigest()
            ),
        ).drop(columns=["bf_race_key", "bf_race_horse_key"])

        return df

    @staticmethod
    def create_price_change_dataset(df: pd.DataFrame) -> pd.DataFrame:
        D("Creating dataset of price changes without non runners")
        price_changes = []
        for horse in df.runner_name.unique():
            horse_df = (
                df[df.runner_name == horse]
                .drop_duplicates(subset=["runner_id"])
                .assign(non_runners=False)
            )
            price_changes.append(
                {
                    "horse": horse_df["runner_name"].iloc[0],
                    "course": horse_df["course"].iloc[0],
                    "race_time": horse_df["race_time"].iloc[0],
                    "race_type": horse_df["race_type"].iloc[0],
                    "runner_id": horse_df["runner_id"].iloc[0],
                    "race_key": horse_df["race_key"].iloc[0],
                    "bf_unique_id": horse_df["bf_unique_id"].iloc[0],
                    "min_price": horse_df["min_price"].iloc[0],
                    "max_price": horse_df["max_price"].iloc[0],
                    "latest_price": horse_df["latest_price"].iloc[0],
                    "earliest_price": horse_df["earliest_price"].iloc[0],
                    "price_change": horse_df["price_change"].iloc[0],
                    "non_runners": horse_df["non_runners"].iloc[0],
                }
            )
        return pd.DataFrame(price_changes)

    @staticmethod
    def get_final_starters(df: pd.DataFrame) -> list:
        return list(
            df[
                (df["race_time"] == df["market_change_time"])
                & (df["status"] != "REMOVED")
            ]["runner_name"].unique()
        )

    @staticmethod
    def get_removals(df: pd.DataFrame) -> dict:
        removals = {}
        for i in df.itertuples():
            if i.status == "REMOVED" and i.runner_name not in removals.keys():
                removals[i.runner_name] = i.Index

        return removals

    @staticmethod
    def create_price_change_dataset_nrs(df: pd.DataFrame) -> pd.DataFrame:
        I("Creating dataset of price changes with non runners")
        removals = BetfairDataProcessor.get_removals(df)
        split_dfs = BetfairDataProcessor.split_dataframe_by_removals(df)
        changes = []
        for df in split_dfs:
            df = BetfairDataProcessor.create_percentage_moves(df)
            changes.append(df.drop_duplicates(subset=["runner_id"], keep="last"))
        changes = pd.concat(changes)
        changes = changes[~changes.runner_name.isin(removals.keys())]
        changes["price_change"] = changes.groupby(["runner_name"])[
            "price_change"
        ].transform("sum")
        price_changes = []
        for horse in changes.runner_name.unique():
            horse_df = changes[changes.runner_name == horse]
            price_changes.append(
                {
                    "horse": horse_df["runner_name"].iloc[0],
                    "course": horse_df["course"].iloc[0],
                    "race_time": horse_df["race_time"].iloc[0],
                    "race_type": horse_df["race_type"].iloc[0],
                    "runner_id": horse_df["runner_id"].iloc[0],
                    "race_key": horse_df["race_key"].iloc[0],
                    "bf_unique_id": horse_df["bf_unique_id"].iloc[0],
                    "min_price": np.nan,
                    "max_price": np.nan,
                    "latest_price": np.nan,
                    "earliest_price": np.nan,
                    "price_change": horse_df["price_change"].iloc[0],
                    "non_runners": True,
                }
            )
        return pd.DataFrame(price_changes)

    @staticmethod
    def split_dataframe_by_removals(df: pd.DataFrame) -> list[pd.DataFrame]:
        market_changes = {
            i.market_change_time for i in df.itertuples() if i.removal_update
        }
        df = df[~df["market_change_time"].isin(market_changes)]
        sublists = [[df.index[0]]]
        for i in range(1, len(df.index)):
            if df.index[i] - df.index[i - 1] == 1:
                sublists[-1].append(df.index[i])
            else:
                sublists.append([df.index[i]])
        return [df.loc[i[0] : i[-1]] for i in sublists]

    @staticmethod
    def decode_betfair_json_data(content: list[str]) -> list[dict]:
        tmp_buffer = StringIO()
        json.dump(content, tmp_buffer)
        tmp_buffer.seek(0)
        data = json.load(tmp_buffer)
        return [json.loads(i) for i in data]

    @staticmethod
    def open_compressed_file(file: str) -> list[dict]:
        with bz2.open(
            file,
            "rt",
        ) as f:
            content = f.read()
            content = content.strip().split("\n")
        return BetfairDataProcessor.decode_betfair_json_data(content)

    @staticmethod
    def get_last_day_in_month():
        today = date.today()
        _, last_day = monthrange(today.year, today.month)
        last_day = date(today.year, today.month, last_day)
        day = last_day.day
        month = last_day.month
        year = last_day.year
        return day, month, year
