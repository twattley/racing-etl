import re
from typing import Optional

import pandas as pd
from api_helpers.helpers.logging_config import I, W
from api_helpers.helpers.processing_utils import ptr

from api_helpers.interfaces.storage_client_interface import IStorageClient
from abc import ABC, abstractmethod


class BaseMatcher(ABC):
    def __init__(
        self,
        storage_client: IStorageClient,
        reference_data: dict[str, pd.DataFrame],
        base_data: pd.DataFrame,
        matching_data: pd.DataFrame,
    ):
        self.db = storage_client
        self.reference_data = reference_data
        self.base_data = base_data
        self.matching_data = matching_data

    def match_data(self) -> pd.DataFrame:
        matched_base_data = self.attempt_already_matched(self.base_data, "rp")
        matched_matching_data = self.attempt_already_matched(self.matching_data, "tf")
        unmatched_base_data = self.base_data[
            ~self.base_data["unique_id"].isin(matched_base_data["unique_id"])
        ]
        unmatched_matching_data = self.matching_data[
            ~self.matching_data["unique_id"].isin(matched_matching_data["unique_id"])
        ]
        unmatched_base_data, unmatched_matching_data = self._format_horse_names(
            (unmatched_base_data, unmatched_matching_data)
        )
        direct_matches = self.attempt_direct_match(
            unmatched_base_data, unmatched_matching_data
        )
        fuzzy_matches = self.attempt_fuzzy_match(
            unmatched_base_data, unmatched_matching_data
        )
        return pd.concat(
            [matched_base_data, matched_matching_data, direct_matches, fuzzy_matches]
        ).drop_duplicates(subset=["unique_id"])

    @staticmethod
    def _clean_entity_name(entity_name: str) -> str:
        remove_country = re.sub(r"\([^)]*\)", "", entity_name).strip()
        remove_non_alpha_characters = re.sub(r"[^a-zA-Z ]", "", remove_country)
        return (
            remove_non_alpha_characters.lower()
            .replace(" ", "")
            .lstrip("0123456789.")
            .strip()
        )

    def _format_horse_names(
        self, datasets: tuple[pd.DataFrame, pd.DataFrame]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        for i in datasets:
            i["cleaned_horse_name"] = i["horse_name"].apply(
                BaseMatcher._clean_entity_name
            )
        return datasets

    def attempt_already_matched(self, data: pd.DataFrame, source: str) -> pd.DataFrame:
        matched_horses = data[
            data["horse_id"].isin(self.reference_data["horse"][f"{source}_id"])
        ]
        matched_trainers = data[
            data["trainer_id"].isin(self.reference_data["trainer"][f"{source}_id"])
        ]
        matched_jockeys = data[
            data["jockey_id"].isin(self.reference_data["jockey"][f"{source}_id"])
        ]
        matched_owners = data[
            data["owner_id"].isin(self.reference_data["owner"][f"{source}_id"])
        ]
        matched_sires = data[
            data["sire_id"].isin(self.reference_data["sire"][f"{source}_id"])
        ]
        matched_dams = data[
            data["dam_id"].isin(self.reference_data["dam"][f"{source}_id"])
        ]

        return pd.concat(
            [
                matched_horses,
                matched_trainers,
                matched_jockeys,
                matched_owners,
                matched_sires,
                matched_dams,
            ]
        ).drop_duplicates(subset=["unique_id"])

    @abstractmethod
    def attempt_direct_match(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def attempt_fuzzy_match(self) -> pd.DataFrame:
        pass

    def partial_join_data(self) -> pd.DataFrame:
        """
        Join data on course race date and horse name

        """
        return self.base_data.merge(
            self.reference_data["course"],
            left_on="course_id",
            right_on="rp_id",
            how="left",
            suffixes=("_rp", "_course"),
        ).merge(
            self.matching_data,
            left_on=["tf_id", "race_date", "cleaned_horse_name"],
            right_on=["course_id", "race_date", "cleaned_horse_name"],
            how="left",
            suffixes=("_rp", "_tf"),
        )

    def join_data(self) -> pd.DataFrame:
        """
        Join data on course race date and horse name

        """
        self.base_data = self.base_data.merge(
            self.reference_data["course"],
            left_on="course_id",
            right_on="rp_id",
            how="left",
        )


class TFMatcher(BaseMatcher):
    """
    Matches data from Racing Post to Timeform
    """

    def __init__(
        self,
        storage_client: IStorageClient,
        reference_data: dict[str, pd.DataFrame],
        base_data: pd.DataFrame,
        matching_data: pd.DataFrame,
    ):
        super().__init__(storage_client, reference_data, base_data, matching_data)
        self.db = storage_client

    def match_data(self) -> tuple[Optional[pd.DataFrame], Optional[str]]:
        I("Loading direct matches")
        (
            rp_sire_data,
            rp_dam_data,
            rp_horse_data,
            rp_jockey_data,
            rp_trainer_data,
            rp_owner_data,
        ) = ptr(
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_sires;"),
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_dams;"),
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_horses;"),
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_jockeys;"),
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_trainers;"),
            lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_owners;"),
        )
        if len(rp_owner_data) == 1 and not rp_owner_data.name.iloc[0]:
            I("None Owner not inserting")
        else:
            db.insert_records("owner", "public", rp_owner_data, ["rp_id"])

        rp_matching_data = pd.concat(
            [
                rp_sire_data.assign(entity_type="sire"),
                rp_dam_data.assign(entity_type="dam"),
                rp_horse_data.assign(entity_type="horse"),
                rp_jockey_data.assign(entity_type="jockey"),
                rp_trainer_data.assign(entity_type="trainer"),
            ]
        )
        missing_dates = tuple(rp_matching_data["race_date"].unique())

        if not missing_dates:
            W("No missing data to match")
            ptr(
                lambda: db.call_procedure(
                    "insert_into_joined_performance_data", "staging"
                ),
                lambda: db.call_procedure(
                    "insert_into_todays_joined_performance_data", "staging"
                ),
            )
            return pd.DataFrame(), None

        if len(missing_dates) == 1:
            missing_dates = f"('{missing_dates[0]}')"

        return rp_matching_data, missing_dates

    def find_fuzzy_matches(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
