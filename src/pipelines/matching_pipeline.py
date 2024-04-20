from datetime import datetime
from typing import Literal

import pandas as pd

from src.entity_matching.matcher import (
    MatchingData,
    entity_match,
    store_matching_results,
    store_owner_data,
)
from src.storage.sql_db import call_procedure, fetch_data, insert_records
from src.utils.logging_config import I, W
from src.utils.processing_utils import ptr

MATCHING_DATA_FOLDER = "./src/data"
STRING_DATE_NOW = datetime.now().strftime("%Y-%m-%d")


def run_matching_pipeline():

    I("Loading direct matches")
    (
        rp_sire_data,
        rp_dam_data,
        rp_horse_data,
        rp_jockey_data,
        rp_trainer_data,
        rp_owner_data,
    ) = ptr(
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_sires;"),
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_dams;"),
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_horses;"),
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_jockeys;"),
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_trainers;"),
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_owners;"),
    )
    store_owner_data(rp_owner_data)
    missing_data = pd.concat(
        [rp_sire_data, rp_dam_data, rp_horse_data, rp_jockey_data, rp_trainer_data]
    )

    missing_dates = tuple(missing_data["race_date"].unique())
    if len(missing_dates) == 1:
        missing_dates = f"('{missing_dates[0]}')"

    if not missing_dates:
        call_procedure("insert_into_joined_performance_data", "staging")
        W("No missing data to match")
        return

    tf_hist_data, tf_present_data = ptr(
        lambda: fetch_data(
            f"SELECT * FROM tf_raw.performance_data WHERE race_date IN {missing_dates}",
        ),
        lambda: fetch_data(
            f"SELECT * FROM rp_raw.performance_data WHERE race_date IN {missing_dates}",
        ),
    )
    tf_matching_data = pd.concat([tf_hist_data, tf_present_data]).drop_duplicates(
        subset="unique_id"
    )

    I(f"Loading matching data for {len(missing_dates)} dates")
    I(f"Found {len(tf_matching_data)} records")

    entity_matching_data = MatchingData(
        tf_data=tf_matching_data,
        rp_data=[
            ("sire", rp_sire_data),
            ("dam", rp_dam_data),
            ("horse", rp_horse_data),
            ("jockey", rp_jockey_data),
            ("trainer", rp_trainer_data),
        ],
    )

    matched, unmatched = entity_match(entity_matching_data)
    store_matching_results(matched, unmatched)
    call_procedure("insert_into_joined_performance_data", "staging")


if __name__ == "__main__":
    run_matching_pipeline()
