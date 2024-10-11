from typing import Optional

import pandas as pd
from api_helpers.helpers.logging_config import I, W
from api_helpers.helpers.processing_utils import ptr

from src.storage.storage_client import get_storage_client

db = get_storage_client("postgres")


def process_racing_post_entity_matching() -> (
    tuple[Optional[pd.DataFrame], Optional[str]]
):
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
            lambda: db.call_procedure("insert_into_joined_performance_data", "staging"),
            lambda: db.call_procedure(
                "insert_into_todays_joined_performance_data", "staging"
            ),
        )
        return pd.DataFrame(), None

    if len(missing_dates) == 1:
        missing_dates = f"('{missing_dates[0]}')"

    return rp_matching_data, missing_dates


if __name__ == "__main__":
    process_racing_post_entity_matching()
