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

def missing_timeform_query(table, missing_dates):
    return f"""
        SELECT pd.horse_name,
            pd.horse_id,
            pd.horse_age,
            pd.jockey_id,
            pd.jockey_name,
            pd.trainer_id,
            pd.trainer_name,
            pd.sire_name,
            pd.sire_id,
            pd.dam_name,
            pd.dam_id,
            pd.race_timestamp,
            pd.unique_id,
            pd.race_date,
            pd.debug_link,
            c.id AS course_id
        FROM tf_raw.{table} pd
        LEFT JOIN public.course c 
        ON pd.course_id = c.tf_id
        WHERE pd.race_date 
        IN {missing_dates}
        """


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
    if len(missing_dates) == 1:
        missing_dates = f"('{missing_dates[0]}')"

    if not missing_dates:
        call_procedure("insert_into_joined_performance_data", "staging")
        W("No missing data to match")
        return

    tf_hist_data, tf_present_data = ptr(
        lambda: fetch_data(
            missing_timeform_query("performance_data", missing_dates)
        ),
        lambda: fetch_data(
            missing_timeform_query("todays_performance_data", missing_dates)
        ),
    )
    tf_matching_data = pd.concat([tf_hist_data, tf_present_data]).drop_duplicates(
        subset="unique_id"
    )

    I(f"Loading matching data for {len(missing_dates)} dates")
    I(f"Found {len(tf_matching_data)} records")

    matched, unmatched = entity_match(tf_matching_data, rp_matching_data)
    store_matching_results(matched, unmatched)
    call_procedure("insert_into_joined_performance_data", "staging")


if __name__ == "__main__":
    run_matching_pipeline()
