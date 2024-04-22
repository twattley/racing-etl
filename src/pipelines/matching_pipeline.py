from datetime import datetime
from typing import Literal

import pandas as pd

from src.entity_matching.matcher import (
    entity_match,
    store_matching_results,
    store_owner_data,
)
from src.storage.sql_db import call_procedure, fetch_data, insert_records
from src.utils.logging_config import I, W
from src.utils.processing_utils import ptr

MATCHING_DATA_FOLDER = "./src/data"
STRING_DATE_NOW = datetime.now().strftime("%Y-%m-%d")

def post_matching_data_checks():
    todays_rp_raw, todays_staging = ptr(
            lambda: fetch_data("SELECT * FROM rp_raw.performance_data;"),
            lambda: fetch_data("SELECT * FROM staging.joined_todays_performance_data;"),
        )
    
    number_of_raw_records = len(todays_rp_raw)
    number_of_staging_records = len(todays_staging)

    if number_of_raw_records != number_of_staging_records:
        W(f"Number of records in staging ({number_of_staging_records}) does not match number of records in raw ({number_of_raw_records})")
    else:
        I(f"Number of records in staging ({number_of_staging_records}) matches number of records in raw ({number_of_raw_records})")

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
    I(f"Found {len(tf_matching_data)} records for dates :{tf_matching_data['race_date'].unique()}")

    matched, unmatched = entity_match(tf_matching_data, rp_matching_data)
    store_matching_results(matched, unmatched)
    ptr(
        lambda: call_procedure("insert_into_joined_performance_data", "staging"),
        lambda: call_procedure("insert_into_joined_todays_performance_data", "staging"),
    )
    post_matching_data_checks()




if __name__ == "__main__":
    run_matching_pipeline()
