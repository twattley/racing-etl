from datetime import datetime

import pandas as pd

from src.entity_matching.betfair_matcher import entity_match_betfair
from src.entity_matching.matcher import (
    entity_match,
    store_matching_results,
    store_owner_data,
)
from src.storage.psql_db import get_db

db = get_db()
from src.utils.logging_config import I, W
from src.utils.processing_utils import pt, ptr

MATCHING_DATA_FOLDER = "./src/data"
STRING_DATE_NOW = datetime.now().strftime("%Y-%m-%d")


def insert_into_performance_data():
    ptr(
        lambda: db.call_procedure("insert_into_joined_performance_data", "staging"),
        lambda: db.call_procedure(
            "insert_into_todays_joined_performance_data", "staging"
        ),
    )


def post_matching_data_checks():
    todays_rp_raw, todays_staging = ptr(
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM rp_raw.todays_performance_data;"
        ),
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM staging.todays_joined_performance_data;"
        ),
    )

    number_of_raw_records = len(todays_rp_raw)
    number_of_staging_records = len(todays_staging)

    if number_of_raw_records != number_of_staging_records:
        W(
            f"MISSING DATA RAW: {number_of_raw_records} STAGING: {number_of_staging_records}"
        )
    else:
        I(f"SUCCESS: RAW: {number_of_raw_records} STAGING: {number_of_staging_records}")


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

    # I("Loading direct matches")
    # (
    #     rp_sire_data,
    #     rp_dam_data,
    #     rp_horse_data,
    #     rp_jockey_data,
    #     rp_trainer_data,
    #     rp_owner_data,
    # ) = ptr(
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_sires;"),
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_dams;"),
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_horses;"),
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_jockeys;"),
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_trainers;"),
    #     lambda: db.fetch_data("SELECT * FROM rp_raw.unmatched_owners;"),
    # )
    # store_owner_data(rp_owner_data)
    # rp_matching_data = pd.concat(
    #     [
    #         rp_sire_data.assign(entity_type="sire"),
    #         rp_dam_data.assign(entity_type="dam"),
    #         rp_horse_data.assign(entity_type="horse"),
    #         rp_jockey_data.assign(entity_type="jockey"),
    #         rp_trainer_data.assign(entity_type="trainer"),
    #     ]
    # )

    # missing_dates = tuple(rp_matching_data["race_date"].unique())
    # if len(missing_dates) == 1:
    #     missing_dates = f"('{missing_dates[0]}')"

    # if not missing_dates:
    #     W("No missing data to match")
    #     insert_into_performance_data()
    #     return

    # tf_hist_data, tf_present_data = ptr(
    #     lambda: db.fetch_data(
    #         missing_timeform_query("performance_data", missing_dates)
    #     ),
    #     lambda: db.fetch_data(
    #         missing_timeform_query("todays_performance_data", missing_dates)
    #     ),
    # )
    # tf_matching_data = pd.concat([tf_hist_data, tf_present_data]).drop_duplicates(
    #     subset="unique_id"
    # )

    # I(f"Loading matching data for {len(missing_dates)} dates")
    # I(
    #     f"Found {len(tf_matching_data)} records for dates :{tf_matching_data['race_date'].unique()}"
    # )

    # matched, unmatched = entity_match(tf_matching_data, rp_matching_data)
    # store_matching_results(matched, unmatched)
    # insert_into_performance_data()
    # post_matching_data_checks()

    I("Starting Betfair matching")
    (
        rp_data,
        bf_data,
    ) = ptr(
        lambda: db.fetch_data(
            """
            SELECT race_timestamp, h.id, horse_name, race_id
            FROM rp_raw.todays_performance_data tpd
            LEFT JOIN horse h
            ON tpd.horse_id = h.rp_id
            """
        ),
        lambda: db.fetch_data(
            """
            SELECT race_time, horse_id, horse_name, market_id_win, market_id_place 
            FROM bf_raw.todays_price_data
            """
        ),
    )
    bf_matched = entity_match_betfair(rp_data, bf_data)
    betfair_entities = bf_matched[["id", "name", "bf_id"]]
    market_ids = bf_matched[["race_time", "race_id", "market_id_win", "market_id_place"]].drop_duplicates()

    pt(
        lambda: db.store_data(betfair_entities, "bf_horse", "public", truncate=True),
        lambda: db.store_data(market_ids, "bf_market", "public"),
    )


if __name__ == "__main__":
    run_matching_pipeline()
