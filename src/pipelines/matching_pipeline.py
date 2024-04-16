from dataclasses import dataclass
from typing import Literal
import pandas as pd
from src.entity_matching.matcher import entity_match
from src.storage.sql_db import call_procedure, fetch_data, store_data
from src.utils.logging_config import E, I
from src.utils.processing_utils import pt, ptr
from datetime import datetime
from src.utils.file_utils import delete_files_in_directory
from src.entity_matching.matcher import MatchingData


MATCHING_DATA_FOLDER = "./src/data"
STRING_DATE_NOW = datetime.now().strftime("%Y-%m-%d")
# ---------------------------------------------------------------


def load_direct_sire_matches():
    call_procedure("insert_formatted_sire_data", "staging")


def load_direct_dam_matches():
    call_procedure("insert_formatted_dam_data", "staging")


def load_direct_horse_matches():
    call_procedure("insert_formatted_horse_data", "staging")


def load_owner_data():
    call_procedure("insert_owner_data", "staging")


# ---------------------------------------------------------------


def load_fuzzy_sire_matches():
    call_procedure("insert_matched_sire_data", "staging")


def load_fuzzy_dam_matches():
    call_procedure("insert_matched_dam_data", "staging")


def load_fuzzy_horse_matches():
    call_procedure("insert_matched_horse_data", "staging")


def load_fuzzy_jockey_matches():
    call_procedure("insert_matched_jockey_data", "staging")


def load_fuzzy_trainer_matches():
    call_procedure("insert_matched_trainer_data", "staging")


# ---------------------------------------------------------------


def refresh_rp_sire_unmatched_data():
    call_procedure("refresh_unmatched_rp_sires", "metrics")


def refresh_rp_dam_unmatched_data():
    call_procedure("refresh_unmatched_rp_dams", "metrics")


def refresh_rp_horse_unmatched_data():
    call_procedure("refresh_unmatched_rp_horses", "metrics")


def refresh_rp_jockey_unmatched_data():
    call_procedure("refresh_unmatched_rp_jockeys", "metrics")


def refresh_rp_trainer_unmatched_data():
    call_procedure("refresh_unmatched_rp_trainers", "metrics")


def refresh_tf_sire_unmatched_data():
    call_procedure("refresh_unmatched_tf_sires", "metrics")


def refresh_tf_dam_unmatched_data():
    call_procedure("refresh_unmatched_tf_dams", "metrics")


def refresh_tf_horse_unmatched_data():
    call_procedure("refresh_unmatched_tf_horses", "metrics")


def refresh_tf_jockey_unmatched_data():
    call_procedure("refresh_unmatched_tf_jockeys", "metrics")


def refresh_tf_trainer_unmatched_data():
    call_procedure("refresh_unmatched_tf_trainers", "metrics")


# ---------------------------------------------------------------


def check_missing_sires():
    rp_missing_sires, tf_missing_sires = ptr(
        lambda: fetch_data("SELECT * FROM metrics.unmatched_rp_sires;"),
        lambda: fetch_data("SELECT * FROM metrics.unmatched_tf_sires;"),
    )
    if rp_missing_sires.empty and tf_missing_sires.empty:
        I("No unmatched sires found")
    else:
        E("Unmatched sires found")


def post_matching_checks():
    I("Checking for unmatched data...")
    rp_unmatched, tf_unmatched = ptr(
        lambda: fetch_data("SELECT * FROM rp_raw.unmatched_links;"),
        lambda: fetch_data("SELECT * FROM tf_raw.unmatched_links;"),
    )


def fetch_base_data_for_matching(
    dataset: Literal["tf", "rp"], from_cache: bool = False
):
    if from_cache:
        I(f"Loading base {dataset} data from cache")
        return pd.read_parquet(
            f"{MATCHING_DATA_FOLDER}/{dataset}/{dataset}_base_data_{STRING_DATE_NOW}.parquet"
        )
    else:
        I(f"Loading base {dataset} data from database")
        delete_files_in_directory(
            f"{MATCHING_DATA_FOLDER}/{dataset}", f"{dataset}_base_data"
        )
        data = fetch_data(f"SELECT * FROM {dataset}_raw.formatted_{dataset}_entities")
        data.to_parquet(
            f"{MATCHING_DATA_FOLDER}/{dataset}/{dataset}_base_data_{STRING_DATE_NOW}.parquet",
            engine="pyarrow",
        )
        return data


def fetch_entites_for_matching(
    dataset: Literal["tf", "rp"], entity: str, from_cache: bool = False
):
    if from_cache:
        I(f"Loading {entity} data from cache")
        return pd.read_parquet(
            f"{MATCHING_DATA_FOLDER}/{dataset}/{dataset}_{entity}_data_{STRING_DATE_NOW}.parquet"
        )
    else:
        I(f"Loading {entity} data from database")
        delete_files_in_directory(
            f"{MATCHING_DATA_FOLDER}/{dataset}", f"{dataset}_{entity}_data"
        )
        data = fetch_data(f"SELECT * FROM {dataset}_raw.unmatched_{entity}s")
        data.to_parquet(
            f"{MATCHING_DATA_FOLDER}/{dataset}/{dataset}_{entity}_data_{STRING_DATE_NOW}.parquet",
            engine="pyarrow",
        )
        return data


def run_matching_pipeline(from_cache=False):

    I("Loading direct matches")

    rp_base_data = fetch_base_data_for_matching("rp", from_cache)

    tf_sire_data, tf_dam_data, tf_horse_data, tf_jockey_data, tf_trainer_data = ptr(
        lambda: fetch_entites_for_matching("tf", "sire", True),
        lambda: fetch_entites_for_matching("tf", "dam", True),
        lambda: fetch_entites_for_matching("tf", "horse", True),
        lambda: fetch_entites_for_matching("tf", "jockey", True),
        lambda: fetch_entites_for_matching("tf", "trainer", True),
    )

    pt(
        load_direct_sire_matches,
        load_direct_dam_matches,
        load_direct_horse_matches,
        load_owner_data,
    )
    entity_matching_data = MatchingData(
        base_set_name="RP",
        base_data=rp_base_data,
        entities_sets=[
            {"sire": tf_sire_data},
            {"dam": tf_dam_data},
            {"horse": tf_horse_data},
            {"jockey": tf_jockey_data},
            {"trainer": tf_trainer_data},
        ],
    )

    matches = entity_match(entity_matching_data)
    store_matches(matches)

    pt(
        load_fuzzy_jockey_matches,
        load_fuzzy_horse_matches,
        load_fuzzy_trainer_matches,
        load_fuzzy_sire_matches,
        load_fuzzy_dam_matches,
    )

    call_procedure("insert_into_joined_performance_data", "staging")

    pt(
        refresh_rp_sire_unmatched_data,
        refresh_rp_dam_unmatched_data,
        refresh_rp_horse_unmatched_data,
        refresh_rp_jockey_unmatched_data,
        refresh_rp_trainer_unmatched_data,
    )

    pt(
        refresh_tf_sire_unmatched_data,
        refresh_tf_dam_unmatched_data,
        refresh_tf_horse_unmatched_data,
        refresh_tf_jockey_unmatched_data,
        refresh_tf_trainer_unmatched_data,
    )


if __name__ == "__main__":
    run_matching_pipeline()
