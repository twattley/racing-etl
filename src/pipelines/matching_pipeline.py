from src.entity_matching.matcher import entity_match
from src.storage.sql_db import call_procedure, fetch_data, store_data
from src.utils.logging_config import I
from src.utils.processing_utils import execute_stored_procedures

# ---------------------------------------------------------------


def load_direct_sire_matches():
    call_procedure("insert_formatted_sire_data", "staging")


def load_direct_dam_matches():
    call_procedure("insert_formatted_dam_data", "staging")


def load_direct_horse_matches():
    call_procedure("insert_formatted_horse_data", "staging")


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


def run_matching_pipeline():

    execute_stored_procedures(
        load_direct_sire_matches, load_direct_dam_matches, load_direct_horse_matches
    )

    for i in [
        {
            "matching_set": "rp",
            "base_set": "tf",
        },
        {
            "matching_set": "tf",
            "base_set": "rp",
        },
    ]:
        I(f"Matching {i['matching_set']} to {i['base_set']}")
        base_data = fetch_data(
            f"SELECT * FROM {i['base_set']}_raw.formatted_{i['base_set']}_entities"
        )
        for entity in ["jockey", "trainer", "horse", "sire", "dam"]:
            I(f"Matching {entity}s")
            matching_data = fetch_data(
                f"SELECT * FROM {i['matching_set']}_raw.unmatched_{entity}s"
            )
            if matching_data.empty:
                continue
            I(f"Found {len(matching_data[f'{entity}_id'].unique())} unique {entity}s")

            matches = entity_match(
                entity, matching_data, base_data, i["matching_set"], i["base_set"]
            )
            if matches.empty:
                continue
            store_data(matches, f"{entity}", "staging", truncate=True)

    execute_stored_procedures(load_fuzzy_jockey_matches, load_fuzzy_horse_matches)
    execute_stored_procedures(
        load_fuzzy_trainer_matches,
        load_fuzzy_sire_matches,
        load_fuzzy_dam_matches,
    )
    call_procedure("insert_into_joined_performance_data", "staging")


if __name__ == "__main__":
    run_matching_pipeline()
