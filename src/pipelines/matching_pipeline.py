from typing import Literal

import pandas as pd
from src.entity_matching.matcher import entity_match
from src.storage.sql_db import call_procedure, fetch_data, store_data

from src.utils.logging_config import I


def run_matching_pipeline():

    # call_procedure("load_direct_matches", "public")

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

    call_procedure("load_fuzzy_matches", "public")


if __name__ == "__main__":
    run_matching_pipeline()
