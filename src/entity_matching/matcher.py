from dataclasses import dataclass
from typing import Union
from src.utils.processing_utils import pt

import pandas as pd
from fuzzywuzzy import fuzz

from src.storage.sql_db import fetch_data, insert_records, store_data
from src.utils.logging_config import I, W, E


@dataclass
class MatchingData:
    tf_data: pd.DataFrame
    rp_data: list[
        tuple[str, pd.DataFrame],
        tuple[str, pd.DataFrame],
        tuple[str, pd.DataFrame],
        tuple[str, pd.DataFrame],
        tuple[str, pd.DataFrame],
    ]


def format_names(
    data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    data["jockey_last_name"] = (
        data["jockey_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(" ", "")
    )
    data["trainer_last_name"] = (
        data["trainer_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(
            r"\s*\([^)]*\)|''|, Ireland$|, USA$|, Canada$|, France$|, Germany$",
            "",
            regex=True,
        )
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )
    for i in ["horse", "sire", "dam", "jockey", "trainer"]:
        data[f"{i}_name"] = (
            data[f"{i}_name"]
            .str.replace("'", "")
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.strip()
            .str.title()
        )
        data[f"filtered_{i}_name"] = (
            data[f"{i}_name"]
            .str.replace("'", "")
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.strip()
        )

    return data


def create_fuzz_scores(
    base_set: pd.DataFrame, matching_set: pd.DataFrame
) -> pd.DataFrame:

    base_set = base_set.assign(
        fuzz_horse=base_set["filtered_horse_name"].apply(
            lambda x: fuzz.ratio(x, matching_set["filtered_horse_name"].iloc[0])
        ),
        fuzz_trainer=base_set["trainer_last_name"].apply(
            lambda x: fuzz.ratio(x, matching_set["trainer_last_name"].iloc[0])
        ),
        fuzz_jockey=base_set["jockey_last_name"].apply(
            lambda x: fuzz.ratio(x, matching_set["jockey_last_name"].iloc[0])
        ),
        fuzz_sire=base_set["filtered_sire_name"].apply(
            lambda x: fuzz.ratio(x, matching_set["filtered_sire_name"].iloc[0])
        ),
        fuzz_dam=base_set["filtered_dam_name"].apply(
            lambda x: fuzz.ratio(x, matching_set["filtered_dam_name"].iloc[0])
        ),
    ).assign(
        total_fuzz=lambda x: x["fuzz_horse"]
        + x["fuzz_trainer"]
        + x["fuzz_sire"]
        + x["fuzz_dam"]
        + x["fuzz_jockey"],
    )

    return base_set[base_set["total_fuzz"] > 480].sort_values(
        by="total_fuzz", ascending=False
    )


def fuzzy_match_entities(data: MatchingData) -> pd.DataFrame:
    unmatched = []
    matches = []
    tf_data = data.tf_data.pipe(format_names)
    for rp_entity, rp_data in data.rp_data:
        if rp_data.empty:
            I(f"No missing {rp_entity} rp data!")
            continue
        rp_data = rp_data.pipe(format_names)
        for filtered_entity_name in rp_data[f"filtered_{rp_entity}_name"].unique():
            sub_rp_data = rp_data[
                rp_data[f"filtered_{rp_entity}_name"] == filtered_entity_name
            ]
            entity_name = sub_rp_data[f"{rp_entity}_name"].iloc[0]
            sub_tf_data = tf_data[
                tf_data["race_date"].isin(sub_rp_data["race_date"])
            ]
            if sub_tf_data.empty:
                W(
                    f"No TF data found for {rp_entity}: {entity_name} on dates {sub_rp_data['race_date'].to_list()}"
                )
                continue

            sub_tf_data = create_fuzz_scores(sub_tf_data, sub_rp_data)
            best_match = sub_tf_data.sort_values(
                by="total_fuzz", ascending=False
            ).head(1)
            if best_match.empty:
                unmatched.append(
                    {
                        "entity": f"{rp_entity}",
                        "race_timestamp": sub_rp_data["race_timestamp"].iloc[0],
                        "name": sub_rp_data[f"{rp_entity}_name"].iloc[0],
                        "debug_link": sub_rp_data["debug_link"].iloc[0],
                    }
                )
            else:
                I(f"Found match for {entity_name}")
                matches.append(
                    {
                        "entity": f"{rp_entity}",
                        "rp_id": sub_rp_data[f"{rp_entity}_id"].iloc[0],
                        "name": sub_rp_data[f"{rp_entity}_name"].iloc[0],
                        "tf_id": best_match[f"{rp_entity}_id"].iloc[0],
                    }
                )

    return pd.DataFrame(matches), pd.DataFrame(unmatched)


def entity_match(entity_matching_data: MatchingData):
    return fuzzy_match_entities(entity_matching_data)



def store_owner_data(owner_data: pd.DataFrame) -> None:
    if not owner_data.empty:
        insert_records("owner", "public", owner_data, ["rp_id"])


def store_matched_data(matched: pd.DataFrame, entity: str) -> None:
    if not matched.empty:
        insert_records(
            entity,
            "public",
            matched[matched["entity"] == entity][["rp_id", "name", "tf_id"]],
            ["rp_id", "name", "tf_id"],
        )


def store_unmatched_data(unmatched: pd.DataFrame, entity: str) -> None:
    if not unmatched.empty:
        insert_records(
            "staging_entity_unmatched",
            "errors",
            unmatched[unmatched["entity"] == entity],
            ["entity", "race_timestamp", "name", "debug_link"],
        )


def store_matching_results(matched: pd.DataFrame, unmatched: pd.DataFrame) -> None:
    pt(
        lambda: store_matched_data(matched, "horse"),
        lambda: store_matched_data(matched, "sire"),
        lambda: store_matched_data(matched, "dam"),
        lambda: store_matched_data(matched, "jockey"),
        lambda: store_matched_data(matched, "trainer"),
    )
    pt(
        lambda: store_unmatched_data(unmatched, "horse"),
        lambda: store_unmatched_data(unmatched, "sire"),
        lambda: store_unmatched_data(unmatched, "dam"),
        lambda: store_unmatched_data(unmatched, "jockey"),
        lambda: store_unmatched_data(unmatched, "trainer"),
    )
