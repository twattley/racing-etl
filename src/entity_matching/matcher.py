from typing import Literal, Tuple
from src.storage.sql_db import store_data


import pandas as pd
from fuzzywuzzy import fuzz

from src.utils.logging_config import I
from dataclasses import dataclass


@dataclass
class MatchingData:
    base_set: str
    base_data: pd.DataFrame
    entities_sets: list[
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
    data["filtered_horse_name"] = (
        data["filtered_horse_name"]
        .str.replace("'", "")
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
    )
    for i in ["horse_name", "sire_name", "dam_name", "jockey_name", "trainer_name"]:
        data[i] = (
            data[i]
            .str.replace("'", "")
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.strip()
            .str.title()
        )

    return data


def create_fuzz_scores(
    base_set: pd.DataFrame, matching_set: pd.DataFrame
) -> pd.DataFrame:
    
    base_set.to_csv('~/Desktop/base_set.csv', index=False)
    matching_set.to_csv('~/Desktop/matching_set.csv', index=False)
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

    base_set.to_csv('~/Desktop/pre_processing_base_set.csv', index=False)
    data = base_set[base_set["total_fuzz"] > 480].sort_values(
        by="total_fuzz", ascending=False
    )

    data.to_csv('~/Desktop/post_processing.csv', index=False)

    return data


def fuzzy_match_entities(data: MatchingData) -> pd.DataFrame:
    unmatched = []
    matches = []
    base_set, base_data = data.base_set, data.base_data
    matching_set = "TF"
    base_data = base_data.pipe(format_names)
    for entity_set in data.entities_sets:
        entity, entity_data = entity_set
        if entity_data.empty:
            I(f"No {matching_set} data to match for {entity}")
            continue
        entity_data = entity_data.pipe(format_names)
        I(f"Matching {matching_set} {entity}s to {base_set}")
        number_of_entities = len(entity_data[f"filtered_{entity}_name"].unique())
        I(f"Number of missing {entity}s: {number_of_entities}")
        for entity_name in entity_data[f"filtered_{entity}_name"].unique():
            I(f"Entity name: {entity_name}")
            sub_entity_data = entity_data[
                entity_data[f"filtered_{entity}_name"] == entity_name
            ]
            sub_entity_data .to_csv('~/Desktop/sub_entity_data.csv', index=False)
            sub_base_data = base_data[
                base_data["race_date"].isin(sub_entity_data["race_date"])
            ]
            if sub_base_data.empty:
                I(f"No {base_set.upper()} data found for {entity} {entity_name}")
                continue
            sub_base_data = create_fuzz_scores(sub_base_data, sub_entity_data)
            best_match = sub_base_data.sort_values(
                by="total_fuzz", ascending=False
            ).head(1)
            if not best_match.empty:
                I(f"Found match for {entity} {entity_name}")
                matches.append(
                    {
                        "entity_name": f"{entity}",
                        f"{base_set.lower()}_id": sub_entity_data[f"{entity}_id"].iloc[
                            0
                        ],
                        "name": sub_entity_data[f"{entity}_name"].iloc[0],
                        f"{matching_set.lower()}_id": best_match[f"{entity}_id"].iloc[
                            0
                        ],
                    }
                )
            else:
                unmatched.append(
                    {
                        "entity": f"{entity}",
                        'race_timestamp': sub_entity_data['race_timestamp'].iloc[0],
                        "name": sub_entity_data[f"{entity}_name"].iloc[0],
                        'debug_link': sub_entity_data['debug_link'].iloc[0]
                    }
                    )
    
    matched = pd.DataFrame(matches)
    unmatched = pd.DataFrame(unmatched)

    return matched, unmatched


def store_matches(matches: pd.DataFrame):
    entities = matches["entity_name"].unique()
    for entity in entities:
        entity_matches = matches[matches["entity_name"] == entity].drop_duplicates(
            subset=["rp_id", "tf_id"]
        )
        I(f"Storing {len(entity_matches)} matches for {entity}")
        store_data(entity_matches, entity, "matches")


def entity_match(entity_maching_data: MatchingData):
    matching_data, base_data = format_names(matching_data, base_data)
    matches = fuzzy_match_entities(entity_maching_data)


if __name__ == "__main__":
    entity_match("dam")
