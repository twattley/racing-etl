from typing import Literal, Tuple

import pandas as pd
from fuzzywuzzy import fuzz

from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I

BREAK_CONDITION = {
    "horse": 1,
    "jockey": 5,
    "trainer": 5,
    "sire": 5,
    "dam": 5,
}


def format_names(
    matching: pd.DataFrame, base: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:

    matching["jockey_last_name"] = (
        matching["jockey_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(" ", "")
    )
    base["jockey_last_name"] = (
        base["jockey_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(" ", "")
    )
    matching["trainer_last_name"] = (
        matching["trainer_name"]
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
    base["trainer_last_name"] = (
        base["trainer_name"]
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
    matching["filtered_horse_name"] = (
        matching["filtered_horse_name"].str.replace("'", "").str.strip()
    )
    base["filtered_horse_name"] = (
        base["filtered_horse_name"].str.replace("'", "").str.strip()
    )

    return matching, base


def fuzzy_match_entities(
    matching: pd.DataFrame,
    base: pd.DataFrame,
    entity: str,
    matching_set: Literal["rp", "tf"],
    base_set: Literal["rp", "tf"],
) -> pd.DataFrame:
    matches = []
    number_of_entities = len(matching[f"filtered_{entity}_name"].unique())
    for i, v in enumerate(matching[f"filtered_{entity}_name"].unique()):
        hits = 0
        I(f"{i}/{number_of_entities}")
        I(f"Entity name: {v}")
        sub_matching = matching[matching[f"filtered_{entity}_name"] == v]
        sub_base = base[base["race_date"].isin(sub_matching["race_date"])]
        if sub_base.empty:
            I(f"No {base_set.upper()} data found for {entity} {v}")
            continue
        for date in sub_matching["race_date"].unique():
            sub_date_matching = sub_matching[(sub_matching["race_date"] == date)].copy()
            I(f"Matching {entity} {v} on {date}")
            I(f'Course ID: {sub_date_matching["course_id"].iloc[0]}')
            sub_date_base = sub_base[
                (sub_base["race_date"] == date)
                & (sub_base["course_id"] == sub_date_matching["course_id"].iloc[0])
            ].copy()
            if sub_date_base.empty:
                I(f"No {base_set.upper()} data found for {entity} {v} on {date}")
                continue
            sub_date_base = sub_date_base.assign(
                fuzz_horse=sub_date_base["filtered_horse_name"].apply(
                    lambda x: fuzz.ratio(
                        x, sub_date_matching["filtered_horse_name"].iloc[0]
                    )
                ),
                fuzz_trainer=sub_date_base["trainer_last_name"].apply(
                    lambda x: fuzz.ratio(
                        x, sub_date_matching["trainer_last_name"].iloc[0]
                    )
                ),
                fuzz_jockey=sub_date_base["jockey_last_name"].apply(
                    lambda x: fuzz.ratio(
                        x, sub_date_matching["jockey_last_name"].iloc[0]
                    )
                ),
                fuzz_sire=sub_date_base["filtered_sire_name"].apply(
                    lambda x: fuzz.ratio(
                        x, sub_date_matching["filtered_sire_name"].iloc[0]
                    )
                ),
                fuzz_dam=sub_date_base["filtered_dam_name"].apply(
                    lambda x: fuzz.ratio(
                        x, sub_date_matching["filtered_dam_name"].iloc[0]
                    )
                ),
            ).assign(
                total_fuzz=lambda x: x["fuzz_horse"]
                + x["fuzz_trainer"]
                + x["fuzz_sire"]
                + x["fuzz_dam"]
                + x["fuzz_jockey"],
                jockey_fuzz=lambda x: x["fuzz_horse"]
                + x["fuzz_sire"]
                + x["fuzz_dam"]
                + x["fuzz_jockey"],
                trainer_fuzz=lambda x: x["fuzz_horse"]
                + x["fuzz_sire"]
                + x["fuzz_dam"]
                + x["fuzz_trainer"],
            )
            best_match = sub_date_base[sub_date_base["total_fuzz"] >= 480].sort_values(
                by="total_fuzz", ascending=False
            )
            if not best_match.empty:
                I(f"Found match for {entity} {v} on {date}")
                matches.append(
                    {
                        f"{matching_set}_{entity}_name": sub_matching[
                            f"{entity}_name"
                        ].iloc[0],
                        f"{matching_set}_{entity}_id": sub_matching[
                            f"{entity}_id"
                        ].iloc[0],
                        f"{base_set}_{entity}_name": best_match[f"{entity}_name"].iloc[
                            0
                        ],
                        f"{base_set}_{entity}_id": best_match[f"{entity}_id"].iloc[0],
                        "fuzz_score": best_match["total_fuzz"].iloc[0],
                    }
                )
                hits += 1
                if hits != BREAK_CONDITION[entity]:
                    continue
            except_trainer = sub_date_base[
                sub_date_base["jockey_fuzz"] == 400
            ].sort_values(by="jockey_fuzz", ascending=False)
            if not except_trainer.empty:
                I(f"Found match for {entity} {v} on {date} except trainer")
                matches.append(
                    {
                        f"{matching_set}_{entity}_name": sub_matching[
                            f"{entity}_name"
                        ].iloc[0],
                        f"{matching_set}_{entity}_id": sub_matching[
                            f"{entity}_id"
                        ].iloc[0],
                        f"{base_set}_{entity}_name": except_trainer[
                            f"{entity}_name"
                        ].iloc[0],
                        f"{base_set}_{entity}_id": except_trainer[f"{entity}_id"].iloc[
                            0
                        ],
                        "fuzz_score": except_trainer["jockey_fuzz"].iloc[0],
                    }
                )
                hits += 1
                if hits != BREAK_CONDITION[entity]:
                    continue
            except_jockey = sub_date_base[
                sub_date_base["trainer_fuzz"] == 400
            ].sort_values(by="trainer_fuzz", ascending=False)
            if not except_jockey.empty:
                I(f"Found match for {entity} {v} on {date} except jockey")
                matches.append(
                    {
                        f"{matching_set}_{entity}_name": sub_matching[
                            f"{entity}_name"
                        ].iloc[0],
                        f"{matching_set}_{entity}_id": sub_matching[
                            f"{entity}_id"
                        ].iloc[0],
                        f"{base_set}_{entity}_name": except_jockey[
                            f"{entity}_name"
                        ].iloc[0],
                        f"{base_set}_{entity}_id": except_jockey[f"{entity}_id"].iloc[
                            0
                        ],
                        "fuzz_score": except_jockey["trainer_fuzz"].iloc[0],
                    }
                )
                hits += 1
                if hits != BREAK_CONDITION[entity]:
                    continue
            hits = 0
            I(f"{BREAK_CONDITION[entity]} attempts, breaking loop")
            break

    matches_df = pd.DataFrame(matches)
    if matches_df.empty:
        I(f"No matches found for {entity}")
        return pd.DataFrame()

    matches = matches_df.drop_duplicates(subset=[f"tf_{entity}_id", f"rp_{entity}_id"])

    matches = matches.rename(
        columns={
            f"tf_{entity}_id": "tf_id",
            f"rp_{entity}_name": "name",
            f"rp_{entity}_id": "id",
        }
    )
    matches = matches.assign(
        name=lambda x: x["name"]
        .str.replace(r"\s*\([^)]*\)", "", regex=True)
        .str.title()
        .str.strip()
    )

    I(f"Found {len(matches)} matches")

    return matches[["id", "name", "tf_id"]]


def entity_match(
    entity: str,
    matching_data: pd.DataFrame,
    base_data: pd.DataFrame,
    matching_set: str,
    base_set: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    I(f"Matching {entity}s")
    if matching_data.empty:
        I(f"No unmatched {entity}s found")
        return
    matching_data, base_data = format_names(matching_data, base_data)
    matches = fuzzy_match_entities(
        matching_data, base_data, entity, matching_set, base_set
    )
    return pd.DataFrame() if matches.empty else matches


if __name__ == "__main__":
    entity_match("dam")
