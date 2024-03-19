from typing import Tuple

import pandas as pd
from fuzzywuzzy import fuzz

from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I

BREAK_CONDITION  = {
    "horse": 1,
    "jockey": 5,
    "trainer": 5,
    "sire": 5,
    "dam": 5,
}


def fetch_entity_data(entity: str) -> Tuple[pd.DataFrame, pd.DataFrame]:

    tf = fetch_data(f"SELECT * FROM tf_raw.unmatched_{entity}s")
    unique_entities = tf[f"{entity}_id"].unique()
    I(f"Found {len(unique_entities)} unique {entity}s")
    rp = fetch_data("SELECT * FROM rp_raw.formatted_rp_entities")

    return tf, rp


def format_names(
    tf: pd.DataFrame, rp: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:

    tf["jockey_last_name"] = (
        tf["jockey_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(" ", "")
    )
    rp["jockey_last_name"] = (
        rp["jockey_name"]
        .apply(lambda x: x.split(" ")[-1])
        .str.lower()
        .str.replace(" ", "")
    )
    tf["trainer_last_name"] = (
        tf["trainer_name"]
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
    rp["trainer_last_name"] = (
        rp["trainer_name"]
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
    tf["filtered_horse_name"] = (
        tf["filtered_horse_name"]
        .str.replace("'", "")
        .str.strip()
    )
    rp["filtered_horse_name"] = (
        rp["filtered_horse_name"]
        .str.replace("'", "")
        .str.strip()
    )

    return tf, rp


def fuzzy_match_entities(
    tf: pd.DataFrame, rp: pd.DataFrame, entity: str
) -> pd.DataFrame:
    matches = []
    number_of_entities = len(tf[f"filtered_{entity}_name"].unique())
    for i, v in enumerate(tf[f"filtered_{entity}_name"].unique()):
        hits = 0
        I(f"{i}/{number_of_entities}")
        I(f"Entity name: {v}")
        sub_tf = tf[tf[f"filtered_{entity}_name"] == v]
        sub_rp = rp[rp["race_date"].isin(sub_tf["race_date"])]
        if sub_rp.empty:
            I(f"No RP data found for {entity} {v}")
            continue
        for date in sub_tf["race_date"].unique():
            sub_date_tf = sub_tf[(sub_tf["race_date"] == date)].copy()
            sub_date_rp = sub_rp[
                (sub_rp["race_date"] == date)
                & (sub_rp["course_id"] == sub_date_tf["course_id"].iloc[0])
            ].copy()
            if sub_date_rp.empty:
                I(f"No RP data found for {entity} {v} on {date}")
                continue
            sub_date_rp = sub_date_rp.assign(
                fuzz_horse=sub_date_rp["filtered_horse_name"].apply(
                    lambda x: fuzz.ratio(x, sub_date_tf["filtered_horse_name"].iloc[0])
                ),
                fuzz_trainer=sub_date_rp["trainer_last_name"].apply(
                    lambda x: fuzz.ratio(x, sub_date_tf["trainer_last_name"].iloc[0])
                ),
                fuzz_jockey=sub_date_rp["jockey_last_name"].apply(
                    lambda x: fuzz.ratio(x, sub_date_tf["jockey_last_name"].iloc[0])
                ),
                fuzz_sire=sub_date_rp["filtered_sire_name"].apply(
                    lambda x: fuzz.ratio(x, sub_date_tf["filtered_sire_name"].iloc[0])
                ),
                fuzz_dam=sub_date_rp["filtered_dam_name"].apply(
                    lambda x: fuzz.ratio(x, sub_date_tf["filtered_dam_name"].iloc[0])
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
            best_match = sub_date_rp[sub_date_rp["total_fuzz"] >= 480].sort_values(
                by="total_fuzz", ascending=False
            )
            if not best_match.empty:
                I(f"Found match for {entity} {v} on {date}")
                matches.append(
                    {
                        f"tf_{entity}_name": sub_tf[f"{entity}_name"].iloc[0],
                        f"tf_{entity}_id": sub_tf[f"{entity}_id"].iloc[0],
                        f"rp_{entity}_name": best_match[f"{entity}_name"].iloc[0],
                        f"rp_{entity}_id": best_match[f"{entity}_id"].iloc[0],
                        "fuzz_score": best_match["total_fuzz"].iloc[0],
                    }
                )
                hits += 1
                if hits != BREAK_CONDITION[entity]:
                    continue
            except_trainer = sub_date_rp[sub_date_rp["jockey_fuzz"] == 400].sort_values(
                by="jockey_fuzz", ascending=False
            )
            if not except_trainer.empty:
                I(f"Found match for {entity} {v} on {date} except trainer")
                matches.append(
                    {
                        f"tf_{entity}_name": sub_tf[f"{entity}_name"].iloc[0],
                        f"tf_{entity}_id": sub_tf[f"{entity}_id"].iloc[0],
                        f"rp_{entity}_name": except_trainer[f"{entity}_name"].iloc[0],
                        f"rp_{entity}_id": except_trainer[f"{entity}_id"].iloc[0],
                        "fuzz_score": except_trainer["jockey_fuzz"].iloc[0],
                    }
                )
                hits += 1
                if hits != BREAK_CONDITION[entity]:
                    continue
            except_jockey = sub_date_rp[sub_date_rp["trainer_fuzz"] == 400].sort_values(
                by="trainer_fuzz", ascending=False
            )
            if not except_jockey.empty:
                I(f"Found match for {entity} {v} on {date} except jockey")
                matches.append(
                    {
                        f"tf_{entity}_name": sub_tf[f"{entity}_name"].iloc[0],
                        f"tf_{entity}_id": sub_tf[f"{entity}_id"].iloc[0],
                        f"rp_{entity}_name": except_jockey[f"{entity}_name"].iloc[0],
                        f"rp_{entity}_id": except_jockey[f"{entity}_id"].iloc[0],
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
    counts = matches_df[f"tf_{entity}_name"].value_counts()

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


def entity_match(entity: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    I(f"Matching {entity}s")
    tf, rp = fetch_entity_data(entity)
    if tf.empty:
        I(f"No unmatched {entity}s found")
        return
    tf, rp = format_names(tf, rp)
    matches = fuzzy_match_entities(tf, rp, entity)
    if matches.empty:
        return
    store_data(matches, f"{entity}", "staging", truncate=True)


if __name__ == "__main__":
    entity_match("horse")
