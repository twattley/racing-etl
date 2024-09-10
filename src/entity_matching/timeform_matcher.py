import pandas as pd
from fuzzywuzzy import fuzz

from src.storage.psql_db import get_db
from src.utils.logging_config import I, W
from src.utils.processing_utils import pt, ptr

db = get_db()


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
        jockey_fuzz=lambda x: x["fuzz_horse"]
        + x["fuzz_sire"]
        + x["fuzz_dam"]
        + x["fuzz_jockey"],
        trainer_fuzz=lambda x: x["fuzz_horse"]
        + x["fuzz_sire"]
        + x["fuzz_dam"]
        + x["fuzz_trainer"],
    )

    total_fuzz = base_set[base_set["total_fuzz"] > 480].sort_values(
        by="total_fuzz", ascending=False
    )
    if not total_fuzz.empty:
        return total_fuzz.head(1)

    jockey_fuzz = base_set[base_set["jockey_fuzz"] == 400].sort_values(
        by="jockey_fuzz", ascending=False
    )
    if not jockey_fuzz.empty:
        return jockey_fuzz.head(1)

    trainer_fuzz = base_set[base_set["trainer_fuzz"] == 400].sort_values(
        by="trainer_fuzz", ascending=False
    )
    if not trainer_fuzz.empty:
        return trainer_fuzz.head(1)

    return pd.DataFrame()


def run_entity_matching(
    tf_matching_data: pd.DataFrame, rp_matching_data: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    unmatched = []
    matches = []
    tf_data = tf_matching_data.pipe(format_names)
    rp_data = rp_matching_data.pipe(format_names)
    missing_entities = rp_matching_data["entity_type"].unique()
    I(f"Missing entities: {missing_entities}")
    for entity in missing_entities:
        entity_data = rp_data[rp_data["entity_type"] == entity]
        for filtered_entity_name in entity_data[f"filtered_{entity}_name"].unique():
            sub_rp_data = entity_data[
                entity_data[f"filtered_{entity}_name"] == filtered_entity_name
            ]
            entity_name = sub_rp_data[f"{entity}_name"].iloc[0]
            I(f"Matching {entity_name}")
            sub_tf_data = tf_data[tf_data["race_date"].isin(sub_rp_data["race_date"])]
            if sub_tf_data.empty:
                W(
                    f"No TF data found for {entity}: {entity_name} on dates {sub_rp_data['race_date'].to_list()}"
                )
                continue

            best_match = create_fuzz_scores(sub_tf_data, sub_rp_data)
            if best_match.empty:
                unmatched.append(
                    {
                        "entity": f"{entity}",
                        "race_timestamp": sub_rp_data["race_timestamp"].iloc[0],
                        "name": sub_rp_data[f"{entity}_name"].iloc[0],
                        "debug_link": sub_rp_data["debug_link"].iloc[0],
                    }
                )
            else:
                I(f"Found match for {entity_name}")
                matches.append(
                    {
                        "entity": f"{entity}",
                        "rp_id": sub_rp_data[f"{entity}_id"].iloc[0],
                        "name": sub_rp_data[f"{entity}_name"].iloc[0],
                        "tf_id": best_match[f"{entity}_id"].iloc[0],
                    }
                )

    return pd.DataFrame(matches), pd.DataFrame(unmatched)


def store_matched_data(matched: pd.DataFrame, entity: str) -> None:
    if not matched.empty:
        db.insert_records(
            entity,
            "public",
            matched[matched["entity"] == entity][["rp_id", "name", "tf_id"]],
            ["rp_id", "name", "tf_id"],
        )


def store_unmatched_data(unmatched: pd.DataFrame, entity: str) -> None:
    if not unmatched.empty:
        W(f"Unmatched {entity} data found")
        W(unmatched[unmatched["entity"] == entity])
        db.insert_records(
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


def process_timeform_entity_matching(
    rp_matching_data: pd.DataFrame, missing_dates: str
) -> pd.DataFrame:
    tf_hist_data, tf_present_data = ptr(
        lambda: db.fetch_data(
            missing_timeform_query("performance_data", missing_dates)
        ),
        lambda: db.fetch_data(
            missing_timeform_query("todays_performance_data", missing_dates)
        ),
    )
    tf_matching_data = pd.concat([tf_hist_data, tf_present_data]).drop_duplicates(
        subset="unique_id"
    )
    I(f"Loading matching data for {len(missing_dates)} dates")
    I(
        f"Found {len(tf_matching_data)} records for dates :{tf_matching_data['race_date'].unique()}"
    )

    matched, unmatched = run_entity_matching(tf_matching_data, rp_matching_data)
    store_matching_results(matched, unmatched)
