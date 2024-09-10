import pandas as pd
from fuzzywuzzy import fuzz, process

from src.storage.psql_db import get_db
from src.utils.logging_config import I, W
from src.utils.processing_utils import pt, ptr

db = get_db()


def process_todays_betfair_entity_matching():
    db.call_procedure("update_horse_betfair_data", "bf_raw")
    I("Starting Betfair matching")
    (
        rp_data,
        bf_data,
    ) = ptr(
        lambda: db.fetch_data(
            """
            SELECT race_timestamp, h.id as horse_id, horse_name, c.id as course_id
            FROM rp_raw.todays_performance_data tpd
            LEFT JOIN horse h ON tpd.horse_id = h.rp_id
			LEFT JOIN course c ON tpd.course_id = c.rp_id
            """
        ),
        lambda: db.fetch_data("SELECT * FROM bf_raw.missing_horses"),
    )
    if bf_data.empty:
        I("No Betfair Data found!")
    else:
        I("Found Betfair Data")
        id_mappings = []
        for row in bf_data.itertuples():
            sub_rp_data = rp_data[
                (rp_data["race_timestamp"] == row.race_time)
                & (rp_data["course_id"] == row.course_id)
            ]
            if sub_rp_data.empty:
                W(f"No RP data found for {row.race_time} {row.course_id}")
                continue
            I(
                f"Found {sub_rp_data.shape[0]} RP data for {row.race_time} {row.course_id}"
            )
            best_match = process.extractOne(
                row.horse_name,
                sub_rp_data["horse_name"],
                scorer=fuzz.ratio,
                processor=lambda x: x.lower().replace(" ", ""),
                score_cutoff=90,
            )
            horse_name = best_match[0]
            horse_id = sub_rp_data.loc[
                sub_rp_data["horse_name"] == horse_name, "horse_id"
            ].iloc[0]
            I(f"Best match for {row.horse_name}: {horse_name} {horse_id}")
            id_mappings.append({"horse_id": horse_id, "bf_horse_id": row.horse_id})

        matched_data = pd.DataFrame(id_mappings)
        unmatched_data = bf_data[~bf_data["horse_id"].isin(matched_data["bf_horse_id"])]
        pt(
            lambda: db.store_data(
                matched_data, "entity_matches", "bf_raw", truncate=True
            ),
            lambda: db.store_data(
                unmatched_data, "unmatched_horses", "bf_raw", truncate=True
            ),
        )
        db.call_procedure("update_horse_betfair_ids", "public")


if __name__ == "__main__":
    process_todays_betfair_entity_matching()
