from src.storage.sql_db import fetch_data, store_data
from src.utils.processing_utils import ptr
import pandas as pd
from fuzzywuzzy import process

from src.utils.logging_config import W, I


def filter_horse_name(data):
    data["filtered_horse_name"] = (
        data["horse_name"]
        .str.replace("'", "")
        .str.replace(" ", "")
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
        .str.lower()
    )
    return data


def entity_match_betfair(rp: pd.DataFrame, bf: pd.DataFrame) -> pd.DataFrame:
    rp = filter_horse_name(rp)
    bf = filter_horse_name(bf)

    matches = []
    unmatched = []
    for i in bf.itertuples():
        sub_rp = rp[(rp["race_timestamp"] == i.race_time)]
        best_match = process.extractOne(
            i.filtered_horse_name, sub_rp["filtered_horse_name"]
        )
        if not best_match:
            W(f"No match for {i.horse_name}")
            unmatched.append(i.horse_name)
            continue
        if best_match[1] < 90:
            W(f"Match score below 90 for {i.horse_name}, race_time: {i.race_time}")
            W(f'Best match: {best_match}')
            unmatched.append(i.horse_name)
            continue
        rp_data = sub_rp[sub_rp["filtered_horse_name"] == best_match[0]].drop_duplicates()
        if len(rp_data) > 1:
            W(f"Multiple matches for {i.horse_name}, race_time: {i.race_time}")
            unmatched.append(i.horse_name)
            continue
        if  len(rp_data) == 0:
            W(f"No matches for {i.horse_name}, race_time: {i.race_time}")
            unmatched.append(i.horse_name)
            continue
        matches.append(
            {
                "id": rp_data.id.iloc[0],
                "name": rp_data.horse_name.iloc[0],
                "bf_id": i.horse_id,
            }
        )

    I(f"Matched {len(matches)} of {len(bf)} horses")

    return pd.DataFrame(matches)


if __name__ == "__main__":
    I("Starting Betfair matching")
    (
        rp_data,
        bf_data,
    ) = ptr(
        lambda: fetch_data(
            """
            SELECT race_timestamp, h.id, horse_name 
            FROM rp_raw.todays_performance_data tpd
            LEFT JOIN horse h
            ON tpd.horse_id = h.rp_id
            """
        ),
        lambda: fetch_data(
            """
            SELECT race_time, horse_id, horse_name 
            FROM bf_raw.todays_price_data
            WHERE status = 'ACTIVE'
            """
        ),
    )

    matched = entity_match_betfair(rp_data, bf_data)
    store_data(matched, "bf_horse", "public", truncate=True)



