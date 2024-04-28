import pandas as pd
from fuzzywuzzy import process

from src.utils.logging_config import W


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


def entity_match_betfair(rp_data: pd.DataFrame, bf_data: pd.DataFrame) -> pd.DataFrame:
    rp_data = rp_data.pipe(filter_horse_name)
    bf_data = bf_data.pipe(filter_horse_name)
    matched = []
    unmatched = []
    for i in bf_data.itertuples():
        sub_rp = rp_data[(rp_data["race_timestamp"] == i.race_time)]
        best_match = process.extractOne(
            i.filtered_horse_name, sub_rp["filtered_horse_name"]
        )
        rp_data = sub_rp[sub_rp["filtered_horse_name"] == best_match[0]]
        if best_match[1] < 90:
            unmatched.append(i.horse_name)
            continue
        matched.append(
            {
                "id": rp_data.id.iloc[0],
                "name": rp_data.horse_name.iloc[0],
                "bf_id": i.horse_id,
            }
        )
    if unmatched:
        W(f"Unmatched horses: {unmatched}")

    return pd.DataFrame(matched)
