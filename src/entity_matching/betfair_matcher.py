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
        sub_rp = rp[(rp['race_timestamp'] == i.race_time)]
        best_match = process.extractOne(i.filtered_horse_name, sub_rp['filtered_horse_name'])
        if not best_match:
            W(f"No match for {i.horse_name}")
            unmatched.append(i.horse_name)
            continue
        if best_match[1] < 90:
            W(f"Match score below 90 for {i.horse_name}")
            unmatched.append(i.horse_name)
            continue
        rp_data = sub_rp[sub_rp['filtered_horse_name'] == best_match[0]]
        if len(rp_data) > 1 or len(rp_data) == 0:
            W(f"Multiple matches or no matches for {i.horse_name}")
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

    if unmatched:
        W(f"Unmatched horses: {unmatched}")

    return pd.DataFrame(matches)





