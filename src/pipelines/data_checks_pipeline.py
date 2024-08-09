from src.storage.psql_db import get_db
from src.utils.logging_config import E, I
from src.utils.processing_utils import ptr

import json


def write_json(data: dict | list, file_path: str, indent: int = 4) -> None:
    try:
        with open(file_path, "w") as file:
            json.dump(data, file, indent=indent)
    except Exception as e:
        print(f"Error writing to JSON file: {e}")


db = get_db()


def construct_cache_data():
    market_ids = db.fetch_data(
        """
        SELECT race_time, race_id, market_id_win, market_id_place 
        FROM public.bf_market
        where race_time::date = current_date
        """
    )

    market_ids["race_time"] = market_ids["race_time"].apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S")
    )
    if market_ids.empty:
        raise ValueError("No market ids found for today")

    write_json(
        market_ids.to_dict("records"),
        "/Users/tomwattley/Code/python/racing-api-project/racing-api/src/cache/market_ids.json",
    )


def run_data_checks_pipeline():
    missing_records_between_sets, missing_rp_data_in_final = ptr(
        lambda: db.fetch_data("SELECT * FROM metrics.record_count_differences_vw"),
        lambda: db.fetch_data("SELECT * FROM metrics.missing_raw_data"),
    )

    if missing_records_between_sets.empty:
        I("No missing data between RP and TF")
    else:
        for record in missing_records_between_sets.itertuples():
            E(
                f"MISSING DATA:\n {record.course} {record.race_date} {record.rp_num_records} {record.tf_num_records}"
            )

    if missing_rp_data_in_final.empty:
        I("No missing data in final performance data table")
    else:
        for record in missing_rp_data_in_final.itertuples():
            E(f"MISSING RP DATA IN FINAL: {record.race_timestamp} {record.horse_name}")

    construct_cache_data()


if __name__ == "__main__":
    run_data_checks_pipeline()
