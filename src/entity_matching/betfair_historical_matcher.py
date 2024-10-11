from datetime import timedelta

import pandas as pd
import pytz
from api_helpers.helpers.logging_config import I
from api_helpers.helpers.processing_utils import ptr
from fuzzywuzzy import process

from src.storage.storage_client import get_storage_client

db = get_storage_client("postgres")


def fetch_unmatched_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    (
        rp_data,
        bf_data,
    ) = ptr(
        lambda: db.fetch_data(
            """
        SELECT * 
        FROM bf_raw.missing_price_performance_data
        """
        ),
        lambda: db.fetch_data(
            """
        SELECT * 
        FROM bf_raw.missing_price_data
        """
        ),
    )

    return rp_data, bf_data


def fix_dates(
    rp_data: pd.DataFrame, bf_data: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    bf_data["race_date"] = pd.to_datetime(bf_data["race_date"]).dt.strftime("%Y-%m-%d")
    rp_data["race_timestamp"] = (
        rp_data["race_timestamp"].dt.tz_localize("UTC").dt.tz_convert("Europe/London")
    )

    return rp_data, bf_data


def utc_to_uk_time(utc_dt):
    if utc_dt.tzinfo is None or utc_dt.tzinfo.utcoffset(utc_dt) is None:
        utc_dt = pytz.utc.localize(utc_dt)
    uk_timezone = pytz.timezone("Europe/London")
    return utc_dt.astimezone(uk_timezone)


def find_best_match(name, choices, threshold=80):
    best_match = process.extractOne(name, choices)
    if best_match and best_match[1] >= threshold:
        return best_match[0]
    return None


def time_within_tolerance(time1, time2, tolerance_minutes=61):
    if time1.tzinfo is None:
        time1 = pytz.UTC.localize(time1)
    if time2.tzinfo is None:
        time2 = pytz.UTC.localize(time2)
    return abs(time1 - time2) <= timedelta(minutes=tolerance_minutes)


def combine_records(rp_row, bf_row):
    combined = {
        "race_time": rp_row["race_timestamp"],
        "race_date": rp_row["race_date"],
        "horse_name": rp_row["horse_name"],
        "horse_id": rp_row["horse_id"],
        "course_id": rp_row["course_id"],
        "meeting_id": rp_row["meeting_id"],
        "unique_id": rp_row["unique_id"],
        "race_id": rp_row["race_id"],
        "runner_id": bf_row["runner_id"],
        "race_key": bf_row["race_key"],
        "bf_unique_id": bf_row["bf_unique_id"],
        "price_change": bf_row["price_change"],
    }
    return combined


def process_historical_betfair_entity_matching():
    rf, bf = fix_dates(*fetch_unmatched_data())
    combined_records = []

    for i, bf_row in bf.iterrows():
        I(f"Processing record {i} of {len(bf)}")
        bf_uk_time = utc_to_uk_time(bf_row["race_time"])
        I(f"Processing: {bf_uk_time} - {bf_row['course']} - {bf_row['horse']}")

        rp_data = rf[rf["race_timestamp"].dt.date == bf_uk_time.date()]

        if len(rp_data) == 0:
            I("No RP data for this date")
            continue

        rp_courses = rp_data["course_name"].unique()

        best_course_match = find_best_match(bf_row["course"], rp_courses)

        if best_course_match:
            I(f"Best course match found: {best_course_match}")

            course_matched_data = rp_data[rp_data["course_name"] == best_course_match]

            time_matched_data = course_matched_data[
                course_matched_data["race_timestamp"].apply(
                    lambda x: time_within_tolerance(x, bf_uk_time)
                )
            ]

            if len(time_matched_data) == 0:
                I(f"No races within time tolerance for course: {best_course_match}")
                continue

            rp_horses = time_matched_data["horse_name"].unique()

            best_horse_match = find_best_match(bf_row["horse"], rp_horses)

            if best_horse_match:
                I(f"Best horse match found: {best_horse_match}")

                final_matched_data = time_matched_data[
                    time_matched_data["horse_name"] == best_horse_match
                ]

                if len(final_matched_data) == 1:
                    combined_record = combine_records(
                        final_matched_data.iloc[0], bf_row
                    )
                    combined_records.append(combined_record)
                    I("Record combined successfully")
                else:
                    I(f"Multiple or no matches found: {len(final_matched_data)}")
            else:
                I(f"No match found for horse: {bf_row['horse']}")
        else:
            I(f"No match found for course: {bf_row['course']}")

    final_df = pd.DataFrame(combined_records)
    final_df = final_df.drop_duplicates()
    db.store_data(final_df, "historical_price_data", "public")
    db.call_procedure("update_matched_historical_price_data", "bf_raw")


if __name__ == "__main__":
    process_historical_betfair_entity_matching()
