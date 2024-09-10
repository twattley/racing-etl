from src.storage.psql_db import get_db
from src.utils.logging_config import I, W
from src.utils.processing_utils import ptr

db = get_db()


def post_matching_data_checks():
    todays_rp_raw, todays_staging = ptr(
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM rp_raw.todays_performance_data;"
        ),
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM staging.todays_joined_performance_data;"
        ),
    )

    number_of_raw_records = len(todays_rp_raw)
    number_of_staging_records = len(todays_staging)

    if number_of_raw_records != number_of_staging_records:
        W(
            f"MISSING DATA RAW: {number_of_raw_records} STAGING: {number_of_staging_records}"
        )
    else:
        I(f"SUCCESS: RAW: {number_of_raw_records} STAGING: {number_of_staging_records}")
