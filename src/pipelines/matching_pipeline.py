from src.entity_matching.betfair_historical_matcher import (
    process_historical_betfair_entity_matching,
)
from src.entity_matching.betfair_todays_matcher import (
    process_todays_betfair_entity_matching,
)
from src.entity_matching.post_matching_checks import post_matching_data_checks
from src.entity_matching.racing_post_matcher import process_racing_post_entity_matching
from src.entity_matching.timeform_matcher import process_timeform_entity_matching
from src.storage.psql_db import get_db
from src.utils.logging_config import W
from src.utils.processing_utils import ptr

db = get_db()


def run_matching_pipeline():
    rp_matching_data, missing_dates = process_racing_post_entity_matching()
    if missing_dates:
        process_timeform_entity_matching(rp_matching_data, missing_dates)
    else:
        W("No missing data to match")
    process_todays_betfair_entity_matching()
    process_historical_betfair_entity_matching()
    ptr(
        lambda: db.call_procedure("insert_into_joined_performance_data", "staging"),
        lambda: db.call_procedure(
            "insert_into_todays_joined_performance_data", "staging"
        ),
    )
    post_matching_data_checks()


if __name__ == "__main__":
    run_matching_pipeline()
