from src.storage.psql_db import get_db
from src.utils.processing_utils import pt

db = get_db()


def run_insert_pipeline():
    db.call_procedure("insert_historical_unioned_performance_data", "public")
    pt(
        lambda: db.call_procedure(
            "amend_winning_position_of_first_place_horse", "public"
        ),
        lambda: db.call_procedure("delete_recent_records", "public"),
    )
    db.call_procedure("insert_unioned_performance_data_today", "public")
    db.call_procedure("insert_unioned_performance_data_historical", "public")
