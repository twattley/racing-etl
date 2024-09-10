from src.storage.psql_db import get_db

db = get_db()


def run_insert_pipeline():
    db.call_procedure("insert_unioned_performance_data_historical", "public")
    db.call_procedure("create_todays_performance_data_vw", "public")


if __name__ == "__main__":
    run_insert_pipeline()
