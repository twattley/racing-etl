from src.storage.storage_client import get_storage_client

db = get_storage_client("postgres")


def run_insert_pipeline():
    db.call_procedure("insert_unioned_performance_data_historical", "public")
    db.call_procedure("insert_unioned_non_uk_ire_performance_data_historical", "public")
    db.call_procedure("create_todays_performance_data_vw", "public")


if __name__ == "__main__":
    run_insert_pipeline()
