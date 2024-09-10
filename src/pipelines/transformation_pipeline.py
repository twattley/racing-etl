import json

from src.data_models.transform.race_model import RaceDataModel
from src.data_models.transform.transformed_model import TransformedDataModel
from src.storage.psql_db import get_db
from src.transform.transform_data import transform_data
from src.utils.logging_config import I, W
from src.utils.processing_utils import pt, ptr

db = get_db()


def write_json(data: dict | list, file_path: str, indent: int = 4) -> None:
    try:
        with open(file_path, "w") as file:
            json.dump(data, file, indent=indent)
    except Exception as e:
        print(f"Error writing to JSON file: {e}")


def post_transform_today_checks():
    I("Running post-transform checks")
    todays_staging, todays_data = ptr(
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM staging.todays_joined_performance_data;"
        ),
        lambda: db.fetch_data(
            "SELECT DISTINCT unique_id FROM public.todays_performance_data;"
        ),
    )

    number_of_staging_records = len(todays_staging)
    number_of_data_records = len(todays_data)

    if number_of_staging_records != number_of_data_records:
        W(
            f"MISSING DATA STAGING: {number_of_staging_records} DATA: {number_of_data_records}"
        )
    else:
        I(
            f"SUCCESS: STAGING: {number_of_staging_records} DATA: {number_of_data_records}"
        )


def run_transformation_pipeline():
    def process_results_data():
        results_data = db.fetch_data(
            "SELECT * FROM public.missing_performance_data_vw;"
        )
        if results_data.empty:
            I("No missing historical data to transform.")
            return

        accepted_data, rejected_data, race_data = transform_data(
            data=results_data,
            transform_data_model=TransformedDataModel,
            race_data_model=RaceDataModel,
            data_type="results",
        )
        pt(
            lambda: db.store_data(
                accepted_data, "transformed_performance_data", "staging", truncate=True
            ),
            lambda: db.store_data(
                rejected_data,
                "staging_transformed_performance_data_rejected",
                "errors",
                truncate=True,
            ),
            lambda: db.store_data(
                race_data, "transformed_race_data", "staging", truncate=True
            ),
        )

    def process_todays_data():
        todays_data = db.fetch_data(
            "SELECT * FROM public.missing_todays_performance_data_vw;"
        )
        if todays_data.empty:
            I("No missing today's data to transform.")
            return

        todays_accepted_data, todays_rejected_data, todays_race_data = transform_data(
            data=todays_data,
            transform_data_model=TransformedDataModel,
            race_data_model=RaceDataModel,
            data_type="todays",
        )
        pt(
            lambda: db.store_data(
                todays_accepted_data,
                "todays_transformed_performance_data",
                "staging",
                truncate=True,
            ),
            lambda: db.store_data(
                todays_rejected_data,
                "staging_todays_transformed_performance_data_rejected",
                "errors",
                # truncate=True,
            ),
            lambda: db.store_data(
                todays_race_data,
                "todays_transformed_race_data",
                "staging",
                truncate=True,
            ),
        )

    pt(process_results_data, process_todays_data)

    pt(
        lambda: db.call_procedure("insert_transformed_performance_data", "public"),
        lambda: db.call_procedure("insert_transformed_race_data", "public"),
        lambda: db.call_procedure(
            "insert_todays_transformed_performance_data", "public"
        ),
        lambda: db.call_procedure("insert_todays_transformed_race_data", "public"),
    )

    post_transform_today_checks()


if __name__ == "__main__":
    run_transformation_pipeline()
