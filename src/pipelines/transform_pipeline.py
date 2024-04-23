from src.data_models.transform.race_model import RaceDataModel
from src.data_models.transform.transformed_model import TransformedDataModel
from src.storage.sql_db import call_procedure, fetch_data, store_data
from src.transform.transform_data import transform_data
from src.utils.logging_config import I
from src.utils.processing_utils import pt


def run_transform_pipeline():
    results_data = fetch_data("SELECT * FROM public.missing_performance_data_vw;")
    # todays_data = fetch_data("SELECT * FROM public.todays_missing_performance_data_vw;")

    if results_data.empty:
        I("No missing historical data to transform.")

    accepted_data, rejected_data, race_data = transform_data(
        data=results_data,
        transform_data_model=TransformedDataModel,
        race_data_model=RaceDataModel,
    )

    pt(
        lambda: store_data(
            accepted_data, "transformed_performance_data", "staging", truncate=True
        ),
        lambda: store_data(
            rejected_data,
            "staging_transformed_performance_data_rejected",
            "errors",
            truncate=True,
        ),
        lambda: store_data(
            race_data, "transformed_race_data", "staging", truncate=True
        ),
    )

    pt(
        lambda: call_procedure("insert_transformed_performance_data", "public"),
        lambda: call_procedure("insert_transformed_race_data", "public"),
    )


if __name__ == "__main__":
    run_transform_pipeline()
