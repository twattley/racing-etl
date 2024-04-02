from src.storage.sql_db import fetch_data, store_data
from src.transform.transform_data import (
    RaceDataModel,
    TransformedDataModel,
    load_transformed_performance_data,
    load_transformed_race_data,
    transform_data,
)
from src.utils.logging_config import I
from src.utils.processing_utils import execute_stored_procedures


def run_transform_pipeline():
    data = fetch_data("SELECT * FROM public.missing_performance_data_vw;")
    if data.empty:
        I("No missing data to transform.")
        return
    accepted_data, rejected_data, race_data = transform_data(
        data=data,
        transform_data_model=TransformedDataModel,
        race_data_model=RaceDataModel,
    )
    store_data(accepted_data, "transformed_performance_data", "staging", truncate=True)
    store_data(
        rejected_data, "transformed_performance_data_rejected", "staging", truncate=True
    )
    store_data(race_data, "transformed_race_data", "staging", truncate=True)

    execute_stored_procedures(
        load_transformed_performance_data,
        load_transformed_race_data,
    )


if __name__ == "__main__":
    run_transform_pipeline()
