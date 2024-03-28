from src.storage.sql_db import fetch_data, store_data
from src.transform.transform_data import (
    RaceDataModel,
    TransformedDataModel,
    refresh_missing_entity_counts,
    refresh_missing_record_counts,
    transform_data,
    load_transformed_performance_data,
    load_transformed_race_data,
)
from src.utils.processing_utils import execute_stored_procedures




def run_transform_pipeline():
    data = fetch_data("SELECT * FROM staging.missing_performance_data")
    transformed_data, race_data = transform_data(
        data=data,
        transform_data_model=TransformedDataModel,
        race_data_model=RaceDataModel,
    )
    store_data(
        transformed_data, "transformed_performance_data", "staging", truncate=True
    )
    store_data(race_data, "transformed_race_data", "staging", truncate=True)

    execute_stored_procedures(
        load_transformed_performance_data,
        load_transformed_race_data,
    )

    execute_stored_procedures(
        refresh_missing_record_counts,
        refresh_missing_entity_counts,
    )


