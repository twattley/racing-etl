from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.scraping_pipeline import run_scraping_pipeline
from src.storage.sql_db import call_procedure, fetch_data, store_data
from src.transform.transform_data import transform_data
from src.transform.data_model import RaceDataModel, TransformedDataModel


def run_daily_pipeline():
    run_scraping_pipeline()
    run_matching_pipeline()
    call_procedure("insert_into_joined_performance_data", "staging")
    data = fetch_data("SELECT * FROM staging.joined_performance_data LIMIT 100")
    transformed_data, race_data = transform_data(
        data=data,
        transform_data_model=TransformedDataModel,
        race_data_model=RaceDataModel,
    )

    store_data(
        transformed_data, "transformed_performance_data", "staging", truncate=True
    )
    call_procedure("insert_transformed_race_data", "public")
    call_procedure("insert_transformed_performance_data", "public")


if __name__ == "__main__":
    run_daily_pipeline()
