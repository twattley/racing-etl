from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.scraping_pipeline import run_scraping_pipeline
from src.storage.sql_db import call_procedure, fetch_data, store_data
from src.transform.transform_data import transform_data


def run_daily_pipeline():
    run_scraping_pipeline()
    run_matching_pipeline()
    call_procedure("insert_into_joined_performance_data", "staging")
    transormed_data = transform_data(
        fetch_data("select * from staging.joined_performance_data")
    )
    store_data(transormed_data, "transformed_data", "staging", truncate=True)
    call_procedure("load_transformed_data", "public")


if __name__ == "__main__":
    run_daily_pipeline()
