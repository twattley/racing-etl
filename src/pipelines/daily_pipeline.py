from src.storage.storage_client import get_storage_client
from src.pipelines.ingestion_pipeline import run_ingestion_pipeline
from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.transformation_pipeline import run_transformation_pipeline
from src.pipelines.load_pipeline import run_load_pipeline


def run_daily_pipeline():
    storage_client = get_storage_client("postgres")
    # run_ingestion_pipeline(storage_client)
    run_matching_pipeline(storage_client)
    # run_transformation_pipeline(storage_client)
    # run_load_pipeline(storage_client)


if __name__ == "__main__":
    run_daily_pipeline()
