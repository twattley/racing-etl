from src.pipelines.data_checks_pipeline import run_data_checks_pipeline
from src.pipelines.ingestion_pipeline import run_ingestion_pipeline
from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.transformation_pipeline import run_transformation_pipeline


def run_daily_pipeline():
    run_ingestion_pipeline()
    run_matching_pipeline()
    run_transformation_pipeline()
    run_data_checks_pipeline()


if __name__ == "__main__":
    run_daily_pipeline()
