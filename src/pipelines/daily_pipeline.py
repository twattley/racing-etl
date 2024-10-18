from api_helpers.helpers.logging_config import I

from src.pipelines.data_checks_pipeline import run_data_checks_pipeline
from src.pipelines.insert_pipeline import run_insert_pipeline
from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.transformation_pipeline import run_transformation_pipeline


def run_daily_pipeline():
    # I("***************** RUNNING INGESTION PIPELINE *****************")
    # run_ingestion_pipeline()
    I("***************** RUNNING MATCHING PIPELINE *****************")
    run_matching_pipeline()
    I("***************** RUNNING TRANSFORMATION PIPELINE *****************")
    run_transformation_pipeline()
    I("***************** RUNNING INSERT PIPELINE *****************")
    run_insert_pipeline()
    I("***************** RUNNING DATA CHECKS PIPELINE *****************")
    run_data_checks_pipeline()


if __name__ == "__main__":
    run_daily_pipeline()
