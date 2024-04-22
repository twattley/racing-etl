from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.scraping_pipeline import run_scraping_pipeline
from src.pipelines.transform_pipeline import run_transform_pipeline


def run_daily_pipeline():
    run_scraping_pipeline()
    run_matching_pipeline()
    # run_transform_pipeline()


if __name__ == "__main__":
    run_daily_pipeline()
