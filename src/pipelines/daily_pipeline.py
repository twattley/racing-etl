from src.pipelines.scraping_pipeline import run_scraping_pipeline
from src.pipelines.matching_pipeline import run_entity_matching_pipeline

def run_daily_pipeline():
    run_scraping_pipeline()
    run_entity_matching_pipeline()


if __name__ == "__main__":
    run_daily_pipeline()