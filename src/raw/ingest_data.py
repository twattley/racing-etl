from src.raw.data_types import DataScrapingTask, LinkScrapingTask
from src.raw.scraping_jobs.scrape_historical_data import scrape_historical_data
from src.raw.scraping_jobs.scrape_historical_data_cloud import (
    scrape_historical_data_cloud,
)
from src.raw.scraping_jobs.scrape_historical_data_incremental import (
    scrape_historical_data_incremental,
)
from src.raw.scraping_jobs.scraping_result_links import process_scraping_result_links
from src.storage.psql_db import get_db

db = get_db()


def process_scraping_data(task: DataScrapingTask, job_type: str) -> None:
    if job_type == "historical":
        scrape_historical_data(task)
    elif job_type == "incremental":
        scrape_historical_data_incremental(task)
    elif job_type == "cloud":
        scrape_historical_data_cloud(task)


def run_scraping_task(task):
    if isinstance(task, DataScrapingTask):
        process_scraping_data(task, "historical")
    elif isinstance(task, LinkScrapingTask):
        process_scraping_result_links(task)
