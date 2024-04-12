from datetime import datetime, timedelta

from src.raw.timeform.scrape_todays_data import process_tf_scrape_days_data
from src.storage.sql_db import fetch_data
from src.utils.logging_config import I


def post_scraping_checks():

    I("Checking for missing data...")
    rp_links = fetch_data("SELECT * FROM rp_raw.missing_links;")
    if not rp_links.empty:
        raise ValueError("Not all RP data has been scraped.")
    I("All RP data scraped successfully")

    tf_links = fetch_data("SELECT * FROM tf_raw.missing_links;")
    if not tf_links.empty:
        raise ValueError("Not all TF data has been scraped.")
    I("All TF data scraped successfully")


def run_scraping_pipeline():
    datetime.now().strftime("%Y-%m-%d")
    TOMORROW = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    # process_rp_scrape_links()
    # process_tf_scrape_links()
    # process_rp_scrape_data()
    # process_tf_scrape_data()
    # post_scraping_checks()
    # process_rp_scrape_days_data(TODAY)
    # process_tf_scrape_days_data(TODAY)
    # process_rp_scrape_days_data(TOMORROW)
    process_tf_scrape_days_data(TOMORROW)


if __name__ == "__main__":
    run_scraping_pipeline()
