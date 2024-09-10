from datetime import datetime, timedelta

from src.raw.betfair.fetch_historical_market_data import fetch_historical_market_data
from src.raw.betfair.fetch_todays_market_data import fetch_todays_market_data
from src.raw.racing_post.scrape_data import process_rp_scrape_data
from src.raw.racing_post.scrape_links import process_rp_scrape_links
from src.raw.racing_post.scrape_todays_data import process_rp_scrape_days_data
from src.raw.timeform.scrape_data import process_tf_scrape_data
from src.raw.timeform.scrape_links import process_tf_scrape_links
from src.raw.timeform.scrape_todays_data import process_tf_scrape_days_data
from src.storage.psql_db import get_db
from src.utils.logging_config import I, W
from src.utils.processing_utils import pp, ptr

db = get_db()

TODAY = datetime.now().strftime("%Y-%m-%d")
TOMORROW = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def historical_pipeline():
    I("Running historical pipeline")
    pp(
        (process_rp_scrape_links, None),
        (process_tf_scrape_links, None),
    )

    pp(
        (process_rp_scrape_data, None),
        (process_tf_scrape_data, None),
        (fetch_historical_market_data, None),
    )


def todays_pipeline():
    I("Running todays pipeline")
    pp(
        (
            process_rp_scrape_days_data,
            ([TODAY],),
        ),
        (
            process_tf_scrape_days_data,
            ([TODAY],),
        ),
        (fetch_todays_market_data, None),
    )


def post_results_scraping_checks():
    I("Checking for missing data...")
    rp_links, tf_links = ptr(
        lambda: db.fetch_data("SELECT * FROM rp_raw.missing_links;"),
        lambda: db.fetch_data("SELECT * FROM tf_raw.missing_links;"),
    )
    if not rp_links.empty:
        W("Not all RP data has been scraped.")
    I("All RP data scraped successfully")
    if not tf_links.empty:
        W("Not all TF data has been scraped.")


def post_racecards_scraping_checks():
    I("Checking for missing data...")
    missing_racecards = db.fetch_data(
        """
        SELECT * 
        FROM errors.missing_todays_races 
        WHERE both_sets = false;
        """
    )
    if not missing_racecards.empty:
        W(f"Missing racecards found: {missing_racecards['race_timestamp'].tolist()}")


def post_scraping_checks():
    I("Running post-scraping checks...")
    pp(
        (post_racecards_scraping_checks, None),
        (post_results_scraping_checks, None),
    )


def run_ingestion_pipeline():
    historical_pipeline()
    todays_pipeline()
    post_scraping_checks()


if __name__ == "__main__":
    run_ingestion_pipeline()
