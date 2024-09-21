from datetime import datetime

from src.raw.betfair.fetch_historical_market_data import fetch_historical_market_data
from src.raw.betfair.fetch_todays_market_data import fetch_todays_market_data
from src.raw.betfair.process_historical_market_data import (
    process_historical_market_data,
)
from src.raw.racing_post.scrape_links import process_rp_scrape_links
from src.raw.racing_post.scrape_todays_data import process_rp_scrape_days_data
from src.raw.racing_post.scrape_uk_ire_data import process_rp_scrape_uk_ire_data
from src.raw.timeform.scrape_links import process_tf_scrape_links
from src.raw.timeform.scrape_todays_data import process_tf_scrape_days_data
from src.raw.timeform.scrape_uk_ire_data import process_tf_scrape_uk_ire_data
from src.storage.psql_db import get_db
from src.utils.logging_config import I, W
from src.utils.processing_utils import pp, ptr
from src.raw.timeform.scrape_non_uk_ire_data import process_tf_scrape_non_uk_ire_data
from src.raw.racing_post.scrape_non_uk_ire_data import process_rp_scrape_non_uk_ire_data


db = get_db()

TODAY = datetime.now().strftime("%Y-%m-%d")


def historical_pipeline():
    I("Running historical pipeline")
    pp(
        (process_rp_scrape_links, None),
        (process_tf_scrape_links, None),
        (fetch_historical_market_data, None),
    )

    pp(
        (process_rp_scrape_uk_ire_data, None),
        (process_tf_scrape_uk_ire_data, None),
        (process_historical_market_data, None),
    )
    process_rp_scrape_non_uk_ire_data()
    process_tf_scrape_non_uk_ire_data()


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
    uk_ire_vw = "missing_uk_ire_links"
    non_uk_ire_vw = "missing_non_uk_ire_links"
    uk_ire_rp_links, uk_ire_tf_links, non_uk_ire_rp_links, non_uk_ire_tf_links = ptr(
        lambda: db.fetch_data(f"SELECT * FROM rp_raw.{uk_ire_vw};"),
        lambda: db.fetch_data(f"SELECT * FROM tf_raw.{uk_ire_vw};"),
        lambda: db.fetch_data(f"SELECT * FROM rp_raw.{non_uk_ire_vw};"),
        lambda: db.fetch_data(f"SELECT * FROM tf_raw.{non_uk_ire_vw};"),
    )
    if not uk_ire_rp_links.empty:
        W("Not all UK/IRE RP data has been scraped.")
    if not uk_ire_tf_links.empty:
        W("Not all UK/IRE TF data has been scraped.")
    if not non_uk_ire_rp_links.empty:
        W("Not all non-UK/IRE RP data has been scraped.")
    if not non_uk_ire_tf_links.empty:
        W("Not all non-UK/IRE TF data has been scraped.")


def post_racecards_scraping_checks():
    I("Checking for missing data...")
    missing_racecards = db.fetch_data("SELECT * FROM errors.missing_todays_races;")
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
