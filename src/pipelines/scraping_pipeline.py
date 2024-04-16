from datetime import datetime, timedelta

from src.raw.racing_post.scrape_data import process_rp_scrape_data
from src.raw.racing_post.scrape_links import process_rp_scrape_links
from src.raw.racing_post.scrape_todays_data import process_rp_scrape_days_data
from src.raw.timeform.scrape_data import process_tf_scrape_data
from src.raw.timeform.scrape_links import process_tf_scrape_links
from src.raw.timeform.scrape_todays_data import process_tf_scrape_days_data
from src.storage.sql_db import fetch_data
from src.utils.logging_config import I, W
from src.utils.processing_utils import pp, ptr

TODAY = datetime.now().strftime("%Y-%m-%d")
TOMORROW = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def post_results_scraping_checks():

    I("Checking for missing data...")
    rp_links, tf_links = ptr(
        lambda: fetch_data("SELECT * FROM rp_raw.missing_links;"),
        lambda: fetch_data("SELECT * FROM tf_raw.missing_links;"),
    )
    if not rp_links.empty:
        W("Not all RP data has been scraped.")
    I("All RP data scraped successfully")
    if not tf_links.empty:
        W("Not all TF data has been scraped.")
    I("All TF data scraped successfully")


def post_racecards_scraping_checks():
    I("Checking for missing data...")

    rp_racecards, tf_racecards = ptr(
        lambda: fetch_data(
            "SELECT race_time, race_date FROM rp_raw.todays_performance_data;"
        ),
        lambda: fetch_data(
            "SELECT race_time, race_date FROM tf_raw.todays_performance_data;"
        ),
    )
    rp_race_dates = rp_racecards["race_date"].unique()
    rp_race_times = rp_racecards["race_time"].unique()
    if TODAY not in rp_race_dates:
        W(f"RP racecards for {TODAY} not found in database.")
    if TOMORROW not in rp_race_dates:
        W(f"RP racecards for {TOMORROW} not found in database.")
    I("All RP racecards found in database")

    tf_race_dates = tf_racecards["race_date"].unique()
    tf_race_times = tf_racecards["race_time"].unique()
    if TODAY not in tf_race_dates:
        W(f"TF racecards for {TODAY} not found in database.")
    if TOMORROW not in tf_race_dates:
        W(f"TF racecards for {TOMORROW} not found in database.")
    I("All TF racecards found in database")

    missing_tf_racetimes = set(rp_race_times) - set(tf_race_times)
    missing_rp_racetimes = set(tf_race_times) - set(rp_race_times)
    if missing_tf_racetimes:
        W(f"TF racecards missing the following RP race times: {missing_tf_racetimes}")
    if missing_rp_racetimes:
        W(f"RP racecards missing the following TF race times: {missing_rp_racetimes}")

    todays_rp_counts = rp_racecards[rp_racecards["race_date"] == TODAY].shape[0]
    tomorrows_rp_counts = rp_racecards[rp_racecards["race_date"] == TOMORROW].shape[0]
    todays_tf_counts = tf_racecards[tf_racecards["race_date"] == TODAY].shape[0]
    tomorrows_tf_counts = tf_racecards[tf_racecards["race_date"] == TOMORROW].shape[0]

    if todays_rp_counts != todays_tf_counts:
        W(
            f"Racecard counts for {TODAY} do not match between RP: {len(rp_racecards)} and TF: {len(tf_racecards)}"
        )
    if tomorrows_rp_counts != tomorrows_tf_counts:
        W(
            f"Racecard counts for {TOMORROW} do not match between RP: {len(rp_racecards)} and TF: {len(tf_racecards)}"
        )


def historical_pipeline():
    I("Running historical pipeline")
    pp(
        (process_rp_scrape_links, None),
        (process_tf_scrape_links, None),
    )

    pp(
        (process_rp_scrape_data, None),
        (process_tf_scrape_data, None),
    )
    post_results_scraping_checks()


def todays_pipeline():
    I("Running todays pipeline")
    pp(
        (
            process_rp_scrape_days_data,
            (
                TODAY,
                TOMORROW,
            ),
        ),
        (
            process_tf_scrape_days_data,
            (
                TODAY,
                TOMORROW,
            ),
        ),
    )

    post_racecards_scraping_checks()


def run_scraping_pipeline(historical=True, todays=True):
    if historical:
        historical_pipeline()
    if todays:
        todays_pipeline()


if __name__ == "__main__":
    run_scraping_pipeline(historical=False)
