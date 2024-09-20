import time

import pandas as pd

from src.raw.data_types import LinkScrapingTask
from src.storage.psql_db import get_db
from src.utils.logging_config import I

db = get_db()


def process_scraping_result_links(task: LinkScrapingTask) -> None:
    I("Scrape_links.py execution started.")
    driver = task.driver
    dates = db.fetch_data(f"SELECT * FROM {task.schema}.{task.source_table}")
    if dates.empty:
        I("No missing dates found. Ending the script.")
        return
    dates = [i.strftime("%Y-%m-%d") for i in dates["date"]]
    for date in dates:
        try:
            url = f"{task.base_url}{str(date)}"
            I(f"Generated URL for scraping: {url}")
            driver.get(url)
            time.sleep(5)
            I("Page load complete. Proceeding to scrape links.")
            days_results_links = task.filter_func(driver)
            I(f"Found {len(days_results_links)} valid links for date {date}.")
            days_results_links_df = pd.DataFrame(
                {
                    "date": [date] * len(days_results_links),
                    "link_url": days_results_links,
                }
            )
            I(f"Inserting {len(days_results_links)} links into the database.")
            db.store_data(days_results_links_df, task.destination_table, task.schema)
        except KeyboardInterrupt:
            I("Keyboard interrupt detected. Exiting the script.")
            break
        except Exception as e:
            I(f"An error occurred: {e}. Continuing to next date.")
            continue

    driver.quit()
    I("Scrape_links.py execution completed.")
