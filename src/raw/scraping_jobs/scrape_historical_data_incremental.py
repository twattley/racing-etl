import traceback
import pandas as pd
from src.data_models.base.base_model import convert_and_validate_data
from src.raw.data_types import DataScrapingTask
from src.raw.webdriver_base import select_source_driver
from src.storage.psql_db import get_db
from src.utils.logging_config import E, I

db = get_db()


def scrape_historical_data_incremental(task: DataScrapingTask) -> None:
    driver = None
    try:
        I(f"Starting to process {task.source_name} data")
        while True:
            filtered_links_df = db.fetch_data(
                f"""
                SELECT link_url 
                FROM {task.schema}.{task.source_view}
                """
            )
            I(f"Number of missing links {task.source_name}: {len(filtered_links_df)}")
            if filtered_links_df.empty:
                I("No more links to process. Ending the script.")
                break

            # Initialize the driver only if there are links to process
            if driver is None:
                driver = select_source_driver(task)

            link = filtered_links_df.sample(1).link_url.iloc[0]
            try:
                I(f"Scraping link: {link}")
                driver.get(link)
                data = task.scraper_func(driver, link)
                data = data.pipe(
                    convert_and_validate_data,
                    task.data_model,
                    task.string_fields,
                    task.unique_id,
                )
                if data.empty:
                    I(f"No data found for link: {link}. Moving to the next link.")
                    continue
                db.store_data(data, task.dest_table, task.schema)

                I(f"Successfully processed and stored data for link: {link}")
            except Exception as e:
                E(
                    f"Encountered an error processing link {link}: {e}. Moving to the next link."
                )
                traceback.print_exc()
                db.store_data(
                    pd.DataFrame({"link_url": [link]}),
                    "days_results_links_errors",
                    task.schema,
                )
                continue

    except Exception as e:
        E(f"Unexpected error in process_scraping_data_incremental: {e}")
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
