import traceback
import pandas as pd
from src.data_models.base.base_model import convert_and_validate_data
from src.raw.data_types import DataScrapingTask
from src.raw.webdriver_base import select_source_driver
from src.storage.psql_db import get_db
from src.utils.logging_config import E, I

db = get_db()


def scrape_historical_data(task: DataScrapingTask) -> None:
    dataframes_list = []
    filtered_links_df = db.fetch_data(f"SELECT * FROM {task.schema}.{task.source_view}")
    if filtered_links_df.empty:
        I("No missing links found. Ending the script.")
        return
    driver = select_source_driver(task)
    I(f"Number of missing links {task.source_name}: {len(filtered_links_df)}")

    for index, link in enumerate(filtered_links_df.link_url.unique(), 1):
        try:
            I(f"Scraping link: {link}")
            driver.get(link)
            dataframes_list.append(task.scraper_func(driver, link))
            if index % 20 == 0 or index == len(filtered_links_df):
                I(f"Storing data batch to DB {index // 20}")
                data = pd.concat(dataframes_list)
                data = data.pipe(
                    convert_and_validate_data,
                    task.data_model,
                    task.string_fields,
                    task.unique_id,
                )
                db.store_data(data, task.dest_table, task.schema)
                dataframes_list = []

        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            continue

    if not dataframes_list:
        I("No data scraped. Ending the script.")

    driver.quit()
