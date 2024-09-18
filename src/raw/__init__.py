import os
import time
import traceback
from dataclasses import dataclass

import pandas as pd
from selenium import webdriver

from src.data_models.base.base_model import BaseDataModel, convert_and_validate_data
from src.raw.webdriver_base import select_source_driver
from src.storage.psql_db import get_db
from src.utils.logging_config import E, I
from src.utils.processing_utils import register_job_completion

db = get_db()

data_structure_dict = {
    "race_timestamp": pd.DatetimeTZDtype(tz="Europe/London"),
    "race_date": pd.StringDtype(),
    "course_name": pd.StringDtype(),
    "race_class": pd.StringDtype(),
    "horse_name": pd.StringDtype(),
    "horse_type": pd.StringDtype(),
    "horse_age": pd.StringDtype(),
    "headgear": pd.StringDtype(),
    "conditions": pd.StringDtype(),
    "horse_price": pd.StringDtype(),
    "race_title": pd.StringDtype(),
    "distance": pd.StringDtype(),
    "distance_full": pd.StringDtype(),
    "going": pd.StringDtype(),
    "number_of_runners": pd.StringDtype(),
    "total_prize_money": pd.Int64Dtype(),
    "first_place_prize_money": pd.Int64Dtype(),
    "winning_time": pd.StringDtype(),
    "official_rating": pd.StringDtype(),
    "horse_weight": pd.StringDtype(),
    "draw": pd.StringDtype(),
    "country": pd.StringDtype(),
    "surface": pd.StringDtype(),
    "finishing_position": pd.StringDtype(),
    "total_distance_beaten": pd.StringDtype(),
    "ts_value": pd.StringDtype(),
    "rpr_value": pd.StringDtype(),
    "extra_weight": pd.Float64Dtype(),
    "comment": pd.StringDtype(),
    "race_time": pd.StringDtype(),
    "currency": pd.StringDtype(),
    "course": pd.StringDtype(),
    "jockey_name": pd.StringDtype(),
    "jockey_claim": pd.StringDtype(),
    "trainer_name": pd.StringDtype(),
    "sire_name": pd.StringDtype(),
    "dam_name": pd.StringDtype(),
    "dams_sire": pd.StringDtype(),
    "owner_name": pd.StringDtype(),
    "horse_id": pd.StringDtype(),
    "trainer_id": pd.StringDtype(),
    "jockey_id": pd.StringDtype(),
    "sire_id": pd.StringDtype(),
    "dam_id": pd.StringDtype(),
    "dams_sire_id": pd.StringDtype(),
    "owner_id": pd.StringDtype(),
    "race_id": pd.StringDtype(),
    "course_id": pd.StringDtype(),
    "meeting_id": pd.StringDtype(),
    "unique_id": pd.StringDtype(),
    "debug_link": pd.StringDtype(),
    "created_at": pd.DatetimeTZDtype(tz="Europe/London"),
}

# Create the empty DataFrame
data_structure = pd.DataFrame(columns=data_structure_dict.keys()).astype(
    data_structure_dict
)


@dataclass
class DatabaseInfo:
    schema: str
    job_name: str
    source_view: str
    dest_table: str


@dataclass
class DataScrapingTask:
    driver: webdriver.Chrome
    source_name: str
    schema: str
    source_view: str
    dest_table: str
    job_name: str
    scraper_func: callable
    data_model: BaseDataModel
    string_fields: dict
    unique_id: str


@dataclass
class LinkScrapingTask:
    driver: webdriver.Chrome
    base_url: str
    schema: str
    source_table: str
    destination_table: str
    filter_func: callable


def process_scraping_data(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    dataframes_list = []
    filtered_links_df = db.fetch_data(f"SELECT * FROM {task.schema}.{task.source_view}")
    if filtered_links_df.empty:
        I("No missing links found. Ending the script.")
        return
    I(f"Number of missing links: {len(filtered_links_df)}")
    for link in filtered_links_df.link_url.unique():
        try:
            I(f"Scraping link: {link}")
            driver.get(link)
            dataframes_list.append(task.scraper_func(driver, link))
        except Exception as e:
            E(f"Encountered an error: {e}. Attempting to continue with the next link.")
            traceback.print_exc()
            continue

    if not dataframes_list:
        I("No data scraped. Ending the script.")
        return
    data = pd.concat(dataframes_list)
    data = data.pipe(
        convert_and_validate_data, task.data_model, task.string_fields, task.unique_id
    )
    db.store_data(data, task.dest_table, task.schema)
    register_job_completion(task.job_name)
    driver.quit()


def process_scraping_data_incremental(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    try:
        while True:
            filtered_links_df = db.fetch_data(
                f"""
                SELECT link_url 
                FROM {task.schema}.missing_non_uk_ire_links_tmp 
                LIMIT 1
                """
            )
            if filtered_links_df.empty:
                I("No more links to process. Ending the script.")
                break

            link = filtered_links_df.link_url.iloc[0]
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
                    db.store_data(
                        pd.DataFrame({"link_url": [link]}),
                        "days_results_links_errors",
                        task.schema,
                    )
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

    driver.quit()


def process_scraping_data_cloud(task: DataScrapingTask) -> None:
    driver = select_source_driver(task)
    input_file = (
        f"/root/racing-etl/data/{task.source_name}/missing_non_uk_ire_links.csv"
    )
    output_file = (
        f"/root/racing-etl/data/{task.source_name}/non_uk_ire_performance_data.parquet"
    )
    error_file = f"/root/racing-etl/data/{task.source_name}/error_links.csv"

    while True:
        if os.path.exists(output_file):
            existing_data = pd.read_parquet(output_file)
            processed_links = set(existing_data["debug_link"])
        else:
            existing_data = data_structure
            processed_links = set()

        if os.path.exists(error_file):
            error_links = set(pd.read_csv(error_file)["link_url"])
        else:
            error_links = set()

        filtered_links_df = pd.read_csv(input_file)
        links_to_process = filtered_links_df[
            ~filtered_links_df["link_url"].isin(processed_links)
            & ~filtered_links_df["link_url"].isin(error_links)
        ]

        I(
            f"Total links: {len(filtered_links_df)}, Links to process: {len(links_to_process)}"
        )

        if links_to_process.empty:
            I("No new links to process, exiting.")
            return

        for link in links_to_process["link_url"]:
            try:
                I(f"links left to process: {len(filtered_links_df)}")
                I(f"Scraping link: {link}")
                driver.get(link)
                scraped_data = task.scraper_func(driver, link)

                scraped_data = scraped_data.pipe(
                    convert_and_validate_data,
                    task.data_model,
                    task.string_fields,
                    task.unique_id,
                )
                existing_data = pd.concat(
                    [existing_data, scraped_data], ignore_index=True
                )
                existing_data.to_parquet(output_file, index=False)

                I(f"Successfully processed and stored data for link: {link}")

                filtered_links_df = filtered_links_df[
                    filtered_links_df["link_url"] != link
                ]
                filtered_links_df.to_csv(input_file, index=False)

            except Exception as e:
                E(f"Encountered an error processing link {link}: {e}")

                error_df = pd.DataFrame({"link_url": [link], "error_message": [str(e)]})
                if os.path.exists(error_file):
                    error_df.to_csv(error_file, mode="a", header=False, index=False)
                else:
                    error_df.to_csv(error_file, index=False)
                filtered_links_df = filtered_links_df[
                    filtered_links_df["link_url"] != link
                ]
                filtered_links_df.to_csv(input_file, index=False)
                continue

        I(f"Completed processing batch. Total records in output: {len(existing_data)}")

        time.sleep(1)


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


def run_scraping_task(task):
    if isinstance(task, DataScrapingTask):
        # process_scraping_data(task)
        process_scraping_data_incremental(task)
        # process_scraping_data_cloud(task)
    elif isinstance(task, LinkScrapingTask):
        process_scraping_result_links(task)


def check_already_processed(job_name: str) -> bool:
    return db.fetch_data(
        f"""
        SELECT * 
        FROM metrics.processing_times 
        WHERE job_name = '{job_name}' 
        AND processed_at:: date = CURRENT_DATE
        """
    ).empty
