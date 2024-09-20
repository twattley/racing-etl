import os
import time
from dataclasses import fields
from datetime import datetime

import pandas as pd

from src.data_models.base.base_model import convert_and_validate_data
from src.raw.data_types import DataScrapingTask
from src.raw.webdriver_base import select_source_driver
from src.utils.logging_config import E, I


def create_empty_dataframe(data_model):
    columns = []
    dtypes = {}

    for field in fields(data_model):
        columns.append(field.name)

        if field.type == datetime:
            dtypes[field.name] = "datetime64[ns]"
        elif field.type == int:
            dtypes[field.name] = "int64"
        elif field.type == float:
            dtypes[field.name] = "float64"
        else:
            dtypes[field.name] = "object"

    df = pd.DataFrame(columns=columns)
    df = df.astype(dtypes)

    return df


def scrape_historical_data_cloud(task: DataScrapingTask) -> None:
    input_file = (
        f"/root/racing-etl/data/{task.source_name}/missing_non_uk_ire_links.csv"
    )
    output_file = (
        f"/root/racing-etl/data/{task.source_name}/non_uk_ire_performance_data.parquet"
    )
    error_file = f"/root/racing-etl/data/{task.source_name}/error_links.csv"
    if os.path.exists(output_file):
        existing_data = pd.read_parquet(output_file)
        processed_links = set(existing_data["debug_link"])
    else:
        existing_data = create_empty_dataframe(task.data_model)
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

    driver = select_source_driver(task)

    while True:
        if os.path.exists(output_file):
            existing_data = pd.read_parquet(output_file)
            processed_links = set(existing_data["debug_link"])
        else:
            existing_data = create_empty_dataframe(task.data_model)
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
