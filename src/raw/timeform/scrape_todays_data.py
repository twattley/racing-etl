import hashlib
import re
import time
from datetime import datetime, timedelta
from src.raw import check_already_processed

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.data_models.base.base_model import convert_and_validate_data
from src.data_models.raw.timeform_model import (
    TimeformDataModel,
    table_string_field_lengths,
)
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import E, I
from src.utils.processing_utils import register_job_completion

TODAYS_DATE_FILTER = datetime.now().strftime("%Y-%m-%d")
TOMORROWS_DATE_FILTER = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_course_data() -> pd.DataFrame:
    return fetch_data(
        """
        SELECT * FROM public.course cr
        left join public.country ct on 
        cr.country_id::text = ct.country_id::text
        """
    )


def click_for_racecards(driver, date):
    button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located(
            (
                By.CSS_SELECTOR,
                f"button.w-racecard-grid-nav-button[data-meeting-date='{date}']",
            )
        )
    )
    driver.execute_script("arguments[0].click();", button)
    I(f"Clicked on button for date: {date}")
    time.sleep(10)


def generate_unique_id(row):
    unique_string = f"timeform{row['horse_id']}{row['race_date']}"
    return hashlib.sha512(unique_string.encode()).hexdigest()


def format_entity(entity):
    return entity.replace("-", " ").title().strip()


def get_optional_element_text(row, css_selector):
    try:
        return row.find_element(By.CSS_SELECTOR, css_selector).text.strip()
    except Exception:
        return None


def get_data_from_url(url):
    if url.endswith("/"):
        url = url[:-1]
    *_, course, race_date, race_time, course_id, race, _ = url.split("/")
    course = course.replace("-", " ").title().strip()
    race_timestamp = datetime.strptime(f"{race_date} {race_time}", "%Y-%m-%d %H%M")
    return {
        "course_id": course_id,
        "course": course,
        "race_date": race_date,
        "race_timestamp": race_timestamp,
        "race": race,
    }


def get_horse_data(driver):
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tbody.rp-horse-row"))
    )

    horse_entries = driver.find_elements(By.CSS_SELECTOR, "tbody.rp-horse-row")
    horse_data = []
    trainer_pattern = (
        r"https://www.timeform.com/horse-racing/trainer/([a-zA-Z-]+)/form/([0-9]+)"
    )
    jockey_pattern = (
        r"https://www.timeform.com/horse-racing/jockey/([a-zA-Z-]+)/form/([0-9]+)"
    )
    horse_pattern = r"https://www.timeform.com/horse-racing/horse/form/([a-zA-Z-]+)/([0-9]+)/([a-zA-Z-]+)/([0-9]+)"

    for entry in horse_entries:
        links = entry.find_elements(By.CSS_SELECTOR, "a")
        for link in links:
            href = link.get_attribute("href")
            if href.endswith("/sire"):
                *_, sire_name, sire_id, _ = href.split("/")
            if href.endswith("/dam"):
                *_, dam_name, dam_id, _ = href.split("/")
            if re.search(trainer_pattern, href):
                *_, trainer_name, trainer_id = re.search(trainer_pattern, href).groups()
                continue
            if re.search(jockey_pattern, href):
                *_, jockey_name, jockey_id = re.search(jockey_pattern, href).groups()
                continue
            if re.search(horse_pattern, href):
                *_, horse_name, horse_id, horse_name_link, _ = re.search(
                    horse_pattern, href
                ).groups()
                continue
        horse_name = format_entity(horse_name)
        sire_name = format_entity(sire_name)
        dam_name = format_entity(dam_name)
        trainer_name = format_entity(trainer_name)
        jockey_name = format_entity(jockey_name)

        horse_data.append(
            {
                "horse_name": horse_name,
                "horse_id": horse_id,
                "sire_name": sire_name,
                "sire_id": sire_id,
                "dam_name": dam_name,
                "dam_id": dam_id,
                "trainer_name": trainer_name,
                "trainer_id": trainer_id,
                "jockey_name": jockey_name,
                "jockey_id": jockey_id,
            }
        )

    return pd.DataFrame(horse_data)


def get_links(
    driver: webdriver.Chrome, course_country: pd.DataFrame, date: str
) -> list[str]:
    hrefs = [
        element.get_attribute("href")
        for element in driver.find_elements(By.XPATH, "//a")
    ]
    trimmed_hrefs = []
    for href in hrefs:
        if href.endswith("/"):
            href = href[:-1]
        trimmed_hrefs.append(href)

    patterns = []
    for i in course_country.itertuples():
        pattern = rf"https://www.timeform.com/horse-racing/racecards/{i.tf_name}/{date}/([01]\d|2[0-3])[0-5]\d\/{i.tf_id}/(10|[1-9])/(.*)"
        patterns.append(pattern)

    if not patterns:
        raise ValueError(f"No patterns found on date: {date}")

    return sorted(
        {url for url in hrefs for pattern in patterns if re.search(pattern, url)}
    )


def process_tf_scrape_days_data(dates: list[str]):
    if check_already_processed('scrape_todays_tf_data'):
        I("Todays TF results data already processed")
        return
    I("Todays TF results data scraping started.")

    base_link = "https://www.timeform.com/horse-racing/racecards"
    errors = []
    data = []
    course_country = fetch_course_data()
    driver = get_headless_driver()
    driver.get(base_link)
    for date in dates:
        click_for_racecards(driver, date)
        urls = get_links(driver, course_country, date)
        for url in urls:
            try:
                I(f"Scraping data from: {url}")
                driver.get(url)
                race_data = get_data_from_url(url)
                age_range = get_optional_element_text(
                    driver, "span.rp-header-text[title='Horse age range']"
                )
                bha_rating_range = get_optional_element_text(
                    driver, "span.rp-header-text.pr3[title='BHA rating range']"
                )
                prize_money = get_optional_element_text(
                    driver, "span.rp-header-text.pr3[title='Prize money to winner']"
                )
                horse_data = get_horse_data(driver)
                data.append(
                    horse_data.assign(
                        **race_data,
                        age_range=age_range,
                        hcap_range=bha_rating_range,
                        prize=prize_money,
                        unique_id=lambda x: x.apply(
                            lambda y: hashlib.sha512(
                                f"timeform{y['horse_id']}{y['race_date']}".encode()
                            ).hexdigest(),
                            axis=1,
                        ),
                        finishing_position=None,
                        fractional_price=None,
                        main_race_comment=None,
                        draw=None,
                        tf_rating=None,
                        tf_speed_figure=None,
                        betfair_win_sp=None,
                        going=None,
                        in_play_prices=None,
                        debug_link=url,
                        equipment=None,
                        official_rating=None,
                        race_id=None,
                        betfair_place_sp=None,
                        distance=None,
                        horse_name_link=None,
                        race_type=None,
                        tf_comment=None,
                        horse_age=None,
                        race_time=None,
                        created_at=datetime.now(),
                    )
                )
            except Exception as e:
                E(f"Error processing data for url: {url}")
                errors.append(
                    {
                        "error_url": url,
                        "error_message": str(e),
                        "error_processing_time": datetime.now(),
                    }
                )
        data = pd.concat(data)
        data.pipe(
            convert_and_validate_data,
            TimeformDataModel,
            table_string_field_lengths,
            "unique_id",
        )
        processed_data = fetch_data("SELECT * FROM tf_raw.todays_performance_data")
        data = (
            pd.concat([data, processed_data])
            .sort_values(by="created_at", ascending=False)
            .drop_duplicates(subset=["unique_id"])
        )
        if errors:
            errors = pd.DataFrame(errors)
            E(
                f'there were errors processing the following urls: {errors["error_url"].tolist()}'
            )
            processed_error_data = fetch_data(
                "SELECT * FROM errors.todays_performance_data"
            )
            non_duplicated_errors = errors[
                ~errors["error_url"].isin(processed_error_data["error_url"])
            ].drop_duplicates(subset=["error_url"])
            store_data(non_duplicated_errors, "todays_performance_data", "errors")

        data = data[data["race_date"] >= datetime.now().strftime("%Y-%m-%d")]
        store_data(data, "todays_performance_data", "tf_raw", truncate=True)

    driver.quit()
    register_job_completion("scrape_todays_tf_data")


if __name__ == "__main__":
    process_tf_scrape_days_data([TODAYS_DATE_FILTER, TOMORROWS_DATE_FILTER])
