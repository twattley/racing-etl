import hashlib
import re
import time
from datetime import datetime, timedelta

import pandas as pd
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
from src.utils.logging_config import I


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


def return_urls_for_courses_covered(
    urls: list[str], course_ids: list[int]
) -> list[str]:
    course_ids = [int(i) for i in course_ids]
    covered_urls = []
    for url in urls:
        url_course_id = url.split("/")[8]
        if int(url_course_id) in course_ids:
            covered_urls.append(url)

    return covered_urls


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
    horse_entries = driver.find_elements(By.CSS_SELECTOR, "tbody.rp-horse-row")
    horse_data = []
    for entry in horse_entries:
        links = entry.find_elements(By.CSS_SELECTOR, "a")
        for link in links:
            href = link.get_attribute("href")
            if href.endswith("/sire"):
                *_, sire_name, sire_id, _ = href.split("/")
            if href.endswith("/dam"):
                *_, dam_name, dam_id, _ = href.split("/")
            if "jockey" in href and "form" in href:
                *_, jockey_name, _, jockey_id = href.split("/")
            if "trainer" in href and "form" in href:
                *_, trainer_name, _, trainer_id = href.split("/")
            if "horse/form" in href:
                *_, horse_name, horse_id, a, b, c, d, e = href.split("/")
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


def process_tf_scrape_days_data(date: datetime):

    base_link = "https://www.timeform.com/horse-racing/racecards"

    data = []
    course_country = fetch_course_data()
    course_ids = course_country["tf_id"].unique()
    driver = get_headless_driver()
    driver.get(base_link)
    click_for_racecards(driver, date)
    hrefs = [
        element.get_attribute("href")
        for element in driver.find_elements(By.XPATH, "//a")
    ]
    pattern = rf"https://www.timeform.com/horse-racing/racecards/(.*)/{date}/([12]\d\d\d)/(.*)/(.*)/(.*)"
    matches = sorted({url for url in hrefs if re.search(pattern, url)})
    urls = return_urls_for_courses_covered(matches, course_ids)
    for url in urls:
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
                debug_link=None,
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
    data = data[data["race_date"] >= datetime.now().strftime("%Y-%m-%d")]
    store_data(data, "todays_performance_data", "tf_raw")
    driver.quit()


if __name__ == "__main__":
    process_tf_scrape_days_data(datetime.now().strftime("%Y-%m-%d"))
    process_tf_scrape_days_data(datetime.now().strftime("%Y-%m-%d") + timedelta(days=1))
