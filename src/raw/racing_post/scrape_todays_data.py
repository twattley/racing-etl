import hashlib
import re
import time
from datetime import datetime, timedelta

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from src.raw import check_already_processed
from src.data_models.base.base_model import convert_and_validate_data
from src.data_models.raw.racing_post_model import (
    RacingPostDataModel,
    table_string_field_lengths,
)
from src.raw.webdriver_base import get_headless_driver
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import E, I, W
from src.utils.processing_utils import register_job_completion

BASE_LINK = "https://www.racingpost.com/racecards"
TODAYS_DATE_FILTER = datetime.now().strftime("%Y-%m-%d")
TOMORROWS_DATE_FILTER = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
TODAY_LINK = f"{BASE_LINK}/{TODAYS_DATE_FILTER}"
TOMORROW_LINK = f"{BASE_LINK}/{TOMORROWS_DATE_FILTER}"


def fetch_course_data() -> pd.DataFrame:
    return fetch_data(
        """
        SELECT * FROM public.course cr
        left join public.country ct on 
        cr.country_id::text = ct.country_id::text
        """
    )


PEDIGREE_OWNNER_SETTINGS_BUTTON_TOGGLED = False


def toggle_buttons(driver):
    global PEDIGREE_OWNNER_SETTINGS_BUTTON_TOGGLED
    if PEDIGREE_OWNNER_SETTINGS_BUTTON_TOGGLED:
        I("Settings already toggled")
        return
    else:
        I("Toggling settings button")
        settings_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    ".RC-cardTabsZone__settingsBtn.js-RC-settingsPopover__openBtn",
                )
            )
        )
        driver.execute_script("arguments[0].click();", settings_button)
        time.sleep(2)

        pedigree_switcher = driver.find_element(
            By.ID, "RC-customizeSettings__switcher_pedigrees"
        )
        owner_switcher = driver.find_element(
            By.ID, "RC-customizeSettings__switcher_owner"
        )
        done_button = driver.find_element(
            By.CSS_SELECTOR, "[data-test-selector='RC-customizeSettings__popoverBtn']"
        )
        time.sleep(2)

        driver.execute_script("arguments[0].click();", pedigree_switcher)
        time.sleep(2)
        driver.execute_script("arguments[0].click();", owner_switcher)
        time.sleep(2)
        driver.execute_script("arguments[0].click();", done_button)
        PEDIGREE_OWNNER_SETTINGS_BUTTON_TOGGLED = True


def return_urls_for_courses_covered(
    urls: list[str], course_ids: list[int]
) -> list[str]:
    course_ids = [int(i) for i in course_ids]
    covered_urls = []
    for url in urls:
        url_course_id = url.split("/")[4]
        if int(url_course_id) in course_ids:
            covered_urls.append(url)

    return covered_urls


def get_data_from_url(url):
    if url.endswith("/"):
        url = url[:-1]
    *_, course_id, course, date, race_id = url.split("/")
    course = course.replace("-", " ").title().strip()
    return {
        "course_id": course_id,
        "course_name": course,
        "course": course,
        "race_date": date,
        "race_id": race_id,
    }


def get_race_time(driver: webdriver.Chrome, date: str) -> datetime:
    element = driver.find_element(
        By.XPATH,
        "//span[@class='RC-courseHeader__time'][@data-test-selector='RC-courseHeader__time']",
    )
    time = element.text.strip()
    hours, minutes = time.split(":")
    hours = int(hours)
    if hours < 10:
        hours += 12
    return {
        "race_timestamp": datetime.strptime(
            f"{date} {hours}:{minutes}", "%Y-%m-%d %H:%M"
        )
    }


def get_surface(driver: webdriver.Chrome) -> str:
    course_name_element = driver.find_element(By.CLASS_NAME, "RC-courseHeader__name")
    course_name_text = course_name_element.text.strip()
    return "AW" if "AW" in course_name_text else "Turf"


def get_race_details(driver: webdriver.Chrome) -> dict:

    header_map = {
        "RC-header__raceDistanceRound": "distance",
        "RC-header__raceDistance": "distance_full",
        "RC-header__raceInstanceTitle": "race_title",
        "RC-header__raceClass": "race_class",
        "RC-header__rpAges": "conditions",
        "RC-ticker__winner": "first_place_prize_money",
        "RC-headerBox__winner": "first_place_prize_money",
        "RC-headerBox__runners": "number_of_runners",
        "RC-headerBox__going": "going",
    }
    parent_divs = driver.find_elements(
        By.XPATH,
        "//div[contains(@class, 'RC-cardHeader__courseDetails') or contains(@class, 'RC-cardHeader__keyInfo')]",
    )

    header_data = {}
    for div in parent_divs:
        child_elements = div.find_elements(By.XPATH, ".//*[@data-test-selector]")
        for element in child_elements:
            test_selector = element.get_attribute("data-test-selector")
            text = element.get_attribute("textContent").strip()
            if test_selector in header_map:
                header_data[header_map[test_selector]] = text

    header_data["going"] = header_data.get("going", "").replace("Going: ", "")
    header_data["distance_yards"] = (
        header_data.get("distance_yards", "")
        .replace("yds", "")
        .replace("(", "")
        .replace(")", "")
    )
    if "places" in header_data:
        header_data.pop("places", None)

    header_data["number_of_runners"] = (
        header_data["number_of_runners"]
        .replace("Runners:", "")
        .replace("\n", "")
        .strip()
        .split(" ")[0]
    )
    header_data["going"] = (
        header_data["going"].replace("Going:", "").replace("\n", "").strip()
    )
    header_data["surface"] = get_surface(driver)
    prize_money = (
        header_data["first_place_prize_money"].replace("Winner:\n", "").strip()
    )
    prize_money = (
        round(int(prize_money.replace(",", "").replace("€", "").replace("£", "")), -3)
        // 1000
    )

    header_data["first_place_prize_money"] = prize_money

    return header_data


def get_entity_data_from_link(entity_link):
    entity_id, entity_name = entity_link.split("/")[-2:]
    entity_name = " ".join(i.title() for i in entity_name.split("-"))
    return entity_id, entity_name


def get_optional_element_text(row, css_selector):
    try:
        return row.find_element(By.CSS_SELECTOR, css_selector).text.strip()
    except Exception:
        return None


def clean_entity_name(entity_name):
    return entity_name.replace("-", " ").title().strip()


def get_horse_data(driver: webdriver.Chrome) -> pd.DataFrame:
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, ".RC-runnerRow.js-RC-runnerRow.js-PC-runnerRow")
        )
    )
    runner_rows = driver.find_elements(
        By.CSS_SELECTOR, ".RC-runnerRow.js-RC-runnerRow.js-PC-runnerRow"
    )
    horse_data = []
    for row in runner_rows:
        horse = row.find_element(By.CSS_SELECTOR, "a.RC-runnerName")
        runner_no = row.find_element(
            By.CSS_SELECTOR, "[data-test-selector='RC-cardPage-runnerNumber-no']"
        ).text.strip()
        if "NR" in runner_no:
            W(f"Runner {horse.text.strip()} is a non-runner")
            continue
        if "R" in runner_no:
            W(f"Runner {horse.text.strip()} is a reserve")
            continue
        horse = row.find_element(By.CSS_SELECTOR, "a.RC-runnerName")
        color_sex = row.find_element(
            By.CSS_SELECTOR, "span[data-test-selector='RC-pedigree__color-sex']"
        )
        sire_link_element = row.find_element(
            By.CSS_SELECTOR, "a[data-test-selector='RC-pedigree__sire']"
        )
        dam_link_element = row.find_element(
            By.CSS_SELECTOR, "a[data-test-selector='RC-pedigree__dam']"
        )
        jockey_element = row.find_elements(
            By.CSS_SELECTOR, "a[data-test-selector='RC-cardPage-runnerJockey-name']"
        )
        trainer_link_element = row.find_element(
            By.CSS_SELECTOR, "a[data-test-selector='RC-cardPage-runnerTrainer-name']"
        )
        owner_link_element = row.find_element(
            By.CSS_SELECTOR, "a[data-test-selector='RC-cardPage-runnerOwner-name']"
        )
        horse_href = horse.get_attribute("href")
        horse_id = horse_href.split("/")[5].strip()
        horse_name = horse_href.split("/")[6].strip().split("#")[0]
        sire_href = sire_link_element.get_attribute("href")
        sire_name, sire_id = sire_href.split("/")[-1], sire_href.split("/")[-2]
        dam_href = dam_link_element.get_attribute("href")
        dam_name, dam_id = dam_href.split("/")[-1], dam_href.split("/")[-2]
        owner_href = owner_link_element.get_attribute("href")
        owner_name, owner_id = owner_href.split("/")[-1], owner_href.split("/")[-2]
        jockey_href = jockey_element[0].get_attribute("href")
        jockey_name, jockey_id = jockey_href.split("/")[-1], jockey_href.split("/")[-2]
        trainer_href = trainer_link_element.get_attribute("href")
        trainer_name, trainer_id = (
            trainer_href.split("/")[-1],
            trainer_href.split("/")[-2],
        )
        headgear = get_optional_element_text(row, ".RC-runnerHeadgearCode")
        age = get_optional_element_text(row, ".RC-runnerAge")
        weight_carried_st = get_optional_element_text(row, ".RC-runnerWgt__carried_st")
        weight_carried_lb = get_optional_element_text(row, ".RC-runnerWgt__carried_lb")
        weight_carried = f"{weight_carried_st}-{weight_carried_lb}"
        jockey_claim = get_optional_element_text(
            row,
            "span.RC-runnerInfo__count[data-test-selector='RC-cardPage-runnerJockey-allowance']",
        )
        draw = get_optional_element_text(
            row,
            "span.RC-runnerNumber__draw[data-test-selector='RC-cardPage-runnerNumber-draw']",
        )
        official_rating_element = row.find_element(
            By.CSS_SELECTOR, ".RC-runnerOr[data-test-selector='RC-cardPage-runnerOr']"
        )
        official_rating = (
            official_rating_element.text.strip() if official_rating_element else None
        )

        horse_data.append(
            {
                "horse_name": clean_entity_name(horse_name),
                "horse_id": horse_id,
                "horse_type": color_sex.text.strip(),
                "sire_name": clean_entity_name(sire_name),
                "sire_id": sire_id,
                "dam_name": clean_entity_name(dam_name),
                "dam_id": dam_id,
                "owner_name": clean_entity_name(owner_name),
                "owner_id": owner_id,
                "jockey_name": clean_entity_name(jockey_name),
                "jockey_id": jockey_id,
                "trainer_name": clean_entity_name(trainer_name),
                "trainer_id": trainer_id,
                "headgear": headgear.strip() if headgear else None,
                "horse_age": age.strip() if age else None,
                "horse_weight": weight_carried.strip() if weight_carried else None,
                "jockey_claim": jockey_claim.strip() if jockey_claim else None,
                "draw": draw.strip() if draw else None,
                "official_rating": official_rating,
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
        pattern = rf"https://www.racingpost.com/racecards/{i.rp_id}/{i.rp_name}/{date}/\d{{1,10}}$"
        patterns.append(pattern)

    if not patterns:
        raise ValueError(f"No patterns found on date: {date}")

    return sorted(
        {url for url in hrefs for pattern in patterns if re.search(pattern, url)}
    )


def process_rp_scrape_days_data(dates: list[str]):
    if check_already_processed('scrape_todays_rp_data'):
        I("Todays RP results data already processed")
        return
    I("Todays RP results data scraping started.")
    base_link = "https://www.racingpost.com/racecards"
    pipeline_errors = []
    data = []
    course_country = fetch_course_data()
    course_country["rp_id"].to_list()
    country_map = dict(zip(course_country["rp_id"], course_country["country_name"]))
    driver = get_headless_driver()
    for date in dates:
        link = f"{base_link}/{date}"
        driver.get(link)
        urls = get_links(driver, course_country, date)

        # get the first url just to toggle the pedigree and owner settings
        driver.get(urls[0])
        toggle_buttons(driver)

        for url in urls:
            try:
                I(f"Scraping data from: {url}")
                driver.get(url)
                race_data = get_data_from_url(url)
                race_time = get_race_time(driver, race_data["race_date"])
                header_data = get_race_details(driver)
                horse_data = get_horse_data(driver)
                horse_data = horse_data.assign(
                    **race_data,
                    **race_time,
                    **header_data,
                    race_time=None,
                    horse_price=None,
                    finishing_position=None,
                    rpr_value=None,
                    debug_link=url,
                    total_distance_beaten=None,
                    ts_value=None,
                    total_prize_money=None,
                    currency=None,
                    winning_time=None,
                    dams_sire_id=None,
                    extra_weight=None,
                    dams_sire=None,
                    comment=None,
                    country=country_map.get(race_data["course_id"]),
                    created_at=datetime.now(),
                )

                horse_data = horse_data.assign(
                    unique_id=lambda x: x.apply(
                        lambda y: hashlib.sha512(
                            f"racing_post{y['horse_id']}{y['horse_weight']}{y['race_title']}".encode()
                        ).hexdigest(),
                        axis=1,
                    ),
                    meeting_id=lambda x: x.apply(
                        lambda y: hashlib.sha512(
                            f"{y['course_id']}{y['race_date']}".encode()
                        ).hexdigest(),
                        axis=1,
                    ),
                ).drop(columns=["distance_yards"])
                data.append(horse_data)
            except Exception as e:
                E(f"Error processing data for url: {url} - {str(e)}")
                pipeline_errors.append(
                    {
                        "error_url": url,
                        "error_message": str(e),
                        "error_processing_time": datetime.now(),
                    }
                )
        if not data:
            raise ValueError(f"No data found on date: {date} for Racing Post")
        data = pd.concat(data)
        data.pipe(
            convert_and_validate_data,
            RacingPostDataModel,
            table_string_field_lengths,
            "unique_id",
        )
        processed_data = fetch_data("SELECT * FROM rp_raw.todays_performance_data")
        data = (
            pd.concat([data, processed_data])
            .sort_values(by="created_at", ascending=False)
            .drop_duplicates(subset=["unique_id"])
        )
        if pipeline_errors:
            errors = pd.DataFrame(pipeline_errors)
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
        store_data(data, "todays_performance_data", "rp_raw", truncate=True)
    driver.quit()
    register_job_completion("scrape_todays_rp_data")


if __name__ == "__main__":
    process_rp_scrape_days_data([TOMORROWS_DATE_FILTER])
