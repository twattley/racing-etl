import hashlib
import re
from datetime import datetime, timedelta
import time
import numpy as np
import pandas as pd
import pytz
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.raw import DataScrapingTask, run_scraping_task
from src.raw.webdriver_base import get_headless_driver
from src.utils.logging_config import E, I
from src.storage.sql_db import fetch_data


BASE_LINK = "https://www.racingpost.com/racecards"
TODAYS_DATE_FILTER = datetime.now().strftime("%Y-%m-%d")
TOMORROWS_DATE_FILTER = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
TODAY_LINK = f"{BASE_LINK}/{TODAYS_DATE_FILTER}"
TOMORROW_LINK = f"{BASE_LINK}/{TOMORROWS_DATE_FILTER}"


def fetch_course_ids() -> pd.DataFrame:
    """
    Fetches the course IDs from the Racing Post website.
    """
    return fetch_data("SELECT rp_id FROM course")["rp_id"].to_list()


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
    *_, course_id, course, date, race_id = url.split("/")
    course = course.replace("-", " ").title().strip()
    return {"course_id": course_id, "course": course, "date": date, "race_id": race_id}


def get_race_time_from_url(driver: webdriver.Chrome, url: str, date: str) -> datetime:
    driver.get(url)
    element = driver.find_element(
        By.XPATH,
        "//span[@class='RC-courseHeader__time'][@data-test-selector='RC-courseHeader__time']",
    )

    time = element.text.strip()
    hours, minutes = time.split(":")

    return {
        "race_time": datetime.strptime(
            f"{date} {int(hours) + 12}:{minutes}", "%Y-%m-%d %H:%M"
        )
    }


def get_race_details(d):
    header_map = {
        "RC-header__raceDistanceRound": "distance_furlongs",
        "RC-header__raceDistance": "distance_yards",
        "RC-header__raceInstanceTitle": "race_name",
        "RC-header__raceClass": "class",
        "RC-header__rpAges": "conditons",
        "RC-ticker__terms": "places",
        "RC-ticker__going": "going",
    }
    parent_div = d.find_element(
        By.XPATH, "//div[contains(@class, 'RC-cardHeader__courseDetails')]"
    )

    child_elements = parent_div.find_elements(By.XPATH, ".//*[@data-test-selector]")

    header_data = {}

    for element in child_elements:
        test_selector = element.get_attribute("data-test-selector")
        text = element.get_attribute("textContent").strip()
        header_data[header_map[test_selector]] = text

    header_data["going"] = header_data["going"].replace("Going: ", "")
    (
        header_data["distance_yards"],
        header_data["distance_meters"],
        header_data["distance_kilometers"],
    ) = convert_distances_v2(
        header_data["distance_yards"].replace("(", "").replace(")", "")
    )

    header_data.pop("places", None)

    return header_data


def main():
    driver = get_headless_driver()
    driver.get(TODAY_LINK)
    hrefs = [
        element.get_attribute("href")
        for element in driver.find_elements(By.XPATH, "//a")
    ]
    pattern = (
        rf"https://www.racingpost.com/racecards/\d+/[a-zA-Z-]+/{TODAYS_DATE_FILTER}/\d+"
    )
    matches = sorted({url for url in hrefs if re.search(pattern, url)})
    urls = return_urls_for_courses_covered(matches, fetch_course_ids())
    for url in urls:
        course_id, course, date, race_id = get_data_from_url(url)
        print(course_id, course, date, race_id)
        race_time = get_race_time_from_url(driver, url, date)
        print(race_time)
        time.sleep(5)


if __name__ == "__main__":
    main()
