import numpy as np
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pytz
import re
import json
import hashlib
import re

import os
import shutil
import subprocess
import time

import betfairlightweight
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from src.utils.logging_config import LOGGER



def get_element_text_by_selector(row, css_selector):
    elements = row.find_elements(By.CSS_SELECTOR, css_selector)
    return next(
        (element.text.strip() for element in elements if element.text.strip()),
        None,
    )

def return_element_from_css_selector(table_row, css_selector, multiple_elements=False):
    try:
        element = table_row.find_element(By.CSS_SELECTOR, css_selector)
        if multiple_elements:
            element = element[0]
        return element.text
    except Exception:
        print(f"Element not found for css selector: {css_selector}")
        return None


def find_element_text_by_selector(
    row, selector, default="Information not found for this row"
):
    elements = row.find_elements(By.CSS_SELECTOR, selector)
    return elements[0].text if elements else default


def find_element_text_by_selector_strip(
    row, selector, chars_to_strip, default="Information not found for this row"
):
    elements = row.find_elements(By.CSS_SELECTOR, selector)
    return elements[0].text.strip(chars_to_strip) if elements else default


def title_except_brackets(text):
    text = text.title()

    def uppercase_match(match):
        return match.group(0).upper()

    return re.sub(r"\([^)]*\)", uppercase_match, text)


def get_main_race_comment(driver):

    premium_comment_elements = driver.find_elements(
        By.CSS_SELECTOR, "td[title='Premium Race Comment']"
    )
    for premium_comment_element in premium_comment_elements:
        first_paragraph_elements = premium_comment_element.find_elements(
            By.TAG_NAME, "p"
        )
        if first_paragraph_elements:
            return first_paragraph_elements[0].text.strip()
    return "No Comment Found"


def get_race_details_from_link(link):
    *_, course, race_date, race_time, course_id, race = link.split("/")
    return {
        "course": course,
        "race_date": race_date,
        "race_time": race_time,
        "race_timestamp": datetime.strptime(
            f"{race_date} {race_time}", "%Y-%m-%d %H%M"
        ),
        "course_id": course_id,
        "race": race,
        "race_id": hashlib.sha256(
            f"{course_id}_{race_date}_{race_time}_{race}".encode()
        ).hexdigest(),
    }


def get_race_details_from_page(driver):
    titles = [
        # (variable name, title attribute of the span element)
        ("distance", "Distance expressed in miles, furlongs and yards"),
        ("going", "Race going"),
        ("prize", "Prize money to winner"),
        ("hcap_range", "BHA rating range"),
        ("age_range", "Horse age range"),
        ("race_type", "The type of race"),
    ]
    elements = driver.find_elements(By.CSS_SELECTOR, "span.rp-header-text")

    values = {var: None for var, _ in titles}
    for var, tf_title in titles:
        for element in elements:
            if element.get_attribute("title") == tf_title:
                values[var] = element.text
                break

    values["main_race_comment"] = get_main_race_comment(driver)

    return values


def get_entity_info_from_row(row, selector, index):
    elements = row.find_elements(By.CSS_SELECTOR, selector)
    if elements:
        element = elements[0]
        entity_name = element.text
        if "Sire" in selector or "Dam" in selector:
            entity_name = title_except_brackets(entity_name)
        entity_id = element.get_attribute("href").split("/")[index]
        return entity_name, entity_id


def get_horse_name(row):
    all_links = row.find_elements(By.TAG_NAME, "a")
    for link in all_links:
        horse_links = row.find_elements(By.CSS_SELECTOR, "a.rp-horse")
        all_hrefs = link.get_attribute("href")
        if "horse-form" in all_hrefs:
            tf_horse_id = all_hrefs.split("/")[-1]
            tf_horse_name_link = all_hrefs.split("/")[-2]
        for horse_link in horse_links:
            horse_name = horse_link.text
            if horse_name.strip():
                tf_horse_name = title_except_brackets(
                    re.sub(r"^\d+\.\s+", "", horse_link.text)
                )
        for link in all_links:
            href = link.get_attribute("href")
            if "sire" in href and "dam_sire" not in href:
                tf_sire_name_link = href.split("/")[-3]
                tf_clean_sire_name = tf_sire_name_link.replace("-", " ").title().strip()

        return tf_horse_name, tf_horse_id, tf_horse_name_link, tf_clean_sire_name


def get_performance_data(driver, race_details_link, race_details_page):
    table_rows = driver.find_elements(By.CLASS_NAME, "rp-table-row")

    data = []
    for row in table_rows:
        performance_data = {}
        performance_data["tf_rating"] = return_element_from_css_selector(
            row, "div.rp-circle.rp-rating.res-rating"
        )
        performance_data["tf_speed_figure"] = return_element_from_css_selector(
            row, "td.al-center.rp-tfig"
        )
        performance_data["draw"] = return_element_from_css_selector(row, "span.rp-draw")
        performance_data["trainer_name"], performance_data["trainer_id"] = (
            get_entity_info_from_row(
                row, "td.rp-jockeytrainer-hide > a[title='Trainer']", -1
            )
        )
        performance_data["jockey_name"], performance_data["jockey_id"] = (
            get_entity_info_from_row(
                row, "td.rp-jockeytrainer-hide > a[title='Jockey']", -1
            )
        )
        performance_data["sire_name"], performance_data["sire_id"] = (
            get_entity_info_from_row(row, "span[title='Sire'] > a", -2)
        )
        performance_data["dam_name"], performance_data["dam_id"] = (
            get_entity_info_from_row(row, "span[title='Dam'] > a", -2)
        )
        performance_data["finishing_position"] = get_element_text_by_selector(
            row, 'span.rp-entry-number[title="Finishing Position"]'
        )
        (
            performance_data["horse_name"],
            performance_data["horse_id"],
            performance_data["horse_name_link"],
            performance_data["sire_name_link"],
        ) = get_horse_name(row)
        performance_data["horse_age"] = find_element_text_by_selector(
            row,
            "td.al-center.rp-body-text.rp-ageequip-hide[title='Horse age']",
            "Horse Age information not found for this row",
        )
        equipment = [
            element.text
            for element in row.find_elements(
                By.CSS_SELECTOR, "td.al-center.rp-body-text.rp-ageequip-hide > span"
            )
        ]
        performance_data["equipment"] = equipment[0] if equipment else None
        performance_data["official_rating"] = find_element_text_by_selector_strip(
            row,
            "td.al-center.rp-body-text.rp-ageequip-hide[title='Official rating given to this horse']",
            "()",
            "Official rating information not found for this row",
        )
        performance_data["fractional_price"] = find_element_text_by_selector(
            row, ".price-fractional", "Price information not found for this row"
        )
        performance_data["betfair_win_sp"] = find_element_text_by_selector(
            row,
            "td.al-center.rp-result-sp.rp-result-bsp-hide[title='Betfair Win SP']",
            "Betfair Win SP information not found for this row",
        )
        performance_data["betfair_place_sp"] = find_element_text_by_selector_strip(
            row,
            "td.al-center.rp-result-sp.rp-result-bsp-hide[title='Betfair Place SP']",
            "()",
            "Betfair Place SP information not found for this row",
        )
        performance_data["in_play_prices"] = find_element_text_by_selector(
            row,
            "td.al-center.rp-body-text.rp-ipprices[title='The hi/lo Betfair In-Play prices with a payout of more than GBP100']",
            "Betfair In-Play prices information not found for this row",
        )
        performance_data["tf_comment"] = find_element_text_by_selector(
            row, "tr.rp-entry-comment.rp-comments.rp-body-text"
        )

        performance_data.update(race_details_link)
        performance_data.update(race_details_page)

        data.append(performance_data)

    return pd.DataFrame(data)


if __name__ == "__main__":
    I("Scrape_links.py execution started.")
    driver = get_headless_driver(timeform=True)
    links = get_links()
    for link in links:
        driver.get(link)
        race_details_link = get_race_details_from_link(link)
        race_details_page = get_race_details_from_page(driver)
        data = get_performance_data(driver, race_details_link, race_details_page)
        store_data(data, "timeform")
