import random
import time

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.storage.psql_db import get_db

db = get_db()


def wait_for_element(driver, css_selector, timeout=10):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
    except TimeoutException:
        print(f"Timeout waiting for element: {css_selector}")


def get_result_links(driver):
    link_elements = driver.find_elements(By.CSS_SELECTOR, "tr.ui-table__row a")
    result_links = []
    for link_element in link_elements:
        href = link_element.get_attribute("href")
        if href and "results" in href and "fullReplay" not in href:
            result_links.append(href)
    return result_links


if __name__ == "__main__":
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")

    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    chromedriver_path = "/Users/tomwattley/chromedriver/chromedriver"
    service = Service(executable_path=chromedriver_path)

    driver = webdriver.Chrome(service=service, options=options)

    driver.get("https://www.racingpost.com/")

    cf = pd.read_csv("~/Desktop/course.csv")

    uk_ire_ids = cf["rp_id"].unique()

    url = "https://www.racingpost.com/profile/horse/horse_id/horse_name/form"

    for i in range(1, 10000000):
        df = db.fetch_data("""

            SELECT DISTINCT horse_name, horse_id 
            FROM rp_raw.performance_data
            WHERE (race_title ILIKE '%group%' OR race_title ILIKE '%listed%')
            AND race_date > '2015-01-01'
            AND horse_id NOT IN (SELECT DISTINCT horse_id FROM rp_raw.checked_horse)
        
        """)

        if df.empty:
            print("No more horses to check")
            break

        df = df.sample(frac=1)
        i = df.iloc[0]
        try:
            id_str = str(i.horse_id)
            horse_name = i.horse_name.replace(" ", "-").lower()
            up_url = url.replace("horse_name", horse_name).replace("horse_id", id_str)
            print(up_url)
            # Wait for the specific element to be present on the page
            wait_for_element(driver, "tr.ui-table__row a")
            driver.get(up_url)
            result_links = get_result_links(driver)
            for link in result_links:
                split = link.split("/")
                course_id = split[-4]
                if int(course_id) in uk_ire_ids:
                    continue
                else:
                    print(f"unseen link {link}")
                    db.store_data(
                        pd.DataFrame(
                            {
                                "horse_id": [i.horse_id],
                                "horse_name": [i.horse_name],
                                "link": [link],
                            }
                        ),
                        "non_uk_group_links",
                        "rp_raw",
                    )
            db.store_data(
                pd.DataFrame(
                    {
                        "horse_id": [i.horse_id],
                        "horse_name": [i.horse_name],
                    }
                ),
                "checked_horse",
                "rp_raw",
            )

        except Exception as e:
            print(f"Error: {e}")
            db.store_data(
                pd.DataFrame(
                    {
                        "horse_id": [i.horse_id],
                        "horse_name": [i.horse_name],
                        "error": [e],
                    }
                ),
                "non_uk_group_links_errors",
                "rp_raw",
            )
