from selenium.webdriver.common.by import By

from src.raw import LinkScrapingTask, run_scraping_task
from src.raw.webdriver_base import get_headless_driver

BASE_RP_URL = "https://www.racingpost.com"
RP_RESULTS_URL = f"{BASE_RP_URL}/results/"


def get_results_links(driver):
    links = driver.find_elements(By.CSS_SELECTOR, "a[href*='results/']")
    hrefs = [link.get_attribute("href") for link in links]
    return list(
        {
            i
            for i in hrefs
            if "fullReplay" not in i
            and len(i.split("/")) == 8
            and "winning-times" not in i
        }
    )


def process_rp_scrape_links():
    task = LinkScrapingTask(
        driver=get_headless_driver(),
        base_url=RP_RESULTS_URL,
        schema="rp_raw",
        source_table="missing_dates",
        destination_table="days_results_links",
        filter_func=get_results_links,
    )
    run_scraping_task(task)


if __name__ == "__main__":
    process_rp_scrape_links()
