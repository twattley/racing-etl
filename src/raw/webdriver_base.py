import os
import random
import time

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from src.utils.logging_config import I


class WebDriverBuilder:
    def __init__(self, headless_mode=False):
        self.headless_mode = headless_mode
        self.user_agent = self._get_random_user_agent()
        self.driver = self._create_driver()

    def _create_driver(self):
        options = Options()
        if self.headless_mode:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        prefs = {
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)

        options.add_argument(f"user-agent={self.user_agent}")

        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
        service = Service(executable_path=chromedriver_path)

        return webdriver.Chrome(service=service, options=options)

    @staticmethod
    def _get_random_user_agent():
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 14_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Linux; Android 9; SM-G960F Build/PPR1.180610.011) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 8.0.0; SM-N950F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (X11; CrOS x86_64 13729.56.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.95 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1; rv:88.0) Gecko/20100101 Firefox/88.0",
        ]
        return random.choice(user_agents)

    @classmethod
    def create_with_random_user_agent(cls, headless_mode=False):
        I(f"Creating a new WebDriver with headless mode set to {headless_mode}")
        return cls(headless_mode=headless_mode)


def login_to_timeform(driver):
    # sourcery skip: extract-duplicate-method
    I("Logging in to Timeform")
    hr_login_page = "https://www.timeform.com/horse-racing/account/sign-in?returnUrl=%2Fhorse-racing"
    time.sleep(5)
    driver.get(hr_login_page)
    time.sleep(5)
    email = driver.find_element(by=By.NAME, value="EmailAddress")
    time.sleep(2)
    email.send_keys(os.environ.get("TF_EMAIL"))
    time.sleep(3)
    password = driver.find_element(by=By.NAME, value="Password")
    time.sleep(3)
    password.send_keys(os.environ.get("TF_PASSWORD"))
    time.sleep(3)
    driver.find_element(by=By.CLASS_NAME, value="submit-section").click()
    time.sleep(3)
    I("Log in to Timeform success")

    return driver


def get_headless_driver(timeform=False):
    I(f"Creating a new headless WebDriver for Timeform: {timeform}")
    driver = WebDriverBuilder.create_with_random_user_agent(headless_mode=True).driver
    return login_to_timeform(driver) if timeform else driver


def get_driver(timeform=False):
    I(f"Creating a new WebDriver for Timeform: {timeform}")
    driver = WebDriverBuilder.create_with_random_user_agent().driver
    return login_to_timeform(driver) if timeform else driver


def is_driver_session_valid(driver):
    try:
        _ = driver.current_url
        return True
    except WebDriverException:
        return False
