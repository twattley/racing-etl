import time

import numpy as np
import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.raw.interfaces.link_scraper_interface import ILinkScraper


class TFResultsLinkScraper(ILinkScraper):
    WORLD_COURSES = [
        "rosehill",
        "randwick",
        "toulouse",
        "nakayama",
        "auteuil",
        "keeneland",
        "cologne",
        "saratoga",
        "tokyo",
        "sam-houston-race-park",
        "ellis-park",
        "aqueduct",
        "deauville",
        "prairie-meadows",
        "monmouth-park",
        "happy-valley",
        "doomben",
        "bendigo",
        "sha-tin",
        "meydan",
        "turfway-park",
        "belmont-park",
        "del-mar",
        "tampa-bay-downs",
        "jebel-ali",
        "krefeld",
        "delaware-park",
        "hanover",
        "milan",
        "lone-star-park",
        "flemington",
        "fontainebleau",
        "cagnes-sur-mer",
        "santa-anita",
        "caulfield",
        "pimlico",
        "colonial-downs",
        "munich",
        "geelong",
        "eagle-farm",
        "kentucky-downs",
        "sandown",
        "oaklawn-park",
        "northlands-park",
        "golden-gate-fields",
        "maisons-laffitte",
        "clairefontaine",
        "hamburg",
        "ascot",
        "hastings-racecourse",
        "baden-baden",
        "gulfstream-park",
        "angers",
        "kyoto",
        "parx",
        "hanshin",
        "saint-cloud",
        "churchill-downs",
        "vichy",
        "abu-dhabi",
        "los-alamitos",
        "presque-isle-downs",
        "rome",
        "longchamp",
        "fair-grounds",
        "woodbine",
        "penn-national",
        "laurel-park",
        "hoppegarten",
        "frankfurt",
        "chantilly",
        "warwick-farm",
        "dusseldorf",
        "arlington",
        "doha",
        "moonee-valley",
        "dortmund",
        "thistledown",
    ]
    UK_IRE_COURSES = [
        "kempton-park",
        "lingfield-park",
        "dundalk",
        "chepstow",
        "tramore",
        "pontefract",
        "windsor",
        "musselburgh",
        "punchestown",
        "newmarket",
        "tipperary",
        "downpatrick",
        "towcester",
        "leopardstown",
        "doncaster",
        "carlisle",
        "epsom-downs",
        "wolverhampton",
        "ffos-las",
        "warwick",
        "fontwell-park",
        "wexford",
        "thirsk",
        "fakenham",
        "naas",
        "ludlow",
        "bellewstown",
        "cartmel",
        "exeter",
        "bangor-on-dee",
        "newcastle",
        "uttoxeter",
        "newton-abbot",
        "ballinrobe",
        "catterick-bridge",
        "roscommon",
        "limerick",
        "leicester",
        "galway",
        "fairyhouse",
        "chelmsford-city",
        "southwell",
        "wetherby",
        "bath",
        "gowran-park",
        "cheltenham",
        "hamilton-park",
        "york",
        "ripon",
        "killarney",
        "hexham",
        "down-royal",
        "listowel",
        "hereford",
        "ascot",
        "stratford-on-avon",
        "worcester",
        "curragh",
        "aintree",
        "market-rasen",
        "cork",
        "haydock-park",
        "salisbury",
        "ayr",
        "taunton",
        "yarmouth",
        "sligo",
        "redcar",
        "kilbeggan",
        "laytown",
        "sandown-park",
        "kelso",
        "clonmel",
        "sedgefield",
        "nottingham",
        "huntingdon",
        "chester",
        "plumpton",
        "thurles",
        "beverley",
        "newbury",
        "goodwood",
        "wincanton",
        "navan",
        "brighton",
        "perth",
    ]

    def scrape_links(self, driver: webdriver.Chrome, date: str) -> pd.DataFrame:
        driver.get(f"https://www.timeform.com/horse-racing/results/{str(date)}")
        time.sleep(5)
        days_results_links = self._get_results_links(driver)
        data = pd.DataFrame(
            {
                "race_date": date,
                "link_url": days_results_links,
            }
        )
        data = data.assign(
            course_id=data["link_url"].str.split("/").str[8],
            course_name=data["link_url"].str.split("/").str[5],
        )
        data = data.assign(
            country_category=np.select(
                [
                    data["course_id"].isin(self.UK_IRE_COURSES),
                    data["course_id"].isin(self.WORLD_COURSES),
                ],
                [1, 2],
                default=0,
            ),
        )
        return data

    def _get_pages_results_links(self, driver: webdriver.Chrome) -> list[str]:
        elements = driver.find_elements(
            By.CSS_SELECTOR, 'a.results-title[href*="/horse-racing/result/"]'
        )
        return [element.get_attribute("href") for element in elements]

    def _get_results_links(self, driver: webdriver.Chrome) -> list[str]:
        pages_links = self._get_pages_results_links(driver)
        buttons = driver.find_elements(
            By.CSS_SELECTOR, "button.w-course-region-tabs-button"
        )
        for button in buttons:
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
            button.click()
            time.sleep(10)
            pages_links.extend(self._get_pages_results_links(driver))

        return list(set(pages_links))
