import time

import numpy as np
import pandas as pd
from api_helpers.helpers.logging_config import I
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.raw.interfaces.link_scraper_interface import ILinkScraper


class RPResultsLinkScraper(ILinkScraper):
    WORLD_COURSES = [
        "saratoga",
        "tokyo",
        "ellis-park",
        "aqueduct",
        "deauville",
        "prairie-meadows",
        "monmouth-park",
        "happy-valley",
        "doomben",
        "morphettville",
        "bendigo",
        "hastings",
        "san-siro",
        "sha-tin",
        "meydan",
        "turfway-park",
        "belmont-park",
        "del-mar",
        "tampa-bay-downs",
        "jebel-ali",
        "krefeld",
        "delaware-park",
        "mountaineer-park",
        "hanover",
        "gold-coast",
        "charles-town",
        "ascot-aus",
        "lone-star-park",
        "flemington",
        "fontainebleau",
        "cagnes-sur-mer",
        "santa-anita",
        "caulfield",
        "pimlico",
        "colonial-downs",
        "munich",
        "gosford",
        "geelong",
        "eagle-farm",
        "kentucky-downs",
        "northam",
        "oaklawn-park",
        "golden-gate-fields",
        "maisons-laffitte",
        "clairefontaine",
        "sandown-aus",
        "hamburg",
        "hastings-racecourse",
        "baden-baden",
        "kembla-grange",
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
        "capannelle",
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
        "sam-houston",
        "doha",
        "moonee-valley",
        "dortmund",
        "thistledown",
        "canberra",
        "arlington-park",
        "newcastle-aus",
        "rosehill",
        "randwick",
        "toulouse",
        "nakayama",
        "auteuil",
        "keeneland",
        "cologne",
    ]

    UK_IRE_COURSES = [
        "kempton-aw",
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
        "catterick",
        "wolverhampton-aw",
        "carlisle",
        "ffos-las",
        "warwick",
        "wexford",
        "thirsk",
        "fakenham",
        "wexford-rh",
        "naas",
        "ludlow",
        "epsom",
        "bellewstown",
        "cartmel",
        "exeter",
        "bangor-on-dee",
        "newcastle",
        "uttoxeter",
        "newton-abbot",
        "ballinrobe",
        "southwell-aw",
        "roscommon",
        "limerick",
        "leicester",
        "galway",
        "fairyhouse",
        "chelmsford-aw",
        "southwell",
        "haydock",
        "wetherby",
        "sandown",
        "bath",
        "gowran-park",
        "cheltenham",
        "newmarket-july",
        "york",
        "ripon",
        "killarney",
        "hexham",
        "down-royal",
        "listowel",
        "hereford",
        "ascot",
        "stratford",
        "worcester",
        "fontwell",
        "curragh",
        "aintree",
        "market-rasen",
        "cork",
        "kempton",
        "salisbury",
        "ayr",
        "taunton",
        "yarmouth",
        "lingfield-aw",
        "sligo",
        "redcar",
        "kilbeggan",
        "newcastle-aw",
        "laytown",
        "hamilton",
        "kelso",
        "clonmel",
        "sedgefield",
        "nottingham",
        "huntingdon",
        "dundalk-aw",
        "chester",
        "plumpton",
        "lingfield",
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
        driver.get(f"https://www.racingpost.com/results/{date}")
        time.sleep(5)
        days_results_links = self._get_results_links(driver)
        I(f"Found {len(days_results_links)} valid links for date {date}.")
        data = pd.DataFrame(
            {
                "race_date": date,
                "link_url": days_results_links,
            }
        )
        data = data.assign(
            course_id=data["link_url"].str.split("/").str[4],
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

    def _get_results_links(self, driver: webdriver.Chrome) -> list[str]:
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
