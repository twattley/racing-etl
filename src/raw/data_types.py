from dataclasses import dataclass
from typing import Callable

from selenium import webdriver

from src.data_models.base.base_model import BaseDataModel


@dataclass
class DatabaseInfo:
    schema: str
    job_name: str
    source_view: str
    dest_table: str


@dataclass
class DataScrapingTask:
    driver: webdriver.Chrome
    source_name: str
    schema: str
    source_view: str
    dest_table: str
    job_name: str
    scraper_func: Callable
    data_model: BaseDataModel
    string_fields: dict
    unique_id: str


@dataclass
class LinkScrapingTask:
    driver: webdriver.Chrome
    base_url: str
    schema: str
    source_table: str
    destination_table: str
    filter_func: Callable
