import re

import pandas as pd

from entity_matching.interfaces.entity_matcher_interface import IEntityMatcher


class EntityMatchingService(IEntityMatcher):
    def __init__(self, datasets: tuple[pd.DataFrame]):
        self.datasets = datasets

    def match_data(self) -> pd.DataFrame:
        return self.datasets

    def _cleanup_datasets_for_matching(self) -> tuple[pd.DataFrame]:
        for dataset in self.datasets:
            dataset["cleaned_horse_string"] = dataset["horse"].apply(clean_entity_name)

        return self.datasets


def clean_entity_name(entity_name: str) -> str:
    """
    Given an entity name, remove the country and non-alpha characters and return the cleaned name

    Args:
        entity_name (str): An entity name

    Returns:
        str: A cleaned entity name

    """

    remove_country = re.sub(r"\([^)]*\)", "", entity_name).strip()
    remove_non_alpha_characters = re.sub(r"[^a-zA-Z ]", "", remove_country)
    return (
        remove_non_alpha_characters.lower()
        .replace(" ", "")
        .lstrip("0123456789.")
        .strip()
    )
