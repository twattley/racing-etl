from dataclasses import fields
from src.transform.data_model import (
    BaseDataModel,
    RaceDataModel,
    TranformedDataModel,
    TransformedDataModel,
    convert_data,
)
import pandas as pd
import re
from src.utils.logging_config import E, I
from src.storage.sql_db import fetch_data, store_data

from datetime import datetime
import numpy as np
import pandas as pd
import re


def time_to_seconds(time_str):
    """Convert a time string in format 'Xm Ys' to total seconds."""
    parts = re.split("[msh]", time_str.strip())
    seconds = 0.0
    if "m" in time_str:
        seconds += float(parts[0]) * 60  # Convert minutes to seconds
        parts.pop(0)
    if parts and parts[0]:  # Check if there's a remaining part for seconds
        seconds += float(parts[0])
    return seconds


def process_time_strings(s: str) -> tuple[float, str, float]:
    if "(standard time)" in s:
        return round(time_to_seconds(s), 2), "standard", 0.0
    if "(" not in s:
        return round(time_to_seconds(s), 2), None, None
    outside, inside = re.match(r"([^\(]+)\(([^)]+)\)", s).groups()
    relative = inside.split("by")[0].strip()
    relative_time = time_to_seconds(inside.split("by")[1].strip())
    total_seconds_time = time_to_seconds(outside.strip())

    return round(total_seconds_time, 2), relative, round(relative_time, 2)


def create_pounds(data: pd.DataFrame) -> pd.DataFrame:
    data["weight_carried_st"] = (
        data["rp_horse_weight"]
        .str.split("-")
        .str.get(0)
        .str.extract(r"(\d+)")
        .astype("Int64")
    )
    data["weight_carried_extra_lbs"] = (
        data["rp_horse_weight"]
        .str.split("-")
        .str.get(1)
        .str.extract(r"(\d+)")
        .astype("Int64")
    )
    data["weight_carried_lbs"] = (data["weight_carried_st"] * 14) + data[
        "weight_carried_extra_lbs"
    ]

    return data


def fill_columns(data: pd.DataFrame) -> pd.DataFrame:
    data = data.assign(formatted_tf_draw=lambda x: x["tf_draw"].str.extract(r"(\d+)"))
    data = data.assign(
        draw=data["rp_draw"].fillna(data["formatted_tf_draw"]),
        finishing_position=data["rp_finishing_position"].fillna(
            data["tf_finishing_position"]
        ),
        age=data["rp_horse_age"].fillna(data["tf_horse_age"]),
        official_rating=data["rp_or_value"].fillna(data["tf_official_rating"]),
        created_at=datetime.now(),
    )
    return data


def get_tf_rating_values(data: pd.DataFrame) -> pd.DataFrame:
    view_map = {
        "+": "positive",
        "?": "questionable",
    }
    data = data.assign(
        tf_rating=lambda x: x["tf_tf_rating"].str.extract(r"(\d+)").astype("Int64"),
    )
    data = data.assign(tf_rating_view=lambda x: x["tf_tf_rating"].str.extract(r"(\D+)"))
    data = data.assign(
        tf_rating_view=lambda x: x["tf_rating_view"].map(view_map).fillna("neutral")
    )

    return data


def get_surfaces_from_tf_rating(data: pd.DataFrame) -> pd.DataFrame:

    data = data.assign(
        surface=np.select(
            [
                data["tf_tf_rating"].str.startswith("a"),
                data["tf_tf_rating"].str.startswith("t"),
                data["tf_tf_rating"].str.startswith("p"),
                data["tf_tf_rating"].str.startswith("f"),
            ],
            ["artificial", "tapeta", "polytrack", "fibresand"],
            default="turf",
        )
    )

    return data


def convert_distances(distance_str: str) -> tuple:
    miles_to_yards = 1760
    furlongs_to_yards = 220
    yards_to_meters = 0.9144

    total_yards = 0

    parts = distance_str.split()
    for part in parts:
        if "m" in part:
            miles = int(part.replace("m", ""))
            total_yards += miles * miles_to_yards
        elif "f" in part:
            furlongs = int(part.replace("f", ""))
            total_yards += furlongs * furlongs_to_yards
        elif "y" in part:
            yards = int(part.replace("y", ""))
            total_yards += yards

    total_meters = total_yards * yards_to_meters
    total_kilometers = total_meters / 1000

    return total_yards, round(total_meters, 2), round(total_kilometers, 2)


def get_inplay_high_and_low(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        in_play_low=pd.to_numeric(
            df["tf_in_play_prices"].str.split("/").str.get(1), errors="coerce"
        ).astype(float),
        in_play_high=pd.to_numeric(
            df["tf_in_play_prices"].str.split("/").str.get(0), errors="coerce"
        ).astype(float),
    )


def convert_headgear(e: str) -> str:
    if not e:
        return np.NaN
    headgear_mapping = {
        "b": "blinkers",
        "t": "tongue tie",
        "p": "cheekpieces",
        "c": "cheekpieces",
        "v": "visor",
        "h": "hood",
        "e/s": "eye shield",
        "e": "eye shield",
    }

    headgear = []
    for i, value in headgear_mapping.items():
        if f"{i}1" in e:
            headgear.append(f"{value} (first time)")
            e = e.replace(f"{i}1", "")
        elif i in e:
            headgear.append(value)
            e = e.replace(i, "")

    first_time_headgear = [i for i in headgear if "first time" in i]

    if first_time_headgear:
        first_time_headgear.extend([i for i in headgear if "first time" not in i])
        return ", ".join(first_time_headgear)

    if not headgear and e:
        raise ValueError(f"Unknown headgear code: {e}")
    return ", ".join(headgear)

def create_distance_data(data: pd.DataFrame) -> pd.DataFrame:
    data[["yards", "meters", "kilometers"]] = data["tf_distance"].apply(
        lambda x: pd.Series(convert_distances(x))
    )
    return data

def create_time_data(data: pd.DataFrame) -> pd.DataFrame:
    data[["time_seconds", "relative", "relative_time"]] = data["rp_winning_time"].apply(
        lambda x: pd.Series(process_time_strings(x))
    )
    return data

def create_headgear_data(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(headgear=lambda x: x["rp_headgear"].apply(convert_headgear))

def rename_data_columns(data: pd.DataFrame) -> pd.DataFrame:
    data = data.rename(
        columns={
            "rp_horse_name": "horse_name",
            "rp_course_name": "course_name",
            "rp_horse_id": "horse_id",
            "rp_jockey_id": "jockey_id",
            "rp_jockey_claim": "jockey_claim",
            "rp_trainer_id": "trainer_id",
            "rp_owner_id": "owner_id",
            "rp_ts_value": "ts",
            "rp_rpr_value": "rpr",
            "rp_horse_price": "industry_sp",
            "rp_horse_weight": "weight_carried_st_lbs",
            "rp_extra_weight": "extra_weight_lbs",
            "rp_comment": "in_race_comment",
            "rp_sire_id": "sire_id",
            "rp_dam_id": "dam_id",
            "rp_race_date": "race_date",
            "rp_race_title": "race_title",
            "rp_race_timestamp": "race_time",
            "rp_conditions": "conditions",
            "rp_distance": "distance",
            "rp_going": "going",
            "rp_winning_time": "winning_time",
            "rp_number_of_runners": "number_of_runners",
            "rp_total_prize_money": "total_prize_money",
            "rp_first_place_prize_money": "first_place_prize_money",
            "rp_course_id": "course_id",
            "rp_race_id": "race_id",
            "rp_country": "country",
            "rp_unique_id": "unique_id",
            "tf_rating": "tfr",
            "tf_tf_speed_figure": "tfig",
            "tf_betfair_win_sp": "betfair_win_sp",
            "tf_betfair_place_sp": "betfair_place_sp",
            "tf_in_play_prices": "in_play_prices",
            "tf_tf_comment": "tf_comment",
            "tf_prize": "prize",
            "tf_hcap_range": "hcap_range",
            "tf_age_range": "age_range",
            "tf_race_type": "race_type",
            "tf_main_race_comment": "main_race_comment",
            "rp_meeting_id": "meeting_id",
        }
    )

    return data


def transform_data(
    data: pd.DataFrame,
    transform_data_model: TransformedDataModel,
    race_data_model: RaceDataModel,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    data = (
        data.pipe(fill_columns)
        .pipe(get_surfaces_from_tf_rating)
        .pipe(get_tf_rating_values)
        .pipe(create_pounds)
        .pipe(get_inplay_high_and_low)
        .pipe(create_distance_data)
        .pipe(create_time_data)
        .pipe(create_headgear_data)
        .pipe(rename_data_columns)
    )
    

    transformed_data = data.pipe(convert_data, transform_data_model)
    race_data = data.pipe(convert_data, race_data_model)

    transformed_data = transformed_data[
        [field.name for field in fields(transform_data_model)]
    ].drop_duplicates(subset=["unique_id"])
    race_data = race_data[
        [field.name for field in fields(race_data_model)]
    ].drop_duplicates(subset=["race_id"])

    return transformed_data, race_data


if __name__ == "__main__":
    transformed_data, race_data = transform_data(
        data=fetch_data("raw
        transform_data_model=Tranformed

