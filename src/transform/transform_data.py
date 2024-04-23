import re
from dataclasses import fields

import numpy as np
import pandas as pd

from src.data_models.base.base_model import convert_and_validate_data
from src.data_models.transform.race_model import RaceDataModel
from src.data_models.transform.race_model import (
    table_string_field_lengths as race_string_field_lengths,
)
from src.data_models.transform.transformed_model import TransformedDataModel
from src.data_models.transform.transformed_model import (
    table_string_field_lengths as transform_string_field_lengths,
)
from src.storage.sql_db import fetch_data, store_data
from src.utils.logging_config import I, W
from src.utils.processing_utils import pt


def map_race_time_column(data: pd.DataFrame) -> pd.DataFrame:
    I("Mapping race time column")
    data = data.rename(
        columns={"race_time": "race_time_original", "race_timestamp": "race_time"}
    )
    return data


def time_to_seconds(time_str):
    """Convert a time string in format 'Xm Ys' to total seconds."""
    parts = re.split("[msh]", time_str.strip())
    seconds = 0.0
    if "m" in time_str:
        seconds += float(parts[0]) * 60
        parts.pop(0)
    if parts and parts[0]:
        seconds += float(parts[0])
    return seconds


def process_time_strings(s: str) -> tuple[float, str, float]:
    if s in {"0.00s (standard time)", "0.00s", ""}:
        return np.NaN, np.NaN, np.NaN
    if "(standard time)" in s:
        return (
            round(time_to_seconds(s.replace("(standard time)", "")), 2),
            "standard",
            0.0,
        )
    if "(" not in s:
        return round(time_to_seconds(s), 2), None, None
    outside, inside = re.match(r"([^\(]+)\(([^)]+)\)", s).groups()
    relative_to_standard = inside.split("by")[0].strip()
    relative_time = time_to_seconds(inside.split("by")[1].strip())
    total_seconds_time = time_to_seconds(outside.strip())

    return round(total_seconds_time, 2), relative_to_standard, round(relative_time, 2)


def create_pounds(data: pd.DataFrame) -> pd.DataFrame:
    I("Creating pounds")
    data["weight_carried_st"] = (
        data["weight_carried"]
        .str.split("-")
        .str.get(0)
        .str.extract(r"(\d+)")
        .astype("Int64")
    )
    data["weight_carried_extra_lbs"] = (
        data["weight_carried"]
        .str.split("-")
        .str.get(1)
        .str.extract(r"(\d+)")
        .astype("Int64")
    )
    data["weight_carried_lbs"] = (data["weight_carried_st"] * 14) + data[
        "weight_carried_extra_lbs"
    ]

    return data


def get_tf_rating_view(rating: str) -> str:
    if pd.isna(rating) or not rating:
        return "neutral"
    if rating.endswith("+"):
        return "positive"
    elif rating.endswith("?"):
        return "questionable"
    else:
        return "neutral"


def get_tf_rating_values(data: pd.DataFrame) -> pd.DataFrame:
    I("Getting tf rating values")
    data["tfr_view"] = data["tfr"].apply(get_tf_rating_view)
    return data


def get_surface_type(rating: str) -> str:
    if pd.isna(rating) or not rating:
        return "turf"
    if rating.startswith("a"):
        return "artificial"
    elif rating.startswith("t"):
        return "tapeta"
    elif rating.startswith("p"):
        return "polytrack"
    elif rating.startswith("f"):
        return "fibresand"
    else:
        return "turf"


def get_surfaces_from_tf_rating(data: pd.DataFrame) -> pd.DataFrame:
    I("Getting surfaces from tf rating")
    data["surface"] = data["tfr"].apply(get_surface_type)
    return data


def convert_distances(distance: str) -> tuple:

    frac_map = {
        "½": 0.5,
        "⅓": 0.33,
        "⅔": 0.66,
        "¼": 0.25,
        "¾": 0.75,
        "⅕": 0.2,
        "⅖": 0.4,
        "⅗": 0.6,
        "⅘": 0.8,
        "⅙": 0.167,
        "⅚": 0.833,
        "⅐": 0.143,
        "⅛": 0.125,
        "⅜": 0.375,
        "⅝": 0.625,
        "⅞": 0.875,
    }
    if pd.isna(distance) or not distance:
        return np.nan, np.nan, np.nan
    miles_to_yards = 1760
    furlongs_to_yards = 220
    yards_to_meters = 0.9144

    total_yards = 0
    m, f = 0, 0
    if "m" in distance:
        parts = distance.split("m")
        m = int(parts[0])
        f_part = parts[1] if len(parts) > 1 else ""
    else:
        f_part = distance

    for frac_symbol, decimal_value in frac_map.items():
        if frac_symbol in f_part:
            f = int(f_part.split(frac_symbol)[0]) if f_part.split(frac_symbol)[0] else 0
            f += decimal_value
            break
    else:
        if f_part.replace("f", ""):
            f = int(f_part.replace("f", ""))

    total_yards += m * miles_to_yards + f * furlongs_to_yards
    total_meters = total_yards * yards_to_meters
    total_kilometers = total_meters / 1000

    return total_yards, round(total_meters, 2), round(total_kilometers, 2)


def convert_distances_full(distance: str) -> tuple:
    if pd.isna(distance) or not distance:
        return np.nan, np.nan, np.nan
    miles_to_yards = 1760
    furlongs_to_yards = 220
    yards_to_meters = 0.9144
    numbers = [int(i) for i in re.sub(r"[^\d]", " ", distance).split(" ") if i]
    if len(numbers) == 3:
        m, f, y = numbers
    else:
        f, y = numbers
        m = 0

    total_yards = m * miles_to_yards + f * furlongs_to_yards + y
    total_meters = total_yards * yards_to_meters
    total_kilometers = total_meters / 1000

    return total_yards, round(total_meters, 2), round(total_kilometers, 2)


def get_inplay_high_and_low(df: pd.DataFrame) -> pd.DataFrame:
    I("Getting inplay high and low")
    return df.assign(
        in_play_low=pd.to_numeric(
            df["in_play_prices"].str.split("/").str.get(1), errors="coerce"
        ).astype(float),
        in_play_high=pd.to_numeric(
            df["in_play_prices"].str.split("/").str.get(0), errors="coerce"
        ).astype(float),
    )


def convert_headgear(e: str) -> str:
    if not e:
        return np.NaN
    headgear_mapping = {
        "b": "blinkers",
        "t": "tongue tie",
        "p": "cheekpieces",
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


def convert_distance_to_float(distance_str):
    text_code_to_numeric = {
        "dht": 0,
        "nse": 0.01,
        "shd": 0.1,
        "sht-hd": 0.1,
        "hd": 0.2,
        "sht-nk": 0.3,
        "snk": 0.3,
        "nk": 0.6,
        "dist": 999,
    }
    if pd.isna(distance_str) or not distance_str:
        return 0.0
    clean_str = distance_str.strip("[]")

    if clean_str in text_code_to_numeric:
        return text_code_to_numeric[clean_str]

    if not clean_str:
        return 0.0

    match = re.match(r"(\d+)?(?:\s*)?([½¼¾⅓⅔⅕⅖⅗⅘⅙⅚⅛⅜⅝⅞])?", clean_str)
    whole_number, fraction = match[1], match[2]

    whole_number_part = float(whole_number) if whole_number else 0.0

    fraction_to_decimal = {
        "½": 0.5,
        "⅓": 0.33,
        "⅔": 0.66,
        "¼": 0.25,
        "¾": 0.75,
        "⅕": 0.2,
        "⅖": 0.4,
        "⅗": 0.6,
        "⅘": 0.8,
        "⅙": 0.167,
        "⅚": 0.833,
        "⅛": 0.125,
        "⅜": 0.375,
        "⅝": 0.625,
        "⅞": 0.875,
    }
    fraction_part = fraction_to_decimal.get(fraction, 0.0)

    return whole_number_part + fraction_part


def convert_horse_type_to_colour_sex(data: pd.DataFrame) -> pd.DataFrame:
    I("Converting horse type to colour and sex")
    colour_map = {
        "b": "Bay",
        "ch": "Chestnut",
        "bb": "Blood Bay",
        "bl": "Blue",
        "br": "Brown",
        "gr": "Grey",
        "rg": "Roan",
        "ro": "Roan",
        "bg": "Bay",
        "gb": "Grey Bay",
        "bz": "Bronze",
        "bk": "Black",
        "sk": "Skewbald",
        "wh": "White",
    }
    sex_map = {
        "c": "Colt",
        "f": "Filly",
        "g": "Gelding",
        "h": "Horse",
        "m": "Mare",
        "r": "Ridgling",
    }

    data = data.assign(
        horse_colour=data["horse_type"]
        .str.split(",")
        .str.get(0)
        .str.strip()
        .map(colour_map),
        horse_sex=data["horse_type"].str.split(",").str.get(1).str.strip().map(sex_map),
    ).drop(columns=["horse_type"])

    return data


def create_distance_beaten_data(data: pd.DataFrame) -> pd.DataFrame:
    I("Creating distance beaten data")
    data = data.assign(
        total_distance_beaten_str=data["total_distance_beaten"],
        total_distance_beaten=data["total_distance_beaten"].apply(
            convert_distance_to_float
        ),
    )
    # second_place_mapping = data[data["finishing_position"] == "2"].set_index("race_id")[
    #     "total_distance_beaten"
    # ]
    # condition = data["finishing_position"] == "1"
    # data.loc[condition, "total_distance_beaten"] = (
    #     data[condition]["race_id"].map(second_place_mapping) * -1
    # )

    return data


def clean_race_class_field(data: pd.DataFrame) -> pd.DataFrame:
    I("Cleaning race class field")
    data["race_class"] = (
        data["race_class"]
        .str.replace("(Class", "")
        .str.replace(")", "")
        .str.strip()
        .astype("Int64")
    )
    return data


def create_distance_data(data: pd.DataFrame) -> pd.DataFrame:
    I("Creating distance data")
    data[["distance_yards", "distance_meters", "distance_kilometers"]] = data[
        "distance"
    ].apply(lambda x: pd.Series(convert_distances(x)))
    data[
        ["distance_yards_full", "distance_meters_full", "distance_kilometers_full"]
    ] = data["distance_full"].apply(lambda x: pd.Series(convert_distances_full(x)))
    data = data.assign(
        distance_yards=lambda x: x["distance_yards_full"].fillna(x["distance_yards"]),
        distance_meters=lambda x: x["distance_meters_full"].fillna(
            x["distance_meters"]
        ),
        distance_kilometers=lambda x: x["distance_kilometers_full"].fillna(
            x["distance_kilometers"]
        ),
    ).drop(
        columns=[
            "distance_full",
            "distance_yards_full",
            "distance_meters_full",
            "distance_kilometers_full",
        ]
    )
    return data


def create_time_data(data: pd.DataFrame) -> pd.DataFrame:
    I("Creating time data")
    data[["time_seconds", "relative_to_standard", "relative_time"]] = data[
        "winning_time"
    ].apply(lambda x: pd.Series(process_time_strings(x)))
    return data


def convert_tf_rating(data: pd.DataFrame) -> pd.DataFrame:
    I("Converting tf rating")
    return data.assign(tfr=data["tfr"].str.extract(r"(\d+)").astype("Int64"))


def create_headgear_data(data: pd.DataFrame) -> pd.DataFrame:
    I("Creating headgear data")
    return data.assign(headgear=lambda x: x["headgear"].apply(convert_headgear))


def process_data(
    data: pd.DataFrame,
    transform_data_model: TransformedDataModel,
    race_data_model: RaceDataModel,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = (
        data.pipe(map_race_time_column)
        .pipe(get_surfaces_from_tf_rating)
        .pipe(get_tf_rating_values)
        .pipe(create_pounds)
        .pipe(get_inplay_high_and_low)
        .pipe(create_distance_data)
        .pipe(create_time_data)
        .pipe(create_headgear_data)
        .pipe(convert_tf_rating)
        .pipe(clean_race_class_field)
        .pipe(convert_horse_type_to_colour_sex)
        .pipe(create_distance_beaten_data)
    )
    transformed_data = data.pipe(
        convert_and_validate_data,
        transform_data_model,
        transform_string_field_lengths,
        "unique_id",
    )
    race_data = data.pipe(
        convert_and_validate_data, race_data_model, race_string_field_lengths, "race_id"
    )

    transformed_data = transformed_data[
        [field.name for field in fields(transform_data_model)]
    ].drop_duplicates(subset=["unique_id"])
    race_data = race_data[
        [field.name for field in fields(race_data_model)]
    ].drop_duplicates(subset=["race_id"])

    return transformed_data, race_data


def validate_data(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    non_null_fields = [
        "race_time",
        "race_date",
        "horse_name",
        "course_id",
        "horse_id",
        "jockey_id",
        "trainer_id",
        "owner_id",
        "sire_id",
        "dam_id",
        "unique_id",
    ]

    valid_rows = data[non_null_fields].notna().all(axis=1)

    accepted_df = data[valid_rows]
    rejected_df = data[~valid_rows]

    if not rejected_df.empty:
        for index, row in rejected_df.iterrows():
            missing_fields = [field for field in non_null_fields if pd.isna(row[field])]
            W(
                f"Row {index} rejected due to missing values in: {', '.join(missing_fields)}"
            )

    return accepted_df, rejected_df


def transform_data(
    data: pd.DataFrame,
    transform_data_model: TransformedDataModel,
    race_data_model: RaceDataModel,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    transformed_data, race_data = process_data(
        data=data,
        transform_data_model=transform_data_model,
        race_data_model=race_data_model,
    )

    accepted_data, rejected_data = transformed_data.pipe(validate_data)

    return accepted_data, rejected_data, race_data


if __name__ == "__main__":
    data = fetch_data("SELECT * FROM public.missing_performance_data_vw;")
    if data.empty:
        I("No missing data to transform.")
        exit()

    accepted_data, rejected_data, race_data = transform_data(
        data=data,
        transform_data_model=TransformedDataModel,
        race_data_model=RaceDataModel,
    )
    store_data(accepted_data, "transformed_performance_data", "staging", truncate=True)
    store_data(
        rejected_data, "transformed_performance_data_rejected", "staging", truncate=True
    )
    store_data(race_data, "transformed_race_data", "staging", truncate=True)

    pt(
        load_transformed_performance_data,
        load_transformed_race_data,
    )
