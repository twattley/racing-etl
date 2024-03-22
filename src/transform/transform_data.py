import pandas as pd
import re

def time_to_seconds(time_str):
    """Convert a time string in format 'Xm Ys' to total seconds."""
    parts = re.split('[msh]', time_str.strip())
    seconds = 0.0
    if 'm' in time_str:
        seconds += float(parts[0]) * 60  # Convert minutes to seconds
        parts.pop(0)
    if parts and parts[0]:  # Check if there's a remaining part for seconds
        seconds += float(parts[0])
    return seconds

def process_time_strings(s):
    if '(' not in s:
        return round(time_to_seconds(s), 2), None, None
    outside, inside = re.match(r"([^\(]+)\(([^)]+)\)", s).groups()
    
    relative = inside.split('by')[0].strip()
    relative_time = time_to_seconds(inside.split('by')[1].strip())
    total_seconds_time = time_to_seconds(outside.strip())

    return round(total_seconds_time, 2), relative, round(relative_time, 2)

def remove_brackets_from_draw(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(draw=lambda x: x["tf_draw"].str.extract(r"(\d+)"))


def get_tf_rating_values(data: pd.DataFrame) -> pd.DataFrame:
    view_map = {
        "+": "positive",
        "?": "questionable",
    }
    data = data.assign(
        tf_rating=lambda x: x["tf_tf_rating"].str.extract(r"(\d+)").astype(float),
        tf_rating_view=lambda x: x["tf_tf_rating"]
        .str.extract(r"(\D+)")
        .map(view_map)
        .fillna("neutral"),
    )
    data = data.assign(
        tf_rating=pd.to_numeric(data["tf_tf_rating"], errors="coerce")
        .fillna(0)
        .astype(int)
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


def convert_headgear(e: str) -> list:
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
        return first_time_headgear

    if not headgear and e:
        raise ValueError(f"Unknown headgear code: {e}")
    return headgear


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    data = remove_brackets_from_draw(data)
    data = get_tf_rating_values(data)
    data[['yards', 'meters', 'kilometers']] = data['tf_distance'].apply(lambda x: pd.Series(convert_distances(x)))
    data[['time_seconds', 'relative', 'relative_time']] = data['rp_race_time'].apply(lambda x: pd.Series(process_time_strings(x)))
    data = data.assign(headgear=lambda x: x["rp_headgear"].apply(convert_headgear))
    return data

