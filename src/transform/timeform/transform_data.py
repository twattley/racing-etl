import pandas as pd


def remove_brackets_from_draw(data: pd.DataFrame) -> pd.DataFrame:
    return data.assign(draw=lambda x: x["draw"].str.extract(r"(\d+)"))


def get_tf_rating_values(data: pd.DataFrame) -> pd.DataFrame:
    view_map = {
        "+": "positive",
        "?": "questionable",
    }
    data = data.assign(
        tf_rating=lambda x: x["tf_rating"].str.extract(r"(\d+)").astype(float),
        tf_rating_view=lambda x: x["tf_rating"]
        .str.extract(r"(\D+)")
        .map(view_map)
        .fillna("neutral"),
    )
    data = data.assign(
        tf_rating=pd.to_numeric(data["tf_rating"], errors="coerce")
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


# Apply the function to the 'distance_str' column and unpack the results into new columns
# df[['yards', 'meters', 'kilometers']] = df['distance_str'].apply(lambda x: pd.Series(convert_distances(x)))
