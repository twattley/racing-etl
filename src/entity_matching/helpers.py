from datetime import datetime

import pandas as pd


def convert_between_ten_and_ten(row):
    """
    Convert time to 24-hour format if country code is 1 or 2 and time is between 10 AM and 10 PM.
    """
    time_str, country_code = row["race_time"], row["country_code"]
    if country_code not in [1, 2]:
        return time_str
    time_obj = datetime.strptime(time_str, "%I:%M")

    if time_obj.hour >= 10 and time_obj.hour < 22:
        return time_str
    time_obj = datetime.strptime(f"{time_str} PM", "%I:%M %p")
    return time_obj.strftime("%H:%M")


def make_time_24hr(data):

    data["converted_time"] = data.apply(convert_between_ten_and_ten, axis=1)
    data["datetime_str"] = data["race_date"] + " " + data["converted_time"]
    data["t_race_time"] = pd.to_datetime(data["datetime_str"], format="%Y-%m-%d %H:%M")

    data = data.drop(columns=["datetime_str", "converted_time"])

    return data
