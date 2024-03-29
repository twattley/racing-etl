import hashlib
import re

import numpy as np
import pandas as pd

from src.storage.sql_db import store_data

df = pd.read_csv("/Users/tom.wattley/Desktop/missing.csv")


def print_dataframe_for_testing(df):

    print("pd.DataFrame({")

    for col in df.columns:
        value = df[col].iloc[0]
        if re.match(r"\d{4}-\d{2}-\d{2}", str(value)):
            str_test = (
                "[" + " ".join([f"pd.Timestamp('{x}')," for x in list(df[col])]) + "]"
            )
            print(f"'{col}':{str_test},")
        else:
            print(f"'{col}':{list(df[col])},")
    print("})")


print_dataframe_for_testing(df)

horse_name = "Aurora Princess"
horse_id = "000000534130"

trainer_name = "EMMET MULLINS".title()
trainer_id = "000000052794"

sire_name = "THE GURKHA".title()
sire_id = "000000361698"

jockey_name = "SHANE FOLEY".title()
jockey_id = "000000013854"

dam_name = "RUB A DUB DUB".title()
dam_id = "000000383218"

finishing_position = "D"
debug_link = "https://www.timeform.com/horse-racing/result/galway/2021-07-27/1710/209/2"
unique_id = horse_id + finishing_position + debug_link
unique_id = hashlib.sha512(unique_id.encode()).hexdigest()


store_data(
    pd.DataFrame(
        {
            "tf_rating": [np.NaN],
            "tf_speed_figure": [np.NaN],
            "draw": [np.NaN],
            "trainer_name": [trainer_name],
            "trainer_id": [trainer_id],
            "jockey_name": [jockey_name],
            "jockey_id": [jockey_id],
            "sire_name": [sire_name],
            "sire_id": [sire_id],
            "dam_name": [dam_name],
            "dam_id": [dam_id],
            "finishing_position": [finishing_position],
            "horse_name": [horse_name],
            "horse_id": [horse_id],
            "horse_name_link": [horse_name],
            "horse_age": [np.NaN],
            "equipment": [np.NaN],
            "official_rating": [np.NaN],
            "fractional_price": [np.NaN],
            "betfair_win_sp": [np.NaN],
            "betfair_place_sp": [np.NaN],
            "in_play_prices": [np.NaN],
            "tf_comment": ["disqualified"],
            "course": ["galway"],
            "race_date": [
                pd.Timestamp("2021-07-27"),
            ],
            "race_time": [1710],
            "race_timestamp": [
                pd.Timestamp("2021-07-27 17:10:00"),
            ],
            "course_id": [209],
            "race": [2],
            "race_id": [
                "fcd0f1b7aab7168640b39c8d5a18b1ba188117d0a06035d68fca74ea8a56b942"
            ],
            "distance": ["7f"],
            "going": ["Soft"],
            "prize": ["€16,500"],
            "hcap_range": [np.NaN],
            "age_range": ["2yo"],
            "race_type": ["Flat"],
            "main_race_comment": [
                "A most unsatisfactory renewal of an often-informative fillies maiden, the first past the post later identified as the ITC 79-rated Aurora Princess, who had been scheduled to run in a 3-y-o handicap later on the card."
            ],
            "debug_link": [
                "https://www.timeform.com/horse-racing/result/galway/2021-07-27/1710/209/2"
            ],
            "created_at": [
                pd.Timestamp("now"),
            ],
            "sire_name_link": [np.NaN],
            "unique_id": [unique_id],
        }
    ),
    "performance_data",
    "tf_raw",
)
